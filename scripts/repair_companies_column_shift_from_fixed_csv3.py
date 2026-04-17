#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fixed_csv_3/1, fixed_csv_3/2 配下のCSVから PostgreSQL companies（np~ ID）と照合し、列ズレを検出・修復する。

CSV→DB の対応付け（多重マッチング・後段ほどフォールバック）:
  1) hash: 正規化強化後の擬似ID（NFKC・法人格除去・空白除去）が DB id と一致、または従来の strip のみ擬似IDが一致
  2) corp_num: DB の name（13桁のみ）または corporate_number の13桁と CSV 法人番号が一致
  3) attribute: 都道府県 + 住所の市区町村レベル + 代表者名（正規化後）が一致

主な修復内容:
  - name が13桁法人番号のみ → CSVの社名で上書き、退避13桁は corporate_number が空かつ衝突しないときのみ設定
  - representative_name が郵便番号形式 → CSVの代表者名で上書き、退避値を postal_code へ
  - 住所が日付化・概要混入 → CSVの住所で上書き
  - industry_large〜detail に電話・代表者・住所が混入 → 空の宛先カラムへ移送し、当該業種列は NULL（業種2は CSV があれば補填）

衝突回避:
  - Unique制約違反（法人番号重複）を避けるため、更新前に一時テーブル側で既存データとの重複をチェックし、衝突分は corporate_number の更新をスキップします。
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import re
import sys
import types
from collections import Counter
from urllib.parse import urlparse
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

# CSVの巨大なフィールドサイズに対応
csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError as e:
    raise SystemExit("psycopg2 が必要です: pip install psycopg2-binary") from e

BASE_DIR = Path(__file__).resolve().parent.parent
# スクリプトの場所に依らず、常にこのリポジトリ直下の .env を指す絶対パス
_DOTENV_PATH = Path(__file__).resolve().parent.parent / ".env"
try:
    from dotenv import load_dotenv

    load_dotenv(dotenv_path=_DOTENV_PATH, override=True)
    load_dotenv()
except ImportError:
    pass
DEFAULT_CSV_ROOT = BASE_DIR / "fixed_csv_3"
SUBDIRS = ("1", "2")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOG = logging.getLogger(__name__)

# --- 正規化・判定用定数・正規表現 ---
_WS_ALL = re.compile(r"[\s\u3000\t\n\r\f\v]+", re.UNICODE)

LEGAL_ENTITY_PATTERN = re.compile(
    r"株式会社|（株）|\(株\)|㈱|㈲|有限会社|（有）|\(有\)|合同会社|合名会社|合資会社|"
    r"一般社団法人|一般財団法人|公益社団法人|公益財団法人|特定非営利活動法人|NPO法人|"
    r"医療法人|社会福祉法人|学校法人|宗教法人|国立大学法人|独立行政法人|"
    r"有限責任事業組合|投資事業有限責任組合|外国会社等|上場会社",
    re.UNICODE,
)

RE_CORP_13 = re.compile(r"^\d{13}$")
RE_POSTAL = re.compile(r"^\d{3}-\d{4}$")
RE_POSTAL_DIGITS7 = re.compile(r"^\d{7}$")
RE_DATE_ONLY_SLASH = re.compile(r"^\d{4}/\d{1,2}/\d{1,2}$")
RE_DATE_ONLY_HYPHEN = re.compile(r"^\d{4}-\d{1,2}-\d{1,2}$")
RE_OFFICER_MARKER = re.compile(
    r"[（(](専|取|監|代|常|副|会|社長|副長|代会|代専|専務|常務|副社長)"
)
RE_CITY_LEVEL = re.compile(r"^(.+?(?:市|区|町|村|郡))")

# 業種列への誤格納の救出（都道府県は長い名称を先にマッチ）
PREFECTURES: Tuple[str, ...] = tuple(
    sorted(
        (
            "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
            "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
            "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
            "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
            "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
            "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
            "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
        ),
        key=len,
        reverse=True,
    )
)
# NFKC 後の 0 始まり・桁数十分な電話らしき文字列（ハイフンは半角全角）
RE_INDUSTRY_MISPLACED_PHONE = re.compile(r"^0[0-9\-－]{9,}$")
RE_INDUSTRY_MISPLACED_REP = re.compile(
    r"^(\(|（)?(代表取締役|代表者|代表|取締役|共同代表|社長|会長|副社長|専務|常務|監査役|執行役|業務執行|ＣＥＯ|CEO|ＣＦＯ|CFO)",
    re.UNICODE,
)

# --- 正規化ロジック ---

def remove_legal_entities(text: str) -> str:
    s = text
    for _ in range(64):
        n = LEGAL_ENTITY_PATTERN.sub("", s)
        if n == s: break
        s = n
    return s

def normalize_company_name_for_hash(name: str) -> str:
    s = unicodedata.normalize("NFKC", name or "")
    s = remove_legal_entities(s)
    s = _WS_ALL.sub("", s)
    return s

def normalize_prefecture_for_hash(prefecture: str) -> str:
    s = unicodedata.normalize("NFKC", prefecture or "")
    s = _WS_ALL.sub("", s)
    return s

def normalize_person_token(s: str) -> str:
    s = unicodedata.normalize("NFKC", s or "")
    return _WS_ALL.sub("", s)

def pseudo_id(name: str, prefecture: str) -> str:
    n = (name or "").strip()
    p = (prefecture or "").strip()
    raw = f"{n}\x00{p}"
    h = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"np{h[:32]}"

def pseudo_id_normalized(name: str, prefecture: str) -> str:
    return pseudo_id(
        normalize_company_name_for_hash(name),
        normalize_prefecture_for_hash(prefecture),
    )

def address_to_city_level(prefecture: str, address: str) -> str:
    p = normalize_prefecture_for_hash(prefecture)
    a = unicodedata.normalize("NFKC", address or "")
    a = _WS_ALL.sub("", a)
    if not a: return ""
    rest = a[len(p) :] if p and a.startswith(p) else a
    m = RE_CITY_LEVEL.match(rest)
    if m: return m.group(1)
    m2 = re.match(r"^([^0-9０-９丁番号\-ー－―の]{2,32})", rest)
    return m2.group(1) if m2 else rest[:24]

# --- データ構造 ---

@dataclass
class CsvRow:
    name: str
    prefecture: str
    corporate_number: Optional[str]
    representative_name: Optional[str]
    postal_code: Optional[str]
    industry_middle: Optional[str]
    address: Optional[str]
    overview: Optional[str]
    business_descriptions: Optional[str]
    source_file: str = ""
    source_line: int = 0

@dataclass
class CsvLookup:
    by_simple_id: Dict[str, CsvRow] = field(default_factory=dict)
    by_norm_id: Dict[str, CsvRow] = field(default_factory=dict)
    by_corp: Dict[str, CsvRow] = field(default_factory=dict)
    by_attr: Dict[str, CsvRow] = field(default_factory=dict)

@dataclass
class FixRow:
    id: str
    name: Optional[str] = None
    corporate_number: Optional[str] = None
    representative_name: Optional[str] = None
    postal_code: Optional[str] = None
    phone_number: Optional[str] = None
    industry_middle: Optional[str] = None
    clear_industry_large: bool = False
    clear_industry_middle: bool = False
    clear_industry_small: bool = False
    clear_industry_detail: bool = False
    address: Optional[str] = None
    overview: Optional[str] = None
    business_descriptions: Optional[str] = None
    reasons: List[str] = field(default_factory=list)

# --- ロジック ---

def csv_attribute_key(c: CsvRow) -> Optional[str]:
    npref = normalize_prefecture_for_hash(c.prefecture)
    nrep = normalize_person_token(c.representative_name or "")
    if not npref or not nrep: return None
    city = address_to_city_level(c.prefecture, c.address or "")
    if not city: return None
    return f"{npref}\x00{city}\x00{nrep}"

def db_attribute_key(db: Dict[str, Any]) -> Optional[str]:
    npref = normalize_prefecture_for_hash(str(db.get("prefecture") or ""))
    nrep = normalize_person_token(str(db.get("representative_name") or ""))
    if not npref or not nrep: return None
    city = address_to_city_level(str(db.get("prefecture") or ""), str(db.get("address") or ""))
    if not city: return None
    return f"{npref}\x00{city}\x00{nrep}"

def normalize_corporate_number(val: Any) -> Optional[str]:
    if not val: return None
    s = re.sub(r"\D", "", str(val).strip())
    if len(s) == 12: s = "0" + s
    return s if (len(s) == 13 and s.isdigit()) else None

def db_corp_number_candidates(db: Dict[str, Any]) -> List[str]:
    seen: set = set()
    out: List[str] = []
    for src in (db.get("corporate_number"), db.get("name")):
        if not src: continue
        s = str(src).strip()
        if RE_CORP_13.match(s):
            if s not in seen:
                seen.add(s); out.append(s)
            continue
        cn = normalize_corporate_number(s)
        if cn and cn not in seen:
            seen.add(cn); out.append(cn)
    return out

def resolve_csv_for_db_row(db: Dict[str, Any], L: CsvLookup) -> Optional[Tuple[CsvRow, str]]:
    pid = db["id"]
    if pid in L.by_norm_id: return L.by_norm_id[pid], "hash"
    if pid in L.by_simple_id: return L.by_simple_id[pid], "hash"
    for cand in db_corp_number_candidates(db):
        if cand in L.by_corp: return L.by_corp[cand], "corp_num"
    ak = db_attribute_key(db)
    if ak and ak in L.by_attr: return L.by_attr[ak], "attribute"
    return None

def is_name_spilled_corporate_number(name: Optional[str]) -> bool:
    return bool(name and RE_CORP_13.match(name.strip()))

def is_representative_spilled_postal(rep: Optional[str]) -> bool:
    if not rep: return False
    s = rep.strip()
    return bool(RE_POSTAL.match(s) or RE_POSTAL_DIGITS7.match(s))

def is_industry_spilled_officer(text: Optional[str]) -> bool:
    if not text or len(text) > 500: return False
    return bool(RE_OFFICER_MARKER.search(text))

def is_address_anomalous(addr: Optional[str]) -> bool:
    if not addr: return False
    s = addr.strip()
    if RE_DATE_ONLY_SLASH.match(s) or RE_DATE_ONLY_HYPHEN.match(s): return True
    if len(s) >= 280: return True
    if len(s) >= 120 and s.count("。") >= 4: return True
    return False

def empty_db_value(v: Any) -> bool:
    return v is None or (isinstance(v, str) and not v.strip())

def _normalize_industry_cell(s: str) -> str:
    return unicodedata.normalize("NFKC", (s or "").strip())

def classify_industry_misplacement(raw: Optional[str]) -> Optional[str]:
    """業種列の値が phone / representative / address の誤格納ならその種別を返す。"""
    if not raw or len(raw) > 600:
        return None
    s = _normalize_industry_cell(raw)
    if not s:
        return None
    if RE_INDUSTRY_MISPLACED_PHONE.match(s):
        return "phone"
    if RE_INDUSTRY_MISPLACED_REP.match(s):
        return "representative"
    if len(s) >= 8 and any(s.startswith(p) for p in PREFECTURES):
        return "address"
    return None

def apply_industry_misplacement_rescue(r: Dict[str, Any], fr: FixRow) -> None:
    """industry_* から phone_number / representative_name / address へ救出し、当該業種列をクリア対象にする。"""
    cols = ("industry_large", "industry_middle", "industry_small", "industry_detail")
    for col in cols:
        raw = r.get(col)
        if empty_db_value(raw):
            continue
        kind = classify_industry_misplacement(str(raw))
        if not kind:
            continue
        clear_attr = {
            "industry_large": "clear_industry_large",
            "industry_middle": "clear_industry_middle",
            "industry_small": "clear_industry_small",
            "industry_detail": "clear_industry_detail",
        }[col]
        val = _normalize_industry_cell(str(raw))
        if kind == "phone":
            setattr(fr, clear_attr, True)
            if empty_db_value(r.get("phone_number")) and fr.phone_number is None:
                fr.phone_number = val
                fr.reasons.append(f"phone_rescued_from_{col}")
            else:
                fr.reasons.append(f"industry_phone_cleared_only_{col}")
        elif kind == "representative":
            setattr(fr, clear_attr, True)
            if empty_db_value(r.get("representative_name")) and fr.representative_name is None:
                fr.representative_name = val
                fr.reasons.append(f"representative_rescued_from_{col}")
            else:
                fr.reasons.append(f"industry_representative_cleared_only_{col}")
        elif kind == "address":
            setattr(fr, clear_attr, True)
            if empty_db_value(r.get("address")) and fr.address is None:
                fr.address = val
                fr.reasons.append(f"address_rescued_from_{col}")
            else:
                fr.reasons.append(f"industry_address_cleared_only_{col}")

# --- CSV 読込処理 ---

def normalize_header(raw: str) -> str:
    s = str(raw).strip().strip("\ufeff").replace("\n", " ")
    s = s.replace("　", " ").replace("（", "(").replace("）", ")")
    return re.sub(r"\s+", "", s)

def build_header_index(headers: List[str]) -> Dict[str, int]:
    norm_to_idx = {normalize_header(h): i for i, h in enumerate(headers)}
    field_aliases = {
        "会社名": ["会社名"], "都道府県": ["都道府県"], "代表者名": ["代表者名"],
        "法人番号": ["法人番号"], "郵便番号": ["郵便番号"], "業種2": ["業種2", "業種(中)", "業種-中"],
        "住所": ["住所"], "概要": ["概要"], "説明": ["説明"],
    }
    idx: Dict[str, int] = {}
    for key, aliases in field_aliases.items():
        for alias in aliases:
            ni = normalize_header(alias)
            if ni in norm_to_idx:
                idx[key] = norm_to_idx[ni]; break
    return idx

def cell(row: List[str], idx: Optional[int]) -> Optional[str]:
    if idx is None or idx < 0 or idx >= len(row): return None
    s = str(row[idx] or "").strip()
    return s if s else None

def iter_csv_rows() -> Iterator[Tuple[str, int, List[str], List[str], Dict[str, int]]]:
    for sub in SUBDIRS:
        root = DEFAULT_CSV_ROOT / sub
        if not root.is_dir(): continue
        for path in sorted(root.rglob("*.csv")):
            rel = str(path.relative_to(BASE_DIR))
            try:
                with path.open("r", encoding="utf-8-sig", newline="") as f:
                    reader = csv.reader(f)
                    headers = next(reader, None)
                    if not headers: continue
                    hidx = build_header_index(headers)
                    if "会社名" not in hidx: continue
                    for lineno, row in enumerate(reader, start=2):
                        if not row or all(not (c or "").strip() for c in row): continue
                        yield rel, lineno, headers, row, hidx
            except UnicodeDecodeError:
                LOG.warning("UTF-8読込エラー: %s", rel)

def build_csv_lookup_tables() -> Tuple[CsvLookup, List[str]]:
    lookup = CsvLookup()
    warnings = []
    for rel, lineno, _, row, hidx in iter_csv_rows():
        name = cell(row, hidx.get("会社名"))
        pref = cell(row, hidx.get("都道府県")) or ""
        if not name: continue
        
        row_obj = CsvRow(
            name=name.strip(), prefecture=pref.strip(),
            corporate_number=normalize_corporate_number(cell(row, hidx.get("法人番号"))),
            representative_name=cell(row, hidx.get("代表者名")),
            postal_code=cell(row, hidx.get("郵便番号")),
            industry_middle=cell(row, hidx.get("業種2")),
            address=cell(row, hidx.get("住所")),
            overview=cell(row, hidx.get("概要")),
            business_descriptions=cell(row, hidx.get("説明")),
            source_file=rel, source_line=lineno
        )
        lookup.by_simple_id[pseudo_id(name, pref)] = row_obj
        lookup.by_norm_id[pseudo_id_normalized(name, pref)] = row_obj
        if row_obj.corporate_number:
            lookup.by_corp[row_obj.corporate_number] = row_obj
        ak = csv_attribute_key(row_obj)
        if ak: lookup.by_attr[ak] = row_obj
    return lookup, warnings

# --- DB 処理 ---

def _database_url_for_psycopg2(url: str) -> str:
    u = url.strip()
    for prefix in ("postgresql+psycopg2://", "postgres+psycopg2://", "postgresql+psycopg://"):
        if u.startswith(prefix): return "postgresql://" + u[len(prefix) :]
    return u


def _env_strip(key: str) -> str:
    v = os.getenv(key)
    if v is None:
        return ""
    return str(v).strip().strip('"').strip("'")


# Cloud SQL Proxy 等のローカル転送でよく使うポート（環境変数未指定時の既定）
_DEFAULT_PG_PORT = 5434


def _strip_outer_quotes(s: str) -> str:
    """先頭末尾の ' または " を繰り返し除去（.env の引用符対策）。"""
    t = str(s).strip()
    while len(t) >= 2 and t[0] == t[-1] and t[0] in "\"'":
        t = t[1:-1].strip()
    return t


def _read_project_root_dotenv(env_path: Path) -> Dict[str, str]:
    """プロジェクトルート .env を常にパースして key→値（空は捨てる）。"""
    if not env_path.is_file():
        return {}
    try:
        from dotenv import dotenv_values
    except ImportError:
        return {}
    out: Dict[str, str] = {}
    for k, v in (dotenv_values(env_path) or {}).items():
        if k is None or v is None:
            continue
        kk = str(k).strip()
        sv = _strip_outer_quotes(str(v).strip().strip('"').strip("'"))
        if sv:
            out[kk] = sv
    return out


def _conn_var(key: str, file_vals: Dict[str, str]) -> str:
    """
    プロジェクト .env（file_vals）を os.environ より優先する。
    シェルに誤った DATABASE_USER 等が残っていても、リポジトリ直下 .env を優先して接続できるようにする。
    """
    fv = file_vals.get(key)
    if fv:
        return _strip_outer_quotes(str(fv))
    raw = os.environ.get(key)
    if raw is not None and str(raw).strip() != "":
        return _strip_outer_quotes(str(raw).strip().strip('"').strip("'"))
    return ""


def _mask_secret(val: Optional[str], *, label: str = "") -> str:
    """ログ用。値の有無と長さだけ示し中身は伏せる。"""
    if val is None:
        return f"{label}(unset)"
    s = str(val)
    if not s.strip():
        return f"{label}(empty len=0)"
    return f"{label}set=yes len={len(s)} head_tail={s[0]!r}…{s[-1]!r}"


def _parse_database_url_robust(url: str) -> Optional[Any]:
    """
    DATABASE_URL を分解する。

    urllib.parse.urlparse は userinfo 内のパスワードに ``/`` が含まれると誤解析する
    （例: ``postgresql://postgres:Legatus2000/@127.0.0.1:5434/postgres`` で username/password が None になる）。
    その場合は ``user:password`` と ``host:port/path`` の境界を最後の ``@`` で rsplit して復元する。
    """
    u = _database_url_for_psycopg2(url.strip())
    if not u:
        return None
    low = u.lower()
    if not low.startswith(("postgresql://", "postgres://")):
        p = urlparse(u)
        return p if p.netloc else None
    rest = u.split("://", 1)[1]
    if "@" not in rest:
        return urlparse(u)
    creds, host_db = rest.rsplit("@", 1)
    if ":" in creds:
        user, password = creds.split(":", 1)
    else:
        user, password = creds, ""
    path = "/"
    hp = host_db
    if "/" in host_db:
        hp, pr = host_db.split("/", 1)
        path = "/" + pr
    port: Optional[int] = None
    host = hp
    if hp.startswith("["):
        idx = hp.find("]:")
        if idx != -1:
            ps = hp[idx + 2 :]
            if ps.isdigit():
                host = hp[1:idx]
                port = int(ps)
        else:
            host = hp.strip("[]")
    elif ":" in hp:
        host_part, ps = hp.rsplit(":", 1)
        if ps.isdigit():
            host = host_part
            port = int(ps)
        else:
            host = hp
    db_part = (path or "/").lstrip("/")
    dbname = (db_part.split("/")[0] if db_part else "") or "postgres"
    return types.SimpleNamespace(
        username=user or None,
        password=password if password is not None else None,
        hostname=host or None,
        port=port,
        path=path or "/",
        dbname_from_path=dbname,
    )


def _port_from_env_and_file(file_vals: Dict[str, str]) -> Optional[int]:
    """POSTGRES_PORT 等。プロセス環境が空なら file_vals から _conn_var で解決。"""
    for key in ("POSTGRES_PORT", "DATABASE_PORT", "PGPORT"):
        raw = _conn_var(key, file_vals)
        if not raw:
            continue
        try:
            return int(raw)
        except ValueError:
            LOG.warning("環境変数 %s=%r は整数として解釈できません。無視します。", key, raw)
    return None


def _resolve_discrete_pg_port(file_vals: Dict[str, str]) -> int:
    """個別パラメータ接続時のポート: .env / 環境で指定があればそれ、なければ 5434。"""
    return _port_from_env_and_file(file_vals) or _DEFAULT_PG_PORT


def _password_from_conn_vars(file_vals: Dict[str, str], parsed: Optional[Any]) -> str:
    """パスワード: 個別変数 → DATABASE_URL 分解結果の順。"""
    for key in ("POSTGRES_PASSWORD", "DATABASE_PASS", "DB_PASSWORD", "PGPASSWORD"):
        s = _conn_var(key, file_vals)
        if s:
            return s
    if parsed is not None and parsed.password is not None:
        return _strip_outer_quotes(str(parsed.password))
    return ""


def _user_from_conn_vars(file_vals: Dict[str, str], parsed: Optional[Any]) -> str:
    """ユーザー: 個別変数が空なら DATABASE_URL の username を os の USER より先に使う。"""
    u = _conn_var("POSTGRES_USER", file_vals) or _conn_var("DATABASE_USER", file_vals)
    if u:
        return u
    if parsed is not None and parsed.username:
        return str(parsed.username)
    return (
        _env_strip("USER")
        or _env_strip("LOGNAME")
        or "postgres"
    )


def _host_is_localhost_for_proxy_port(host: str) -> bool:
    """要件: 127.0.0.1 または localhost のときプロキシポート 5434 を強制する。"""
    if not host:
        return False
    h = host.lower().strip("[]")
    return h in ("127.0.0.1", "localhost")


def get_connection():
    """
    PostgreSQL へ接続する。

    - ``Path(__file__).resolve().parent.parent / ".env"`` を load_dotenv + dotenv_values で読む。
    - 接続用の値は **プロジェクト .env（file_vals）を os.environ より優先**（_conn_var）。
    - ``DATABASE_URL`` のパスワードに ``/`` が含まれる場合、urllib.parse.urlparse は壊れるため
      ``_parse_database_url_robust``（最後の ``@`` で rsplit）を使う。
    - psycopg2.connect は host / port / user / password / dbname のキーワード引数のみ。
    - host が 127.0.0.1 または localhost のとき port は 5434 に固定。
    """
    env_path = Path(__file__).resolve().parent.parent / ".env"
    abs_env = str(env_path.resolve())
    LOG.info(
        "load_dotenv 対象: path=%r os.path.exists=%s",
        abs_env,
        os.path.exists(abs_env),
    )
    try:
        from dotenv import find_dotenv, load_dotenv

        try:
            fd = find_dotenv(usecwd=True)
        except TypeError:
            fd = find_dotenv()
        LOG.info(
            "dotenv find_dotenv (cwd 探索) -> %r exists=%s",
            fd,
            bool(fd) and os.path.exists(fd),
        )
        load_dotenv(dotenv_path=env_path, override=True)
        load_dotenv()
    except ImportError:
        LOG.warning("python-dotenv 未インストール。pip install python-dotenv を推奨します。")
    except Exception as ex:
        LOG.warning("load_dotenv 中に例外: %s", ex)

    file_vals = _read_project_root_dotenv(env_path)
    LOG.info(
        "カレントの .env: exists=%s abspath=%r",
        os.path.exists(".env"),
        os.path.abspath(".env"),
    )
    LOG.info(
        "プロジェクトルート .env: path=%r is_file=%s file_vals キー数=%d keys=%s",
        abs_env,
        env_path.is_file(),
        len(file_vals),
        sorted(file_vals.keys()),
    )
    LOG.info(
        "load_dotenv 直後 os.environ: DATABASE_PASS %s | POSTGRES_PASSWORD %s | POSTGRES_USER %r | DATABASE_USER %r",
        _mask_secret(os.environ.get("DATABASE_PASS")),
        _mask_secret(os.environ.get("POSTGRES_PASSWORD")),
        os.environ.get("POSTGRES_USER"),
        os.environ.get("DATABASE_USER"),
    )
    if "DATABASE_PASS" in file_vals:
        LOG.info("file_vals DATABASE_PASS %s", _mask_secret(file_vals.get("DATABASE_PASS")))

    url = _conn_var("DATABASE_URL", file_vals)
    if url:
        naive = urlparse(_database_url_for_psycopg2(url))
        LOG.info(
            "【比較】urllib.parse.urlparse: netloc=%r username=%r password_none=%s path=%r "
            "(パスワードに / があると user/pass が None になり得る)",
            naive.netloc,
            naive.username,
            naive.password is None,
            naive.path,
        )
    parsed: Optional[Any] = _parse_database_url_robust(url) if url else None
    if parsed is not None:
        LOG.info(
            "【採用】robust parse: hostname=%r port=%r username=%r %s path=%r",
            getattr(parsed, "hostname", None),
            getattr(parsed, "port", None),
            getattr(parsed, "username", None),
            _mask_secret(getattr(parsed, "password", None)),
            getattr(parsed, "path", None),
        )

    keepalive_kwargs = dict(
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )
    sslmode = _conn_var("POSTGRES_SSLMODE", file_vals) or _conn_var("DATABASE_SSLMODE", file_vals) or "prefer"
    explicit_host = _conn_var("POSTGRES_HOST", file_vals) or _conn_var("DATABASE_HOST", file_vals)
    p_env = _port_from_env_and_file(file_vals)

    if url and parsed is not None:
        if hasattr(parsed, "dbname_from_path"):
            db_from_url = str(parsed.dbname_from_path)
        else:
            pth = (getattr(parsed, "path", "") or "").lstrip("/")
            db_from_url = (pth.split("/")[0] if pth else "") or "postgres"

        user = _user_from_conn_vars(file_vals, parsed)
        password = _password_from_conn_vars(file_vals, parsed)

        dbname = _conn_var("POSTGRES_DB", file_vals) or _conn_var("DATABASE_NAME", file_vals) or db_from_url
        host = explicit_host or (parsed.hostname or "127.0.0.1")
        host = str(host).strip()

        if _host_is_localhost_for_proxy_port(host):
            port = 5434
        else:
            port = p_env if p_env is not None else (parsed.port if parsed.port is not None else 5432)

        LOG.info(
            "【接続直前】今まさに psycopg2.connect しようとしています (DATABASE_URL 分岐): "
            "host=%r port=%r user=%r dbname=%r %s",
            host,
            port,
            user,
            dbname,
            _mask_secret(password, label="password "),
        )
        return psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=dbname,
            sslmode=sslmode,
            **keepalive_kwargs,
        )

    host = explicit_host or "127.0.0.1"
    host = str(host).strip()
    port = _resolve_discrete_pg_port(file_vals)
    user = _user_from_conn_vars(file_vals, None)
    password = _password_from_conn_vars(file_vals, None)
    dbname = _conn_var("POSTGRES_DB", file_vals) or _conn_var("DATABASE_NAME", file_vals) or "postgres"

    LOG.info(
        "【接続直前】今まさに psycopg2.connect しようとしています (個別変数のみ): "
        "host=%r port=%r user=%r dbname=%r %s",
        host,
        port,
        user,
        dbname,
        _mask_secret(password, label="password "),
    )
    return psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname,
        sslmode=sslmode,
        **keepalive_kwargs,
    )

def fetch_companies_np(cur) -> List[Dict[str, Any]]:
    cur.execute("""
        SELECT id, name, corporate_number, representative_name, postal_code,
               prefecture, phone_number,
               industry_large, industry_middle, industry_small, industry_detail,
               address, overview, business_descriptions
        FROM companies WHERE id ~ '^np[0-9a-f]{32}$'
    """)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

def compute_fixes(lookup: CsvLookup, db_rows: List[Dict[str, Any]], fill_null_text: bool) -> Tuple[List[FixRow], List[dict]]:
    fixes, report = [], []
    for r in db_rows:
        pid = r["id"]
        res = resolve_csv_for_db_row(r, lookup)
        if not res:
            report.append({"kind": "csv_missing", "id": pid, "db_name": r.get("name"), "db_corporate_number": r.get("corporate_number")})
            continue
        c, match_type = res
        fr = FixRow(id=pid)
        if is_name_spilled_corporate_number(r.get("name")):
            spilled = r["name"].strip()
            fr.name = c.name
            fr.reasons.append("name_was_corporate_number")
            if empty_db_value(r.get("corporate_number")):
                fr.corporate_number = spilled
                fr.reasons.append("corporate_number_from_spilled_name")
        elif (r.get("name") or "").strip() != c.name:
            report.append({"kind": "name_mismatch_non_anomaly", "id": pid, "match_type": match_type, "db": r.get("name"), "csv": c.name})
        if is_representative_spilled_postal(r.get("representative_name")):
            if c.representative_name: fr.representative_name = c.representative_name
            fr.reasons.append("representative_was_postal")
            pc = r["representative_name"].strip()
            if RE_POSTAL_DIGITS7.match(pc): pc = f"{pc[:3]}-{pc[3:]}"
            fr.postal_code = pc
            fr.reasons.append("postal_code_from_spilled_representative")
        apply_industry_misplacement_rescue(r, fr)
        if fr.clear_industry_middle and c.industry_middle:
            fr.industry_middle = c.industry_middle
            fr.reasons.append("industry_middle_from_csv_after_rescue")
        if is_industry_spilled_officer(r.get("industry_middle")) and not fr.clear_industry_middle:
            if c.industry_middle:
                fr.industry_middle = c.industry_middle
                fr.reasons.append("industry_middle_officer_noise")
        if is_address_anomalous(r.get("address")):
            if c.address:
                fr.address = c.address
                fr.reasons.append("address_anomaly")
        if fill_null_text:
            if empty_db_value(r.get("overview")) and c.overview:
                fr.overview = c.overview; fr.reasons.append("overview_fill")
            if empty_db_value(r.get("business_descriptions")) and c.business_descriptions:
                fr.business_descriptions = c.business_descriptions; fr.reasons.append("business_descriptions_fill")
        if fr.reasons:
            fixes.append(fr)
            patch = {
                k: getattr(fr, k)
                for k in (
                    "name",
                    "corporate_number",
                    "representative_name",
                    "postal_code",
                    "phone_number",
                    "industry_middle",
                    "address",
                    "overview",
                    "business_descriptions",
                )
                if getattr(fr, k) is not None
            }
            cleared_industry = [
                n
                for n, a in (
                    ("industry_large", "clear_industry_large"),
                    ("industry_middle", "clear_industry_middle"),
                    ("industry_small", "clear_industry_small"),
                    ("industry_detail", "clear_industry_detail"),
                )
                if getattr(fr, a)
            ]
            if cleared_industry:
                patch["industry_columns_cleared"] = cleared_industry
            db_before = {k: r.get(k) for k in patch if k != "industry_columns_cleared"}
            for col in cleared_industry:
                db_before[col] = r.get(col)
            report.append(
                {
                    "kind": "fix_candidate",
                    "id": pid,
                    "match_type": match_type,
                    "reasons": list(fr.reasons),
                    "db_before": db_before,
                    "patch": patch,
                    "csv_source": f"{c.source_file}:{c.source_line}",
                }
            )
    return fixes, report

def apply_fixes(conn, fixes: List[FixRow], batch_size: int) -> int:
    if not fixes: return 0
    cur = conn.cursor()
    total = 0
    tuples = [
        (
            f.id,
            f.name,
            f.corporate_number,
            f.representative_name,
            f.postal_code,
            f.phone_number,
            f.industry_middle,
            f.clear_industry_large,
            f.clear_industry_middle,
            f.clear_industry_small,
            f.clear_industry_detail,
            f.address,
            f.overview,
            f.business_descriptions,
        )
        for f in fixes
    ]
    for i in range(0, len(tuples), batch_size):
        chunk = tuples[i : i + batch_size]
        cur.execute("""
            CREATE TEMP TABLE IF NOT EXISTS _company_shift_fixes (
                id TEXT PRIMARY KEY, name TEXT, corporate_number TEXT, representative_name TEXT,
                postal_code TEXT, phone_number TEXT, industry_middle TEXT,
                clear_industry_large BOOLEAN, clear_industry_middle BOOLEAN,
                clear_industry_small BOOLEAN, clear_industry_detail BOOLEAN,
                address TEXT, overview TEXT, business_descriptions TEXT
            ) ON COMMIT DROP;
            TRUNCATE _company_shift_fixes;
        """)
        execute_values(cur, "INSERT INTO _company_shift_fixes VALUES %s", chunk)

        # 衝突回避ロジック: DB内に既に存在する法人番号をNULL化
        cur.execute("""
            UPDATE _company_shift_fixes AS f
            SET corporate_number = NULL
            WHERE f.corporate_number IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM companies AS c 
                  WHERE c.corporate_number = f.corporate_number 
                    AND c.id != f.id
              );
        """)
        # バッチ内の重複も排除
        cur.execute("""
            UPDATE _company_shift_fixes AS f
            SET corporate_number = NULL
            FROM (
                SELECT id, ROW_NUMBER() OVER(PARTITION BY corporate_number ORDER BY id) as rn
                FROM _company_shift_fixes
                WHERE corporate_number IS NOT NULL
            ) AS sub
            WHERE f.id = sub.id AND sub.rn > 1;
        """)

        cur.execute("""
            UPDATE companies AS c SET
                name = COALESCE(f.name, c.name),
                corporate_number = CASE WHEN f.corporate_number IS NOT NULL THEN f.corporate_number ELSE c.corporate_number END,
                representative_name = COALESCE(f.representative_name, c.representative_name),
                postal_code = COALESCE(f.postal_code, c.postal_code),
                phone_number = COALESCE(f.phone_number, c.phone_number),
                industry_large = CASE WHEN f.clear_industry_large THEN NULL ELSE c.industry_large END,
                industry_middle = CASE
                    WHEN f.industry_middle IS NOT NULL THEN f.industry_middle
                    WHEN f.clear_industry_middle THEN NULL
                    ELSE c.industry_middle END,
                industry_small = CASE WHEN f.clear_industry_small THEN NULL ELSE c.industry_small END,
                industry_detail = CASE WHEN f.clear_industry_detail THEN NULL ELSE c.industry_detail END,
                address = COALESCE(f.address, c.address),
                overview = COALESCE(f.overview, c.overview),
                business_descriptions = COALESCE(f.business_descriptions, c.business_descriptions)
            FROM _company_shift_fixes AS f WHERE c.id = f.id;
        """)
        total += cur.rowcount
    conn.commit(); cur.close()
    return total

def emit_sql(fixes: List[FixRow], out_path: Path, batch_size: int) -> None:
    if not fixes: return
    lines = [
        "BEGIN;",
        "CREATE TEMP TABLE _company_shift_fixes (id TEXT PRIMARY KEY, name TEXT, corporate_number TEXT, representative_name TEXT, postal_code TEXT, phone_number TEXT, industry_middle TEXT, clear_industry_large BOOLEAN, clear_industry_middle BOOLEAN, clear_industry_small BOOLEAN, clear_industry_detail BOOLEAN, address TEXT, overview TEXT, business_descriptions TEXT) ON COMMIT DROP;",
    ]
    tuples = [
        (
            f.id,
            f.name,
            f.corporate_number,
            f.representative_name,
            f.postal_code,
            f.phone_number,
            f.industry_middle,
            f.clear_industry_large,
            f.clear_industry_middle,
            f.clear_industry_small,
            f.clear_industry_detail,
            f.address,
            f.overview,
            f.business_descriptions,
        )
        for f in fixes
    ]
    def esc(v: Any) -> str:
        if v is None: return "NULL"
        if isinstance(v, bool): return "TRUE" if v else "FALSE"
        return "'" + str(v).replace("'", "''") + "'"
    for i in range(0, len(tuples), batch_size):
        chunk = tuples[i : i + batch_size]
        lines.append("TRUNCATE _company_shift_fixes;")
        vals = ",\n".join([f"({','.join(esc(x) for x in t)})" for t in chunk])
        lines.append(f"INSERT INTO _company_shift_fixes VALUES\n{vals};")
        # SQLファイル側にも衝突回避SQLを同梱
        lines.append("""UPDATE _company_shift_fixes AS f SET corporate_number = NULL WHERE f.corporate_number IS NOT NULL AND EXISTS (SELECT 1 FROM companies AS c WHERE c.corporate_number = f.corporate_number AND c.id != f.id);""")
        lines.append(
            """UPDATE companies AS c SET """
            """name = CASE WHEN f.name IS NOT NULL THEN f.name ELSE c.name END, """
            """corporate_number = CASE WHEN f.corporate_number IS NOT NULL THEN f.corporate_number ELSE c.corporate_number END, """
            """representative_name = CASE WHEN f.representative_name IS NOT NULL THEN f.representative_name ELSE c.representative_name END, """
            """postal_code = CASE WHEN f.postal_code IS NOT NULL THEN f.postal_code ELSE c.postal_code END, """
            """phone_number = CASE WHEN f.phone_number IS NOT NULL THEN f.phone_number ELSE c.phone_number END, """
            """industry_large = CASE WHEN f.clear_industry_large THEN NULL ELSE c.industry_large END, """
            """industry_middle = CASE WHEN f.industry_middle IS NOT NULL THEN f.industry_middle WHEN f.clear_industry_middle THEN NULL ELSE c.industry_middle END, """
            """industry_small = CASE WHEN f.clear_industry_small THEN NULL ELSE c.industry_small END, """
            """industry_detail = CASE WHEN f.clear_industry_detail THEN NULL ELSE c.industry_detail END, """
            """address = CASE WHEN f.address IS NOT NULL THEN f.address ELSE c.address END, """
            """overview = CASE WHEN f.overview IS NOT NULL THEN f.overview ELSE c.overview END, """
            """business_descriptions = CASE WHEN f.business_descriptions IS NOT NULL THEN f.business_descriptions ELSE c.business_descriptions END """
            """FROM _company_shift_fixes AS f WHERE c.id = f.id;"""
        )
    lines.append("COMMIT;")
    out_path.write_text("\n".join(lines), encoding="utf-8")

def print_console_summary(n_db: int, n_fixes: int, report_events: List[dict], summary_only: bool) -> None:
    kinds = Counter(e.get("kind") for e in report_events)
    mt: Counter = Counter()
    for e in report_events:
        if e.get("kind") in ("fix_candidate", "name_mismatch_non_anomaly"):
            m = e.get("match_type")
            if m: mt[str(m)] += 1
    title = "サマリー（--report / --emit-sql / --apply は未指定。DB は変更していません）" if summary_only else "実行サマリー"
    print()
    print("=" * 72)
    print(title)
    print("=" * 72)
    print(f"  DB np~ 行数:       {n_db}")
    print(f"  修復候補（UPDATE対象）: {n_fixes}")
    print(f"  レポートイベント件数: {len(report_events)}")
    print("  --- kind ---")
    for k in sorted(kinds.keys(), key=lambda x: (x is None, str(x))):
        label = k if k is not None else "(null)"
        print(f"    {label}: {kinds[k]}")
    if mt:
        print("  --- match_type（fix_candidate / name_mismatch）---")
        for k in sorted(mt.keys()): print(f"    {k}: {mt[k]}")
    if summary_only:
        print("\n  次の例でファイル出力や適用ができます:")
        print("    --report /tmp/report.jsonl")
        print("    --emit-sql /tmp/fixes.sql")
        print("    --apply")
    print()

def main() -> None:
    ap = argparse.ArgumentParser(description="多重マッチングによる列ズレ修復スクリプト")
    ap.add_argument("--report", metavar="PATH", help="レポート出力")
    ap.add_argument("--emit-sql", metavar="PATH", help="SQL 出力")
    ap.add_argument("--apply", action="store_true", help="DB に反映")
    ap.add_argument("--no-fill-null-text", action="store_true", help="NULL 補完を無効化")
    ap.add_argument("--batch-size", type=int, default=2000)
    args = ap.parse_args()

    has_action = bool(args.report or args.emit_sql or args.apply)

    lookup, _ = build_csv_lookup_tables()
    LOG.info("インデックス構築完了: simple_id=%d norm_id=%d corp=%d attr=%d", len(lookup.by_simple_id), len(lookup.by_norm_id), len(lookup.by_corp), len(lookup.by_attr))

    conn = get_connection()
    try:
        db_rows = fetch_companies_np(conn.cursor())
        fixes, report = compute_fixes(lookup, db_rows, not args.no_fill_null_text)
        LOG.info("DB np* 行数: %d, 修復候補: %d", len(db_rows), len(fixes))

        if args.report:
            with open(args.report, "w", encoding="utf-8") as f:
                for ev in report: f.write(json.dumps(ev, ensure_ascii=False, default=str) + "\n")
        if args.emit_sql:
            emit_sql(fixes, Path(args.emit_sql), args.batch_size)
        if args.apply:
            n = apply_fixes(conn, fixes, args.batch_size)
            LOG.info("適用完了: %d 行", n)

        print_console_summary(len(db_rows), len(fixes), report, summary_only=not has_action)
    finally:
        conn.close()

if __name__ == "__main__":
    main()