#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EDINET「関係会社の状況」取得スクリプト

指定した上場企業の証券コードを元に、EDINET APIから最新の有価証券報告書を取得し、
「関係会社の状況」テーブルをスクレイピングする。

出力項目: 親会社名, 子会社名, 住所, 議決権所有割合

使い方:
  python scripts/fetch_edinet_relations.py --all-listed --days 365 --use-fiscal  # 決算日絞り込み+キャッシュ
  python scripts/fetch_edinet_relations.py --doc-id-csv data/docid_map.csv  # DocID対応表で即ダウンロード
  python scripts/fetch_edinet_relations.py 4578 2181 7270
  python scripts/fetch_edinet_relations.py --doc-id S100XXXX 親会社名

必要:
  - 環境変数 EDINET_API_KEY（EDINET APIキー）
  - pip install requests pandas
"""

import argparse
import csv
import io
import json
import os
import random
import re
import shutil
import sys
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Union
from xml.etree import ElementTree as ET

try:
    import requests
except ImportError:
    print("エラー: requests をインストールしてください: pip install requests")
    sys.exit(1)

# ------------------------------
# 設定（EDINET API Version 2 仕様書 2026.1版準拠）
# ------------------------------
EDINET_API_BASE = "https://disclosure.edinet-fsa.go.jp/api/v2"  # 書類一覧
EDINET_DOCUMENT_BASE = "https://api.edinet-fsa.go.jp/api/v2"  # 書類取得（ダウンロード）
OUTPUT_CSV = "data/edinet_relations.csv"
PROCESSED_CODES_FILE = "data/processed_codes.txt"
EDINET_CODE_LIST_PATH = "data/EdinetcodeDlInfo.csv"
EDINET_CACHE_DIR = "data/cache"  # 書類一覧JSONのキャッシュ（YYYY-MM-DD.json）
SLEEP_MIN, SLEEP_MAX = 1.0, 2.0  # 書類ZIP取得ごとの待機（秒）
LIST_FETCH_SLEEP_MIN, LIST_FETCH_SLEEP_MAX = 2.0, 4.0  # 日付一覧取得ごとの待機（秒）
BATCH_BREAK_COMPANIES = 100  # この企業数ごとに長めの休憩
BATCH_BREAK_SEC = 60  # 長めの休憩（秒）
REQUEST_TIMEOUT = 60  # 書類一覧取得のタイムアウト（秒）
DOWNLOAD_TIMEOUT = 120  # 書類ZIP取得のタイムアウト（秒）
DOC_TYPE_YUKASHOKEN = "120"  # 有価証券報告書（文字列で比較）


def is_doc_type_yukashoken(doc_type_code: Optional[Union[str, int]]) -> bool:
    """docTypeCode が有価証券報告書(120) か。APIが文字列・数値のどちらで返しても一致するよう厳密に判定。"""
    if doc_type_code is None:
        return False
    return str(doc_type_code).strip() == DOC_TYPE_YUKASHOKEN


# ブラウザ風User-Agent（403回避用）
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


@dataclass
class RelationRow:
    """関係会社1件のデータ"""
    parent_name: str
    subsidiary_name: str
    address: str
    voting_rights_pct: str


def normalize_company_name(name: str) -> str:
    """企業名を正規化（(株)→株式会社など）"""
    if not name or not isinstance(name, str):
        return ""
    return (
        name.replace("（株）", "株式会社")
        .replace("(株)", "株式会社")
        .replace("㈱", "株式会社")
        .replace("（有）", "有限会社")
        .replace("(有)", "有限会社")
        .replace("（合）", "合資会社")
        .replace("(合)", "合資会社")
        .replace("（名）", "合名会社")
        .replace("(名)", "合名会社")
        .strip()
    )


def parse_voting_rights(value: str) -> str:
    """議決権所有割合を数値文字列に正規化"""
    if not value:
        return ""
    # 「100.00」「100%」「100％」などから数値部分を抽出
    m = re.search(r"(\d+(?:\.\d+)?)\s*[%％]?", str(value).strip())
    return m.group(1) if m else ""


def get_api_key() -> Optional[str]:
    return os.getenv("EDINET_API_KEY")


def load_listed_with_fiscal(path: str = EDINET_CODE_LIST_PATH) -> list[tuple[str, int]]:
    """
    EdinetcodeDlInfo.csv から「上場」企業の (証券コード4桁, 決算月) を抽出
    決算月: 1-12（決算日列が「3」「03」「3月」などから抽出）
    """
    result: list[tuple[str, int]] = []
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"EDINETコードリストが見つかりません: {path}")

    for enc in ("cp932", "shift_jis", "utf-8-sig", "utf-8"):
        try:
            text = p.read_text(encoding=enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise ValueError(f"エンコーディングが判読できません: {path}")

    lines = text.strip().splitlines()
    if len(lines) < 3:
        return []
    reader = csv.reader(io.StringIO("\n".join(lines[1:])))
    header = next(reader)
    idx_listing = next((i for i, h in enumerate(header) if "上場区分" in (h or "")), 2)
    idx_sec = next((i for i, h in enumerate(header) if "証券コード" in (h or "")), 11)
    idx_fiscal = next((i for i, h in enumerate(header) if "決算" in (h or "")), 5)

    seen: set[str] = set()
    for row in reader:
        if len(row) <= max(idx_listing, idx_sec, idx_fiscal):
            continue
        listing = (row[idx_listing] or "").strip()
        sec = (row[idx_sec] or "").strip()
        fiscal_raw = (row[idx_fiscal] or "").strip()
        if listing != "上場":
            continue
        if not sec or not re.match(r"^\d{4,5}$", sec):
            continue
        code = sec[:4] if len(sec) >= 4 else sec.zfill(4)
        if code in seen:
            continue
        seen.add(code)
        # 決算月を抽出（「3」「03」「3月」「2024-03-31」など）
        fiscal_month = 3  # デフォルト（3月決算が多い）
        if fiscal_raw:
            m = re.search(r"(\d{1,2})", fiscal_raw)
            if m:
                fm = int(m.group(1))
                if 1 <= fm <= 12:
                    fiscal_month = fm
        result.append((code, fiscal_month))
    return result

def build_dates_from_fiscal(fiscal_list: list[tuple[str, int]], months_after: int = 4) -> list[str]:
    """
    決算日から「提出窓口」を算出（決算月の翌月〜+months_afterヶ月）
    有価証券報告書は決算後3〜4ヶ月以内に提出されることが多い
    """
    today = datetime.now().date()
    dates_set: set[str] = set()
    fiscal_months = {fm for _, fm in fiscal_list}
    for fm in fiscal_months:
        for year_offset in (0, 1):
            y = today.year - year_offset
            # 提出窓口: 決算翌月(1)〜決算+(months_after)ヶ月
            for m_offset in range(1, months_after + 1):
                m = fm + m_offset
                if m > 12:
                    m -= 12
                    yy = y + 1
                else:
                    yy = y
                if yy > today.year or (yy == today.year and m > today.month):
                    continue
                last_day = 28 if m == 2 else (30 if m in (4, 6, 9, 11) else 31)
                for d in range(1, last_day + 1):
                    dt = datetime(yy, m, d).date()
                    if dt <= today:
                        dates_set.add(dt.strftime("%Y-%m-%d"))
    return sorted(dates_set, reverse=True)


def load_listed_sec_codes_from_edinet_list(path: str = EDINET_CODE_LIST_PATH) -> list[str]:
    """
    EdinetcodeDlInfo.csv から「上場」かつ証券コードありの4桁コードを抽出
    Shift-JIS/CP932 対応、1行目はメタ、2行目はヘッダー
    """
    codes: list[str] = []
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"EDINETコードリストが見つかりません: {path}")

    for enc in ("cp932", "shift_jis", "utf-8-sig", "utf-8"):
        try:
            text = p.read_text(encoding=enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise ValueError(f"エンコーディングが判読できません: {path}")

    lines = text.strip().splitlines()
    if len(lines) < 3:
        return []
    # 2行目がヘッダー（0-indexedで1）、3行目以降がデータ
    reader = csv.reader(io.StringIO("\n".join(lines[1:])))
    header = next(reader)
    # 列インデックス: 上場区分=2, 証券コード=11（仕様に基づく）
    idx_listing = next((i for i, h in enumerate(header) if "上場区分" in (h or "")), 2)
    idx_sec = next((i for i, h in enumerate(header) if "証券コード" in (h or "")), 11)

    for row in reader:
        if len(row) <= max(idx_listing, idx_sec):
            continue
        listing = (row[idx_listing] or "").strip()
        sec = (row[idx_sec] or "").strip()
        if listing != "上場":
            continue
        if not sec or not re.match(r"^\d{4,5}$", sec):
            continue
        code = sec[:4] if len(sec) >= 4 else sec.zfill(4)
        if code not in codes:
            codes.append(code)
    return codes


def load_processed_codes(path: str = PROCESSED_CODES_FILE) -> set[str]:
    """処理済み証券コードを読み込み"""
    p = Path(path)
    if not p.exists():
        return set()
    return {line.strip() for line in p.read_text(encoding="utf-8").splitlines() if line.strip() and line.strip().isdigit()}


def save_processed_code(sec_code: str, path: str = PROCESSED_CODES_FILE) -> None:
    """処理済み証券コードを1件追記"""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(sec_code + "\n")


def _default_headers() -> dict:
    """APIリクエスト用の共通ヘッダー（User-Agent 等）"""
    return {
        "User-Agent": os.getenv("EDINET_USER_AGENT", DEFAULT_USER_AGENT),
        "Accept": "application/json",
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    }


def _base_params() -> dict:
    """APIキーをクエリパラメータで返す（Version 2: Subscription-Key）。呼び出し側でAPIキー必須を確認すること。"""
    api_key = get_api_key()
    return {"Subscription-Key": api_key} if api_key else {}


def fetch_documents_list(date_str: str) -> Optional[dict]:
    """書類一覧APIを呼び出し（/api/v2/documents.json）"""
    url = f"{EDINET_API_BASE}/documents.json"
    params = {"date": date_str, "type": "2", **_base_params()}
    headers = _default_headers()

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"⚠️ 書類一覧取得エラー ({date_str}): {e}")
        return None


def is_valid_cache_data(data: Optional[dict]) -> bool:
    """
    書類一覧APIのレスポンスが有効か判定。
    results 配列が存在しリストであることを確認。エラーJSONや空データは無効。
    """
    if not data or not isinstance(data, dict):
        return False
    results = data.get("results")
    return isinstance(results, list)


def ensure_dates_cached(dates: list[str], use_cache: bool = True) -> None:
    """
    STEP1&2: 指定日付の書類一覧を1日1リクエストで取得し data/cache/YYYY-MM-DD.json に保存。
    use_cache=True のとき既存ファイルはスキップ。無効なキャッシュは削除して再取得対象にする。
    取得ごとに random.uniform(2.0, 4.0) 秒待機。
    """
    cache_dir = Path(EDINET_CACHE_DIR)
    cache_dir.mkdir(parents=True, exist_ok=True)

    if use_cache:
        for date_str in dates:
            cache_file = cache_dir / f"{date_str}.json"
            if not cache_file.exists():
                continue
            try:
                data = json.loads(cache_file.read_text(encoding="utf-8"))
                if not is_valid_cache_data(data):
                    cache_file.unlink()
            except Exception:
                try:
                    cache_file.unlink()
                except Exception:
                    pass
    to_fetch = dates if not use_cache else [d for d in dates if not (cache_dir / f"{d}.json").exists()]
    if not to_fetch:
        return
    print(f"📡 書類一覧を取得します（{'全件' if not use_cache else '未キャッシュ'} {len(to_fetch)} 日、1日1リクエスト）...")
    for i, date_str in enumerate(to_fetch):
        data = fetch_documents_list(date_str)
        if data:
            (cache_dir / f"{date_str}.json").write_text(
                json.dumps(data, ensure_ascii=False, indent=None), encoding="utf-8"
            )
        if i < len(to_fetch) - 1:
            time.sleep(random.uniform(LIST_FETCH_SLEEP_MIN, LIST_FETCH_SLEEP_MAX))


def _normalize_sec_code(raw: Optional[Union[str, int]]) -> str:
    """
    APIの secCode（5桁数値・文字列など）を4桁文字列に正規化。
    EDINETは "45780" や 45780 で返すことがあるため、先頭4桁で比較用に統一。
    """
    s = str(raw).strip() if raw is not None else ""
    if not s:
        return ""
    # 数字のみにし、先頭4桁（上場銘柄は4桁）
    digits = "".join(c for c in s if c.isdigit())
    return digits[:4].zfill(4) if digits else ""


def build_sec_to_doc_map_from_full_cache(sec_codes: set[str]) -> dict[str, dict]:
    """
    全キャッシュ統合検索: data/cache/*.json をすべて読み込み、
    証券コード一致かつ docTypeCode==120（有報）の書類を抽出。API呼び出しなし。
    5桁(84730)と4桁(8473)のマッチングは _normalize_sec_code で先頭4桁に統一して比較。
    同一証券コードに複数有報がある場合は periodEnd が新しいものを採用。
    """
    sec_set = {str(c).zfill(4) for c in sec_codes}
    result: dict[str, dict] = {}
    cache_dir = Path(EDINET_CACHE_DIR)
    if not cache_dir.exists():
        return result
    json_files = sorted(cache_dir.glob("*.json"))
    first_dumped = False
    for cache_file in json_files:
        try:
            data = json.loads(cache_file.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not is_valid_cache_data(data):
            continue
        results = data.get("results", [])
        if results and not first_dumped:
            first_dumped = True
            sample = results[0]
            sd, ss = sample.get("docTypeCode"), sample.get("secCode")
            print(f"  [キャッシュ確認] {cache_file.name} 先頭1件: secCode={repr(ss)} (正規化→{_normalize_sec_code(ss)}), docTypeCode={repr(sd)}, is_120={is_doc_type_yukashoken(sd)}")
        for r in results:
            if not is_doc_type_yukashoken(r.get("docTypeCode")):
                continue
            sec = _normalize_sec_code(r.get("secCode"))
            if not sec or sec not in sec_set:
                continue
            # 同一証券コードで既存あり：periodEnd が新しい方を採用
            existing = result.get(sec)
            new_end = r.get("periodEnd") or ""
            if existing:
                old_end = existing.get("periodEnd") or ""
                if new_end <= old_end:
                    continue
            result[sec] = {
                "docID": r.get("docID"),
                "filerName": r.get("filerName"),
                "secCode": sec,
                "periodStart": r.get("periodStart"),
                "periodEnd": r.get("periodEnd"),
            }
    return result


def fetch_documents_list_cached(date_str: str) -> Optional[dict]:
    """
    書類一覧を取得（ディスクキャッシュ優先）。
    キャッシュヒット時はAPI呼び出し・スリープなし。
    """
    cache_file = Path(EDINET_CACHE_DIR) / f"{date_str}.json"
    if cache_file.exists():
        try:
            return json.loads(cache_file.read_text(encoding="utf-8"))
        except Exception:
            pass
    _request_sleep()
    return fetch_documents_list(date_str)


def _request_sleep() -> None:
    """1リクエストごとの待機（1〜2秒）"""
    time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))


def build_sec_to_doc_map(sec_codes: set[str], dates_to_fetch: list[str], use_cache: bool = True) -> dict[str, dict]:
    """
    指定日付群を1日1リクエストで取得し、証券コード→書類情報のマップを構築
    キャッシュ利用時は data/edinet_cache/YYYY-MM-DD.json を優先
    docTypeCode==120（有価証券報告書）のみ抽出
    """
    sec_set = {str(c).zfill(4) for c in sec_codes}
    result: dict[str, dict] = {}
    fetch_fn = fetch_documents_list_cached if use_cache else fetch_documents_list

    for i, date_str in enumerate(dates_to_fetch):
        if not use_cache:
            _request_sleep()
        data = fetch_fn(date_str)
        if not data or "results" not in data:
            continue
        for r in data.get("results", []):
            if not is_doc_type_yukashoken(r.get("docTypeCode")):
                continue
            sec = _normalize_sec_code(r.get("secCode"))
            if not sec or sec not in sec_set or sec in result:
                continue
            result[sec] = {
                "docID": r.get("docID"),
                "filerName": r.get("filerName"),
                "secCode": sec,
                "periodStart": r.get("periodStart"),
                "periodEnd": r.get("periodEnd"),
            }
    return result


def load_doc_id_csv(path: str) -> list[tuple[str, str, str]]:
    """
    DocID対応表CSVを読み込み (sec_code, doc_id, filer_name) のリストを返す
    ヘッダー: sec_code,doc_id,filer_name または sec_code,doc_id
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"DocID対応表が見つかりません: {path}")
    rows: list[tuple[str, str, str]] = []
    with open(p, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            sec = (r.get("sec_code") or r.get("証券コード") or r.get("secCode") or "").strip()
            doc_id = (r.get("doc_id") or r.get("docID") or r.get("docid") or "").strip()
            filer = (r.get("filer_name") or r.get("提出者名") or r.get("filerName") or "").strip()
            if sec and doc_id:
                sec4 = sec[:4] if len(sec) >= 4 else sec.zfill(4)
                rows.append((sec4, doc_id, filer or f"証券コード{sec4}"))
    return rows


def fetch_document_zip(doc_id: str, doc_type: str = "1") -> Optional[bytes]:
    """
    書類取得APIでZIPをダウンロード（Version 2: api.edinet-fsa.go.jp, Subscription-Key をクエリに付与）
    doc_type: 1=XBRL, 5=CSV（仕様書に基づく）
    """
    url = f"{EDINET_DOCUMENT_BASE}/documents/{doc_id}"
    params = {"type": doc_type, **_base_params()}
    headers = _default_headers()

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=DOWNLOAD_TIMEOUT)
        resp.raise_for_status()

        content_type = (resp.headers.get("Content-Type") or "").split(";")[0].strip().lower()
        if content_type == "application/json":
            print(f"⚠️ 書類取得がJSONを返しました (docID={doc_id}, type={doc_type})。サーバー拒否の可能性:")
            print(resp.text[:2000] if len(resp.text) > 2000 else resp.text)
            return None

        raw = resp.content
        # バイナリ整合性チェック: JSON（エラーメッセージ）になっていないか厳格に判定
        stripped = raw.strip()
        if stripped.startswith(b"{") or stripped.startswith(b"["):
            preview = raw.decode("utf-8", errors="replace")
            print(f"⚠️ 書類取得がJSON形式を返しました (docID={doc_id}, type={doc_type})。エラー応答の可能性:")
            print(preview[:3000] if len(preview) > 3000 else preview)
            return None
        if not zipfile.is_zipfile(io.BytesIO(raw)):
            preview = raw.decode("utf-8", errors="replace")
            print(f"⚠️ ZIP形式ではありません (docID={doc_id}, type={doc_type})。APIエラーJSONの可能性:")
            print("レスポンス先頭500文字:", preview[:500])
            return None

        return raw
    except requests.RequestException as e:
        print(f"⚠️ 書類ダウンロードエラー (docID={doc_id}): {e}")
        if hasattr(e, "response") and e.response is not None and getattr(e.response, "text", None):
            print("レスポンス:", e.response.text[:1500])
        return None


def fetch_document_zip_with_fallback(doc_id: str, prefer_csv: bool = True) -> tuple[Optional[bytes], str]:
    """
    type=5(CSV) と type=1(XBRL) を試行してZIPを取得。
    prefer_csv=True のとき CSV を優先（Version 2正式サポート、構造が単純で抽出ミスが減る）。
    戻り値: (zip_bytes, "csv"|"xbrl"|"") 取得に失敗した場合は (None, "")
    """
    if prefer_csv:
        data = fetch_document_zip(doc_id, doc_type="5")
        if data is not None:
            return (data, "csv")
        data = fetch_document_zip(doc_id, doc_type="1")
        if data is not None:
            return (data, "xbrl")
    else:
        data = fetch_document_zip(doc_id, doc_type="1")
        if data is not None:
            return (data, "xbrl")
        data = fetch_document_zip(doc_id, doc_type="5")
        if data is not None:
            return (data, "csv")
    return (None, "")


def parse_xbrl_for_relations(zip_bytes: bytes, parent_name: str, debug: bool = False) -> list[RelationRow]:
    """
    XBRL ZIPから関係会社の状況を抽出
    タクソノミの子会社・関連会社関連要素を探す
    """
    rows: list[RelationRow] = []
    namespaces = {
        "xbrli": "http://www.xbrl.org/2003/instance",
        "jpdei": "http://disclosure.edinet-fsa.go.jp/taxonomy/jpdei/2024-12-01/jpdei",
        "jpcrp": "http://disclosure.edinet-fsa.go.jp/taxonomy/jpcrp/2024-12-01/jpcrp",
        "jppfs": "http://disclosure.edinet-fsa.go.jp/taxonomy/jppfs/2024-12-01/jppfs",
        "tse": "http://www.xbrl.tdnet.info/jp/tse/tdnet/t/2024-12-01/tse-t-2024-12-01",
        "xlink": "http://www.w3.org/1999/xlink",
    }
    # 関係会社の名称・住所・議決権の要素名パターン（タクソノミ年版で変わる可能性あり）
    name_patterns = [
        "SubsidiaryCompanyName",
        "RelatedCompanyName",
        "SubsidiaryCompanyNameOfListedCompany",
        "NameOfSubsidiaryCompany",
        "NameOfRelatedCompany",
    ]
    addr_patterns = ["SubsidiaryCompanyAddress", "RelatedCompanyAddress", "AddressOfSubsidiaryCompany"]
    ratio_patterns = [
        "VotingRightsOwnedPercent",
        "OwnershipOfVotingRights",
        "PercentageOfVotingRights",
        "EquityMethodInvestmentRatio",
    ]

    xbrl_count = 0
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
            for name in zf.namelist():
                if not name.lower().endswith(".xbrl") and not name.lower().endswith(".xml"):
                    continue
                xbrl_count += 1
                try:
                    content = zf.read(name)
                except Exception:
                    continue
                # 名前空間を登録してパース
                try:
                    root = ET.fromstring(content)
                except ET.ParseError:
                    continue

                # 全要素を走査（名前空間付き）
                for elem in root.iter():
                    tag = elem.tag
                    if "}" in tag:
                        local = tag.split("}")[-1]
                    else:
                        local = tag

                    text = (elem.text or "").strip()
                    if not text:
                        continue

                    if any(p in local for p in name_patterns):
                        row = RelationRow(
                            parent_name=parent_name,
                            subsidiary_name=text,
                            address="",
                            voting_rights_pct="",
                        )
                        rows.append(row)
                    elif rows and any(p in local for p in addr_patterns):
                        rows[-1].address = text
                    elif rows and any(p in local for p in ratio_patterns):
                        pct = parse_voting_rights(text)
                        if pct:
                            rows[-1].voting_rights_pct = pct

    except Exception as e:
        print(f"⚠️ XBRLパースエラー: {e}")

    if debug:
        print(f"    [XBRL] 処理したXBRL/XMLファイル数: {xbrl_count}, 抽出件数: {len(rows)}")
    return rows


def parse_csv_for_relations(zip_bytes: bytes, parent_name: str, debug: bool = False) -> list[RelationRow]:
    """
    ZIP内のXBRL_TO_CSV形式CSVから関係会社を抽出
    （EDINETサイトからダウンロードしたCSV形式に対応）
    """
    rows: list[RelationRow] = []
    current_subsidiary = ""
    current_address = ""
    current_ratio = ""

    csv_count = 0
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
            for name in zf.namelist():
                if not name.lower().endswith(".csv"):
                    continue
                csv_count += 1
                try:
                    content = zf.read(name)
                except Exception:
                    continue
                # エンコーディング検出
                for enc in ("utf-8", "utf-8-sig", "utf-16", "cp932", "shift_jis"):
                    try:
                        text = content.decode(enc)
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    continue

                # タブ区切りを想定
                delim = "\t" if "\t" in text[:500] else ","
                reader = csv.DictReader(io.StringIO(text), delimiter=delim)
                if not reader.fieldnames:
                    continue

                # 項目名・値の列を特定
                item_key = next((k for k in reader.fieldnames if "項目" in k or "要素" in k), "項目名")
                value_key = next((k for k in reader.fieldnames if k in ("値", "value", "Value")), "値")

                for rec in reader:
                    item = (rec.get(item_key) or "").strip()
                    val = (rec.get(value_key) or "").strip()
                    if not item or not val:
                        continue

                    if "子会社" in item or "関連会社" in item:
                        if "名称" in item or "名前" in item or "Name" in item:
                            if current_subsidiary:
                                rows.append(
                                    RelationRow(
                                        parent_name=parent_name,
                                        subsidiary_name=normalize_company_name(current_subsidiary),
                                        address=current_address,
                                        voting_rights_pct=current_ratio,
                                    )
                                )
                            current_subsidiary = val
                            current_address = ""
                            current_ratio = ""
                        elif "持株" in item or "議決権" in item or "比率" in item or "EquityRatio" in item:
                            current_ratio = parse_voting_rights(val) or current_ratio
                        elif "所在地" in item or "住所" in item or "Address" in item:
                            current_address = val

                if current_subsidiary:
                    rows.append(
                        RelationRow(
                            parent_name=parent_name,
                            subsidiary_name=normalize_company_name(current_subsidiary),
                            address=current_address,
                            voting_rights_pct=current_ratio,
                        )
                    )

    except Exception as e:
        print(f"⚠️ CSVパースエラー: {e}")

    if debug:
        print(f"    [CSV] 処理したCSVファイル数: {csv_count}, 抽出件数: {len(rows)}")
    return rows


def extract_relations(
    zip_bytes: bytes,
    parent_name: str,
    debug: bool = False,
    fetched_format: str = "",
) -> list[RelationRow]:
    """
    ZIPから関係会社を抽出。
    CSVを優先し、見つからない場合はXBRLを試行（Version 2正式サポートのCSVは構造が単純）。
    debug=True のときZIP内ファイル一覧と抽出ステップをログ出力。
    """
    if debug:
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
                names = zf.namelist()
                print(f"    [展開] ZIP内ファイル数: {len(names)}")
                for i, n in enumerate(names[:20]):
                    print(f"      - {n}")
                if len(names) > 20:
                    print(f"      ... 他 {len(names) - 20} 件")
        except Exception as e:
            print(f"    [展開] ZIP読み込みエラー: {e}")

    # CSV優先（type=5取得時にCSVのみ含まれるZIP）
    rows = parse_csv_for_relations(zip_bytes, parent_name, debug=debug)
    if debug and not rows:
        print(f"    [抽出] CSV解析: 0件 → XBRLを試行")
    if not rows:
        rows = parse_xbrl_for_relations(zip_bytes, parent_name, debug=debug)
    if debug:
        print(f"    [抽出] 結果: {len(rows)} 件 (取得形式: {fetched_format or '不明'})")
    return rows


def append_to_csv(rows: list[RelationRow], output_file: str = OUTPUT_CSV):
    """edinet_relations.csvに追記（ヘッダーは初回のみ）"""
    p = Path(output_file)
    p.parent.mkdir(parents=True, exist_ok=True)
    file_exists = p.exists()

    with open(output_file, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(["親会社名", "子会社名", "住所", "議決権所有割合"])
        for r in rows:
            w.writerow([
                normalize_company_name(r.parent_name),
                normalize_company_name(r.subsidiary_name),
                r.address,
                r.voting_rights_pct,
            ])


def main():
    parser = argparse.ArgumentParser(description="EDINET関係会社の状況を取得")
    parser.add_argument("codes", nargs="*", help="証券コード（例: 4578 2181 7270）")
    parser.add_argument("--codes", type=str, dest="codes_str", help="カンマ区切り証券コード")
    parser.add_argument("--all-listed", action="store_true", help="EdinetcodeDlInfo.csvから上場企業を自動リストアップ")
    parser.add_argument("--doc-id", type=str, dest="doc_id", help="書類IDを直接指定（一覧APIをスキップ）")
    parser.add_argument("--doc-id-csv", type=str, dest="doc_id_csv", help="証券コード-DocID対応表CSV（sec_code,doc_id,filer_name）日付検索を完全スキップ")
    parser.add_argument("--days", type=int, default=1, help="書類検索期間（過去何日分を遡るか、デフォルト1=直近1日のみ）")
    parser.add_argument("--use-fiscal", action="store_true", help="--all-listed時、決算日から提出窓口（4ヶ月）に絞って検索（API呼び出し削減）")
    parser.add_argument("--no-cache", action="store_true", help="書類一覧のディスクキャッシュを無効化")
    parser.add_argument("--force-refresh-cache", action="store_true", help="data/cache/ を無視し、v2エンドポイントから書類一覧を再取得して上書き")
    parser.add_argument("--clear-cache", action="store_true", help="実行開始時に data/cache/ を削除してから処理を開始")
    parser.add_argument("--output", type=str, default=OUTPUT_CSV, help="出力CSVパス")
    parser.add_argument("--overwrite", action="store_true", help="既存CSVを上書きして新規作成（通常は追記）")
    parser.add_argument("--no-resume", action="store_true", help="処理済み記録を無視して最初から実行")
    parser.add_argument("--debug", action="store_true", help="1社に絞って詳細解析モード（ZIP内ファイル一覧、抽出ステップをログ出力）")
    args = parser.parse_args()

    if args.codes_str:
        codes = [c.strip() for c in args.codes_str.split(",") if c.strip()]
    elif args.all_listed:
        try:
            codes = load_listed_sec_codes_from_edinet_list()
            print(f"📋 EDINETコードリストから上場企業 {len(codes)} 社を読み込みました")
        except FileNotFoundError as e:
            print(f"❌ {e}")
            print("   data/EdinetcodeDlInfo.csv を EDINET サイトからダウンロードしてください。")
            sys.exit(1)
        except Exception as e:
            print(f"❌ コードリスト読み込みエラー: {e}")
            sys.exit(1)
    else:
        codes = [str(c).strip() for c in args.codes if c]

    # --doc-id モード / --doc-id-csv モード
    doc_id_direct = (args.doc_id or "").strip()
    doc_id_csv_path = (args.doc_id_csv or "").strip()
    use_doc_id_mode = bool(doc_id_direct)
    use_doc_id_csv_mode = bool(doc_id_csv_path)

    if not use_doc_id_mode and not use_doc_id_csv_mode and not codes:
        print("証券コード、--codes、または --all-listed を指定してください。")
        print("例: python fetch_edinet_relations.py 4578 2181 7270")
        print("例: python fetch_edinet_relations.py --all-listed --days 365")
        sys.exit(1)

    # APIキー必須（EDINET API v2: Subscription-Key をクエリパラメータで送信）
    if not get_api_key():
        print("❌ 環境変数 EDINET_API_KEY が未設定です。")
        print("   EDINET API キー取得: https://disclosure.edinet-fsa.go.jp/api/auth/index.aspx?mode=1")
        print("   設定例: export EDINET_API_KEY=your_key")
        sys.exit(1)

    output_path = args.output

    # 処理済みコード（--all-listed 時のレジューム用）
    processed = set() if args.no_resume else load_processed_codes()
    if processed and not use_doc_id_mode and not use_doc_id_csv_mode:
        codes = [c for c in codes if c not in processed]
        print(f"⏭ 処理済み {len(processed)} 件をスキップ、残り {len(codes)} 件を処理します")

    # CSVはデフォルトで追記。--overwrite 時のみ上書き
    if args.overwrite and Path(output_path).exists():
        Path(output_path).unlink()
        print(f"既存CSVを削除: {output_path}")

    total = 0
    request_count = 0

    if use_doc_id_mode:
        # 書類ID直接指定モード（一覧APIをスキップ）
        parent = codes[0] if codes else "不明"
        doc_id = doc_id_direct
        print(f"\n📋 書類ID直接指定モード: {doc_id} (親会社名: {parent})")
        _request_sleep()
        zip_bytes, fmt = fetch_document_zip_with_fallback(doc_id)
        if zip_bytes:
            relations = extract_relations(zip_bytes, parent, debug=args.debug, fetched_format=fmt)
            seen = set()
            unique = []
            for r in relations:
                key = (r.parent_name, r.subsidiary_name, r.address, r.voting_rights_pct)
                if key not in seen and r.subsidiary_name:
                    seen.add(key)
                    unique.append(r)
            if unique:
                append_to_csv(unique, output_path)
                total = len(unique)
                print(f"  ✅ {total} 件を抽出・保存")
            else:
                print(f"  ⚠️ 関係会社データが抽出できませんでした")
    elif use_doc_id_csv_mode:
        # DocID対応表CSVモード（日付検索を完全スキップ、書類取得APIのみ）
        try:
            doc_list = load_doc_id_csv(doc_id_csv_path)
        except FileNotFoundError as e:
            print(f"❌ {e}")
            sys.exit(1)
        print(f"📋 DocID対応表読み込み: {len(doc_list)} 社（日付検索スキップ）")
        total = 0
        for idx, (sec, doc_id, parent) in enumerate(doc_list):
            if sec in processed:
                continue
            if idx > 0 and idx % BATCH_BREAK_COMPANIES == 0:
                print(f"  ⏸  {BATCH_BREAK_SEC}秒休憩（{idx}社処理済み）")
                time.sleep(BATCH_BREAK_SEC)
            _request_sleep()
            zip_bytes, fmt = fetch_document_zip_with_fallback(doc_id)
            if not zip_bytes:
                print(f"[{idx + 1}/{len(doc_list)}] {sec} ダウンロード失敗")
                save_processed_code(sec)
                continue
            do_debug = args.debug and (idx < 1 or len(doc_list) == 1)
            relations = extract_relations(zip_bytes, parent, debug=do_debug, fetched_format=fmt)
            seen = set()
            unique = [r for r in relations if r.subsidiary_name and (k := (r.parent_name, r.subsidiary_name, r.address, r.voting_rights_pct)) not in seen and not seen.add(k)]
            if unique:
                append_to_csv(unique, output_path)
                total += len(unique)
                print(f"[{idx + 1}/{len(doc_list)}] {sec} → {len(unique)}件保存")
            else:
                print(f"[{idx + 1}/{len(doc_list)}] {sec} → 関係会社データなし")
            save_processed_code(sec)
    else:
        # 一括キャッシュ→オフライン検索→ピンポイント取得
        if args.clear_cache:
            cache_dir = Path(EDINET_CACHE_DIR)
            if cache_dir.exists():
                shutil.rmtree(cache_dir)
                print(f"🗑 data/cache/ を削除しました")
            cache_dir.mkdir(parents=True, exist_ok=True)

        sec_set = {str(c).zfill(4) for c in codes}
        if args.all_listed and args.use_fiscal:
            try:
                fiscal_list = load_listed_with_fiscal()
                fiscal_map = {sec: fm for sec, fm in fiscal_list if sec in sec_set}
                dates_to_fetch = build_dates_from_fiscal(list(fiscal_map.items()))
                print(f"📅 検索対象: 決算日ベース絞り込み {len(dates_to_fetch)} 日分")
            except Exception as e:
                print(f"⚠️ 決算日読み込み失敗、--days にフォールバック: {e}")
                dates_to_fetch = [(datetime.now().date() - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(args.days)]
                print(f"📅 検索対象: 過去{args.days}日分")
        else:
            dates_to_fetch = [(datetime.now().date() - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(args.days)]
            print(f"📅 検索対象: 過去{args.days}日分（キャッシュ: {'無効' if args.no_cache else '有効'}）")
        use_cache = not args.no_cache and not args.force_refresh_cache
        if args.force_refresh_cache:
            print("🔄 --force-refresh-cache: 既存キャッシュを無視し、書類一覧をv2から再取得します")
        ensure_dates_cached(dates_to_fetch, use_cache=use_cache)
        doc_map = build_sec_to_doc_map_from_full_cache(sec_set)
        cache_count = len(list(Path(EDINET_CACHE_DIR).glob("*.json"))) if Path(EDINET_CACHE_DIR).exists() else 0
        print(f"   有価証券報告書(docTypeCode=120): {len(doc_map)} 社分を検出（全キャッシュ統合スキャン、{cache_count} ファイル）")

        # --debug 時は1社に絞って詳細解析
        codes_to_process = codes[:1] if args.debug and codes else codes
        if args.debug and len(codes) > 1:
            print(f"🔍 --debug: 1社に絞って詳細解析（{codes[0]}）")

        for idx, sec in enumerate(codes_to_process):
            sec4 = str(sec).zfill(4)
            doc_info = doc_map.get(sec4)
            if not doc_info:
                print(f"[{idx + 1}/{len(codes_to_process)}] {sec} 検索中... 未発見")
                # 1年分の全キャッシュをスキャンして有報が存在しなかった場合のみ処理済みにする
                save_processed_code(sec)
                continue

            doc_id = doc_info.get("docID")
            parent = doc_info.get("filerName") or f"証券コード{sec}"

            # 100企業ごとに休憩（企業数ベース）
            if idx > 0 and idx % BATCH_BREAK_COMPANIES == 0:
                print(f"  ⏸  {BATCH_BREAK_SEC}秒休憩（{idx}社処理済み）")
                time.sleep(BATCH_BREAK_SEC)

            _request_sleep()
            zip_bytes, fmt = fetch_document_zip_with_fallback(doc_id)
            if not zip_bytes:
                print(f"[{idx + 1}/{len(codes_to_process)}] {sec} 検索中... 発見 → ダウンロード失敗")
                # ダウンロード失敗時は処理済みにしない（次回リトライする）
                continue

            relations = extract_relations(zip_bytes, parent, debug=args.debug, fetched_format=fmt)
            seen = set()
            unique = []
            for r in relations:
                key = (r.parent_name, r.subsidiary_name, r.address, r.voting_rights_pct)
                if key not in seen and r.subsidiary_name:
                    seen.add(key)
                    unique.append(r)

            if unique:
                append_to_csv(unique, output_path)
                total += len(unique)
                print(f"[{idx + 1}/{len(codes_to_process)}] {sec} 検索中... 発見 → {len(unique)}件保存")
                save_processed_code(sec)
            else:
                print(f"[{idx + 1}/{len(codes_to_process)}] {sec} 検索中... 発見 → 関係会社データなし")
                # 有報は見つかったが抽出0件の場合は処理済みにしない（抽出ロジック改善後にリトライ）

    print(f"\n📁 出力: {output_path}")
    print(f"📊 合計 {total} 件を追加しました")


if __name__ == "__main__":
    main()
