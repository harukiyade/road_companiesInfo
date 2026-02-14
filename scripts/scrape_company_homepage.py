#!/usr/bin/env python3
"""
公式サイトの特定およびHP情報のディープ・スクレイピングスクリプト（Python版）

PostgreSQL（Cloud SQL）上の companies テーブルの企業に対し、
1. URL未保有時: DuckDuckGo検索で公式サイトURLを特定
2. URL保有時: HPから「会社概要」「お問い合わせ」ページを自動判別し遷移・解析
3. 代表者名・電話番号・問い合わせフォームURLを抽出し、DBを更新

依存: pip install psycopg2-binary requests beautifulsoup4 duckduckgo-search

使い方:
  python scripts/scrape_company_homepage.py [--limit N] [--offset N] [--workers W] [--dry-run]
"""
from __future__ import annotations

import argparse
import os
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Optional
from urllib.parse import urljoin, urlparse

import psycopg2
from psycopg2.extras import RealDictCursor
import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# バリデーション・クリーニング（scrape_extended_fields.ts から移植）
# ---------------------------------------------------------------------------

HTTP_URL_EXTRACT_REGEX = re.compile(
    r"https?://[A-Za-z0-9\-._~:/?#\[\]@!$&'()*+,;=%]+"
)


def is_valid_url(url: Optional[str]) -> bool:
    if not url or not isinstance(url, str):
        return False
    s = url.strip()
    if not s:
        return False
    try:
        from urllib.parse import urlparse
        parsed = urlparse(s)
        return parsed.scheme in ("http", "https")
    except Exception:
        return False


def extract_first_http_url(input_str: str) -> Optional[str]:
    m = HTTP_URL_EXTRACT_REGEX.search(input_str)
    return m.group(0) if m else None


def clean_url_before_save(url: Optional[str]) -> Optional[str]:
    if not url or not isinstance(url, str):
        return None
    s = url.strip()
    if not s:
        return None
    extracted = extract_first_http_url(s)
    return extracted or s


def clean_address_before_save(address: Optional[str]) -> Optional[str]:
    """住所末尾の「/地図」「Googleマップで表示」等を除去"""
    if not address or not isinstance(address, str):
        return None
    s = re.sub(r"\s+", " ", address).strip()
    if not s:
        return None

    patterns = [
        r"/地図.*$",
        r"Google\s*マップで表示.*$",
        r"Google\s*マップ.*$",
        r"Google\s*Maps?.*$",
        r"地図を表示.*$",
        r"地図を見る.*$",
    ]
    for p in patterns:
        s = re.sub(p, "", s, flags=re.IGNORECASE)

    s = (
        s.replace("Googleマップで表示", "")
        .replace("Googleマップ", "")
        .replace("/地図", "")
    )
    s = re.sub(r"\s+", " ", s).strip()
    return s if s else None


def is_valid_phone_number(phone: Optional[str]) -> bool:
    if not phone or not isinstance(phone, str):
        return False
    s = phone.strip()
    if not s:
        return False
    if not re.match(r"^[0-9\-()]+$", s):
        return False
    digits = re.sub(r"\D", "", s)
    if len(digits) < 10 or len(digits) > 15:
        return False
    for bad in ("function", "script", "eval", "window", "document"):
        if bad in s.lower():
            return False
    return True


def is_valid_executive_name(name: Optional[str]) -> bool:
    """役員名として不自然な文字列を除外"""
    if not name or not isinstance(name, str):
        return False

    s = re.sub(r"^(代表者|代表取締役|社長|取締役|監査役|執行役員|役員)[：:\s]*", "", name, flags=re.I).strip()
    if not s or len(s) < 2 or len(s) > 50:
        return False

    invalid_patterns = [
        r"^(代表者|代表取締役|社長|取締役|監査役|執行役員|役員)",
        r"(http|https|www\.|\.co\.jp|\.com|\.jp)",
        r"(jQuery|function|var|document|window|offset|top|height)",
        r"(登録企業数|社以上|で解決|お困り対応|サポート)",
        r"(詳しく見る|最新ニュース|年末年始|休業|お知らせ)",
        r"(HOME|とは|事業内容|会社案内|採用情報|お問合せ|資料請求|PAGE TOP)",
        r"(Copyright|All Rights Reserved)",
        r"[0-9]",
        r"[a-zA-Z]{3,}",
    ]
    for p in invalid_patterns:
        if re.search(p, s, re.I):
            return False

    return bool(re.match(r"^[ぁ-んァ-ヶ一-龠々ー]+$", s))


def sanitize_scraped_data_for_save(data: dict[str, Any]) -> dict[str, Any]:
    """保存直前にURL/住所をクリーニング"""
    out = dict(data)

    for key in ("company_url", "contact_form_url"):
        if key in out and out[key]:
            cleaned = clean_url_before_save(out[key])
            if cleaned:
                out[key] = cleaned

    for key in ("address", "headquarters_address", "representative_home_address"):
        if key in out and out[key]:
            cleaned = clean_address_before_save(out[key])
            if cleaned:
                out[key] = cleaned

    return out


# ---------------------------------------------------------------------------
# User-Agent・リクエスト設定
# ---------------------------------------------------------------------------

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

REQUEST_TIMEOUT = 15
SLEEP_MIN = 1.5
SLEEP_MAX = 4.0
REQUEST_HEADERS = {"Accept-Language": "ja,en;q=0.9", "Accept": "text/html,application/xhtml+xml"}


def _headers() -> dict[str, str]:
    h = dict(REQUEST_HEADERS)
    h["User-Agent"] = random.choice(USER_AGENTS)
    return h


# ---------------------------------------------------------------------------
# DuckDuckGo 検索
# ---------------------------------------------------------------------------

def search_company_url(company_name: str, address: str) -> Optional[str]:
    """DuckDuckGoで社名＋住所から公式サイトURLを検索"""
    try:
        from duckduckgo_search import DDGS
    except ImportError:
        print("  [WARN] duckduckgo-search が未インストール: pip install duckduckgo-search", file=sys.stderr)
        return None

    query = f"{company_name} 公式サイト"
    if address and address.strip():
        # 都道府県程度を付与（長い住所はノイズになりやすい）
        prefecture = (address[:20] if len(address) > 20 else address).strip()
        query = f"{company_name} {prefecture} 公式"

    time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

    try:
        with DDGS() as ddgs:
            results = list(ddgs.text(query, region="jp-jp", max_results=5))
        for r in results:
            href = r.get("href")
            if href and is_valid_url(href):
                # SNS・ニュース・検索結果ページを除外
                skip_domains = ("facebook.com", "twitter.com", "x.com", "instagram.com", "linkedin.com",
                               "news.google", "yahoo.co.jp/news", "atpress.ne.jp", "prtimes.jp")
                if any(d in href.lower() for d in skip_domains):
                    continue
                return href
    except Exception as e:
        print(f"  [WARN] DuckDuckGo検索エラー: {e}", file=sys.stderr)

    return None


# ---------------------------------------------------------------------------
# HPスクレイピング
# ---------------------------------------------------------------------------

# 「会社概要」「お問い合わせ」ページのリンク候補
ABOUT_CONTACT_KEYWORDS = [
    "about", "company", "会社概要", "会社案内", "企業情報", "概要",
    "contact", "inquiry", "問い合わせ", "お問い合わせ", "お問合せ", "contact-us",
]


def _find_internal_links(soup: BeautifulSoup, base_url: str) -> list[tuple[str, str]]:
    """会社概要・お問い合わせ系の内部リンクを収集 (url, label)"""
    found: list[tuple[str, str]] = []
    seen: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        text = (a.get_text() or "").strip().lower()

        if not href or href.startswith(("#", "javascript:", "mailto:")):
            continue

        full_url = urljoin(base_url, href)
        if not is_valid_url(full_url):
            continue

        parsed = urlparse(base_url)
        target = urlparse(full_url)
        if parsed.netloc != target.netloc:
            continue

        if full_url in seen:
            continue
        seen.add(full_url)

        href_lower = href.lower()
        text_lower = text
        for kw in ABOUT_CONTACT_KEYWORDS:
            if kw in href_lower or kw in text_lower:
                found.append((full_url, kw))
                break

    return found


def _scrape_page(url: str, null_fields: set[str]) -> dict[str, Any]:
    """1ページから代表者名・電話番号・問い合わせURLを抽出"""
    data: dict[str, Any] = {}

    try:
        resp = requests.get(url, headers=_headers(), timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding or "utf-8"
    except Exception as e:
        print(f"    [WARN] 取得失敗 {url}: {e}", file=sys.stderr)
        return data

    soup = BeautifulSoup(resp.text, "html.parser")
    text = re.sub(r"\s+", " ", (soup.get_text() or "")).strip()

    # 電話番号
    if "phone_number" in null_fields or "representative_name" in null_fields or "contact_form_url" in null_fields:
        phone_m = re.search(r"(?:電話|TEL|Tel|電話番号)[：:\s]*([0-9\-()]{10,15})", text, re.I)
        if phone_m:
            raw = re.sub(r"[^\d\-]", "", phone_m.group(1))
            if is_valid_phone_number(raw):
                data["phone_number"] = raw

    # 代表者名
    if "representative_name" in null_fields:
        rep_m = re.search(r"(?:代表者|代表取締役|社長)[：:\s]*([^\n\r]{1,50})", text)
        if rep_m:
            name = rep_m.group(1).strip()
            if is_valid_executive_name(name):
                data["representative_name"] = re.sub(
                    r"^(代表者|代表取締役|社長|取締役|監査役|執行役員|役員)[：:\s]*",
                    "",
                    name,
                    flags=re.I,
                ).strip()

    # 問い合わせフォームURL
    if "contact_form_url" in null_fields:
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            for kw in ("contact", "inquiry", "問い合わせ", "お問い合わせ"):
                if kw in href.lower() or kw in (a.get_text() or "").lower():
                    full = urljoin(url, href)
                    if is_valid_url(full):
                        data["contact_form_url"] = full
                        break
            if "contact_form_url" in data:
                break

    return data


def scrape_from_homepage(
    homepage_url: str,
    company_name: str,
    null_fields: set[str],
) -> dict[str, Any]:
    """
    トップページ＋会社概要・お問い合わせページをクロールして情報を収集
    """
    data: dict[str, Any] = {}

    # 1. トップページ
    page_data = _scrape_page(homepage_url, null_fields)
    for k, v in page_data.items():
        if v and k not in data:
            data[k] = v

    if not null_fields:
        return data

    # 2. 会社概要・お問い合わせリンクを辿る（最大3ページ）
    try:
        resp = requests.get(homepage_url, headers=_headers(), timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding or "utf-8"
    except Exception:
        return data

    soup = BeautifulSoup(resp.text, "html.parser")
    links = _find_internal_links(soup, homepage_url)
    visited: set[str] = {homepage_url}
    follow_count = 0

    for link_url, _ in links:
        if follow_count >= 3:
            break
        if link_url in visited:
            continue
        visited.add(link_url)
        time.sleep(random.uniform(0.5, 1.5))

        page_data = _scrape_page(link_url, null_fields)
        for k, v in page_data.items():
            if v and k not in data:
                data[k] = v
        follow_count += 1

    # 3. company_url を設定（ルートURL）
    if homepage_url and not data.get("company_url"):
        try:
            p = urlparse(homepage_url)
            root = f"{p.scheme}://{p.netloc}"
            if is_valid_url(root):
                data["company_url"] = root
        except Exception:
            pass

    return data


# ---------------------------------------------------------------------------
# DB処理
# ---------------------------------------------------------------------------

def get_db_config() -> dict[str, Any]:
    return {
        "host": os.environ.get("POSTGRES_HOST", "127.0.0.1"),
        "port": os.environ.get("POSTGRES_PORT", "5432"),
        "database": os.environ.get("POSTGRES_DB", "postgres"),
        "user": os.environ.get("POSTGRES_USER", "postgres"),
        "password": os.environ.get("POSTGRES_PASSWORD", ""),
    }


def fetch_companies(
    conn,
    limit: int,
    offset: int,
) -> list[dict[str, Any]]:
    """company_url が空、または特定項目が欠損しているレコードを取得"""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, name, company_url, contact_form_url, phone_number,
                   representative_name, address, headquarters_address, prefecture
            FROM companies
            WHERE name IS NOT NULL AND TRIM(name) != ''
              AND (
                company_url IS NULL OR TRIM(company_url) = '' OR
                contact_form_url IS NULL OR TRIM(contact_form_url) = '' OR
                phone_number IS NULL OR TRIM(phone_number) = '' OR
                representative_name IS NULL OR TRIM(representative_name) = ''
              )
            ORDER BY id
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        return cur.fetchall()


def update_company(conn, company_id: str, data: dict[str, Any], dry_run: bool = False) -> list[str]:
    """
    COALESCE で既存データを保護しつつ UPDATE。
    有効な値があるフィールドのみ更新する。
    """
    allowed = {
        "company_url", "contact_form_url", "phone_number", "contact_phone_number",
        "representative_name", "address", "headquarters_address",
    }
    updates: dict[str, Any] = {}

    if data.get("company_url") and is_valid_url(data["company_url"]):
        updates["company_url"] = clean_url_before_save(data["company_url"])
    if data.get("contact_form_url") and is_valid_url(data["contact_form_url"]):
        updates["contact_form_url"] = clean_url_before_save(data["contact_form_url"])
    if data.get("phone_number") and is_valid_phone_number(data["phone_number"]):
        updates["phone_number"] = data["phone_number"]
        updates["contact_phone_number"] = data["phone_number"]
    if data.get("representative_name") and is_valid_executive_name(data["representative_name"]):
        updates["representative_name"] = data["representative_name"]
    if data.get("address"):
        cleaned = clean_address_before_save(data["address"])
        if cleaned:
            updates["address"] = cleaned
    if data.get("headquarters_address"):
        cleaned = clean_address_before_save(data["headquarters_address"])
        if cleaned:
            updates["headquarters_address"] = cleaned

    if not updates:
        return []

    set_parts: list[str] = []
    params: list[Any] = []
    idx = 1

    for col, val in updates.items():
        set_parts.append(f"{col} = COALESCE(NULLIF(%s, ''), {col})")
        params.append(val if val else "")
        idx += 1

    set_parts.append("updated_at = NOW()")
    params.append(company_id)

    if dry_run:
        return list(updates.keys())

    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE companies SET {', '.join(set_parts)} WHERE id = %s",
            params,
        )
    conn.commit()
    return list(updates.keys())


# ---------------------------------------------------------------------------
# 1社処理
# ---------------------------------------------------------------------------

@dataclass
class ProcessResult:
    company_id: str
    company_name: str
    updated_fields: list[str] = field(default_factory=list)
    status: str = "skipped"
    error: Optional[str] = None


def process_one_company(
    row: dict[str, Any],
    dry_run: bool,
    db_config: dict[str, Any],
) -> ProcessResult:
    """1社分の処理（スレッドから呼ばれる）"""
    company_id = str(row["id"])
    company_name = (row.get("name") or "").strip()
    existing_url = (row.get("company_url") or "").strip()
    address = (row.get("address") or row.get("headquarters_address") or row.get("prefecture") or "").strip()

    null_fields: set[str] = set()
    if not existing_url:
        null_fields.add("company_url")
    if not (row.get("contact_form_url") or "").strip():
        null_fields.add("contact_form_url")
    if not (row.get("phone_number") or "").strip():
        null_fields.add("phone_number")
    if not (row.get("representative_name") or "").strip():
        null_fields.add("representative_name")

    result = ProcessResult(company_id=company_id, company_name=company_name)

    try:
        url_to_scrape = existing_url
        if not url_to_scrape:
            url_to_scrape = search_company_url(company_name, address)
            if not url_to_scrape:
                result.status = "no_url"
                return result

        data = scrape_from_homepage(url_to_scrape, company_name, null_fields)
        data = sanitize_scraped_data_for_save(data)

        if not data:
            result.status = "no_data"
            return result

        conn = psycopg2.connect(**db_config)
        try:
            updated = update_company(conn, company_id, data, dry_run=dry_run)
            result.updated_fields = updated
            result.status = "success" if updated else "no_valid"
        finally:
            conn.close()

    except Exception as e:
        result.status = "error"
        result.error = str(e)

    return result


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="企業HPスクレイピング＆DB更新")
    parser.add_argument("--limit", type=int, default=100, help="取得件数")
    parser.add_argument("--offset", type=int, default=0, help="オフセット")
    parser.add_argument("--workers", type=int, default=3, help="並列ワーカー数")
    parser.add_argument("--dry-run", action="store_true", help="DB更新しない")
    args = parser.parse_args()

    db_config = get_db_config()
    if not db_config.get("password"):
        print("環境変数 POSTGRES_PASSWORD を設定してください。", file=sys.stderr)
        sys.exit(1)

    conn = psycopg2.connect(**db_config)
    try:
        rows = fetch_companies(conn, args.limit, args.offset)
    finally:
        conn.close()

    if not rows:
        print("対象企業がありません。")
        return

    print(f"対象: {len(rows)} 件（offset={args.offset}, limit={args.limit}）")
    if args.dry_run:
        print("[DRY-RUN] DBは更新しません")

    ok = 0
    err = 0

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {
            ex.submit(process_one_company, row, args.dry_run, db_config): row
            for row in rows
        }
        for fut in as_completed(futures):
            row = futures[fut]
            try:
                res = fut.result()
                if res.status == "success":
                    ok += 1
                    print(f"  [OK] {res.company_id} {res.company_name[:30]} → {res.updated_fields}")
                elif res.status == "error":
                    err += 1
                    print(f"  [ERR] {res.company_id} {res.company_name[:30]} {res.error}")
            except Exception as e:
                err += 1
                print(f"  [ERR] {row.get('id')} {e}")

    print(f"\n完了: 成功={ok}, エラー={err}, 対象={len(rows)}")


if __name__ == "__main__":
    main()
