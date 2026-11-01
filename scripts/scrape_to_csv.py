#!/usr/bin/env python3
"""
企業HPスクレイピング → DBスキーマ準拠CSV出力スクリプト
====================================================

処理フロー:
  1. input_urls.csv を読み込み
  2. 各URLの「会社概要」ページをスクレイピング
  3. ルールエンジンで正規化 (金額/人数 → BIGINT, 設立年 → INTEGER 等)
  4. データ検証 (必須項目チェック・型チェック)
  5. output_companies_for_db.csv を出力

出力カラム（companies テーブル物理カラム名）:
  corporate_number, name, url, headquarters_address, representative_name,
  founded_year, capital_stock, employee_count, revenue, listed, industry_large

使い方:
  python scripts/scrape_to_csv.py [--input input_urls.csv] [--output output_companies_for_db.csv]
                                  [--sleep 2.0] [--timeout 15] [--max-pages 3]

入力CSVフォーマット（最低限 url カラムが必要）:
  url
  https://example.co.jp
  https://another.co.jp
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import random
import re
import sys
import time
from dataclasses import dataclass, field, asdict
from typing import Any, Optional
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# ログ設定
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# WAF回避: UA ＋ ブラウザ指紋一致ヘッダープロファイル
# Sec-CH-UA / Sec-Fetch-* は送信する UA と一致させることが重要
# ---------------------------------------------------------------------------

# (UA文字列, {追加ヘッダー辞書}) のリスト
# Chrome / Edge / Firefox を混在させてフィンガープリントを分散
_UA_PROFILES: list[tuple[str, dict[str, str]]] = [
    # --- Chrome 126 / Windows ---
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        {
            "Sec-CH-UA":          '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
            "Sec-CH-UA-Mobile":   "?0",
            "Sec-CH-UA-Platform": '"Windows"',
        },
    ),
    # --- Chrome 126 / macOS ---
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        {
            "Sec-CH-UA":          '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
            "Sec-CH-UA-Mobile":   "?0",
            "Sec-CH-UA-Platform": '"macOS"',
        },
    ),
    # --- Edge 126 / Windows ---
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
        {
            "Sec-CH-UA":          '"Not/A)Brand";v="8", "Chromium";v="126", "Microsoft Edge";v="126"',
            "Sec-CH-UA-Mobile":   "?0",
            "Sec-CH-UA-Platform": '"Windows"',
        },
    ),
    # --- Firefox 127 / Windows (Sec-CH-UA なし) ---
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
        {},  # Firefox は Sec-CH-UA を送らない
    ),
]

# すべてのプロファイルに共通する基本ヘッダー
_COMMON_HEADERS: dict[str, str] = {
    "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language":           "ja,en-US;q=0.9,en;q=0.7",
    "Accept-Encoding":           "gzip, deflate, br",
    "Upgrade-Insecure-Requests": "1",
    "Connection":                "keep-alive",
    "Cache-Control":             "max-age=0",
}

# トップページ取得時 (Sec-Fetch-Site: none)
_FETCH_HEADERS_TOP: dict[str, str] = {
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}

# 同一サイト内リンク遷移時 (Sec-Fetch-Site: same-origin)
_FETCH_HEADERS_SUBPAGE: dict[str, str] = {
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
}

# 「会社概要」系ページを示すキーワード (URL・リンクテキスト)
ABOUT_PAGE_KEYWORDS = [
    "company", "about", "profile",
    "会社概要", "会社案内", "企業情報", "企業概要", "概要", "会社情報",
    "corporate", "overview",
]

# 各フィールドのTHラベル候補（汎用マッチング用）
FIELD_LABEL_MAP: dict[str, list[str]] = {
    "corporate_number":    ["法人番号"],
    "name":                ["会社名", "社名", "商号", "法人名", "企業名", "名称"],
    "representative_name": ["代表者", "代表取締役", "社長", "代表", "取締役社長", "代表者名"],
    "headquarters_address":["本社所在地", "所在地", "住所", "本店所在地", "本社住所", "本社", "オフィス"],
    "founded_year":        ["設立", "設立年月日", "設立年", "創業", "創業年", "創立"],
    "capital_stock":       ["資本金", "資本"],
    "employee_count":      ["従業員数", "社員数", "従業員", "スタッフ数", "人員"],
    "revenue":             ["売上高", "年商", "売上", "売上金額"],
    "listed":              ["上場", "株式上場", "上場区分", "市場"],
    "industry_large":      ["事業内容", "業種", "主な事業", "業態"],
    "url":                 ["ホームページ", "URL", "公式サイト", "ウェブサイト"],
}

# 出力カラム順 (companies テーブル物理カラム名)
OUTPUT_COLUMNS = [
    "corporate_number",
    "name",
    "url",
    "headquarters_address",
    "representative_name",
    "founded_year",
    "capital_stock",
    "employee_count",
    "revenue",
    "listed",
    "industry_large",
]

# 必須カラム
REQUIRED_COLUMNS = ["corporate_number", "name"]


# ---------------------------------------------------------------------------
# ルールエンジン: テキスト → 数値 変換
# ---------------------------------------------------------------------------

# 漢数字単位の乗数
_KANJI_UNITS: dict[str, int] = {
    "兆": 10**12,
    "千億": 10**11,
    "百億": 10**10,
    "十億": 10**9,
    "億": 10**8,
    "千万": 10**7,
    "百万": 10**6,
    "十万": 10**5,
    "万": 10**4,
    "千": 10**3,
    "百": 10**2,
}


def _remove_noise(text: str) -> str:
    """全角数字・スペースなどを正規化"""
    text = text.strip()
    # 全角数字 → 半角
    text = text.translate(str.maketrans("０１２３４５６７８９", "0123456789"))
    # 全角カンマ・ピリオド → 半角
    text = text.replace("，", ",").replace("．", ".")
    # ゼロ幅文字等を除去
    text = re.sub(r"[\u200b-\u200f\ufeff]", "", text)
    return text


def parse_money_to_bigint(text: str) -> Optional[int]:
    """
    金額テキストを円単位の BIGINT に変換。

    例:
      "3,000万円"     → 30000000
      "30億円"        → 3000000000
      "1億2,000万円"  → 120000000
      "3,000,000,000" → 3000000000
      "50百万円"      → 50000000
    """
    if not text:
        return None

    s = _remove_noise(text)

    # コンマ区切り純数字（例: "3,000,000,000"）
    pure_num = re.sub(r"[,\s円]", "", s)
    if re.fullmatch(r"\d+", pure_num):
        val = int(pure_num)
        # 桁数が少なすぎる場合は単位不明とみなし None
        return val if val > 0 else None

    # 漢数字単位混在パターン: "1億2,000万" など
    total = 0
    remaining = s
    matched_any = False

    for unit, multiplier in _KANJI_UNITS.items():
        pattern = rf"([\d,]+(?:\.\d+)?)\s*{re.escape(unit)}"
        m = re.search(pattern, remaining)
        if m:
            num_str = m.group(1).replace(",", "")
            try:
                total += float(num_str) * multiplier
                remaining = remaining[: m.start()] + remaining[m.end() :]
                matched_any = True
            except ValueError:
                pass

    if matched_any and total > 0:
        return int(total)

    # 数字のみ抽出して判断
    nums = re.findall(r"[\d,]+", s)
    if nums:
        raw = nums[0].replace(",", "")
        try:
            return int(raw) if int(raw) > 0 else None
        except ValueError:
            pass

    return None


def parse_count_to_bigint(text: str) -> Optional[int]:
    """
    人数テキストを BIGINT に変換。

    例:
      "約120名"      → 120
      "120人（パート含む）" → 120
      "1,200人"      → 1200
      "約1,200名以上" → 1200
    """
    if not text:
        return None

    s = _remove_noise(text)
    # 先頭の「約」「約 」「≒」を除去
    s = re.sub(r"^[約≒]\s*", "", s)
    # 最初の数字（コンマ区切り含む）を抽出
    m = re.search(r"([\d,]+)", s)
    if m:
        num_str = m.group(1).replace(",", "")
        try:
            val = int(num_str)
            return val if val > 0 else None
        except ValueError:
            pass

    return None


def parse_founded_year(text: str) -> Optional[int]:
    """
    設立年テキストを西暦 4 桁 INTEGER に変換。

    例:
      "1985年4月"       → 1985
      "昭和60年"        → 1985
      "平成5年"         → 1993
      "令和3年"         → 2021
      "2021/04/01"      → 2021
    """
    if not text:
        return None

    s = _remove_noise(text)

    # 西暦 4 桁を優先
    m = re.search(r"((?:19|20)\d{2})", s)
    if m:
        return int(m.group(1))

    # 元号 → 西暦変換
    era_map = {
        "令和": 2018,
        "平成": 1988,
        "昭和": 1925,
        "大正": 1911,
        "明治": 1867,
    }
    for era, base in era_map.items():
        m = re.search(rf"{era}(\d{{1,2}})年", s)
        if m:
            nen = int(m.group(1))
            return base + nen

    # 2 桁年のみ（"85年" → 1985, "05年" → 2005）
    m = re.search(r"(\d{2})年", s)
    if m:
        y2 = int(m.group(1))
        return (1900 + y2) if y2 >= 50 else (2000 + y2)

    return None


def parse_listed_status(text: str) -> Optional[str]:
    """上場区分の正規化"""
    if not text:
        return None
    s = text.strip()

    listed_keywords = ["東証", "名証", "札証", "福証", "プライム", "スタンダード", "グロース",
                       "上場", "東京証券取引所", "大阪証券取引所"]
    unlisted_keywords = ["非上場", "未上場"]

    if any(k in s for k in unlisted_keywords):
        return "非上場"
    if any(k in s for k in listed_keywords):
        # 市場区分を抽出
        for market in ["プライム", "スタンダード", "グロース", "JASDAQ", "マザーズ"]:
            if market in s:
                return f"上場（{market}）"
        return "上場"

    return s[:50]  # 不明な場合もそのまま返す（最大50文字）


def parse_corporate_number(text: str) -> Optional[str]:
    """13桁法人番号を文字列として正規化（先頭0落ち防止）"""
    if not text:
        return None
    s = re.sub(r"[\s\-ー－]", "", _remove_noise(text))
    # 13桁のみ有効
    m = re.search(r"(\d{13})", s)
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# スクレイピング: HTML テーブル → Key-Value ペア抽出
# ---------------------------------------------------------------------------

def extract_kv_from_table(soup: BeautifulSoup) -> dict[str, str]:
    """
    「会社概要」によくある <th>ラベル</th><td>値</td> 形式のテーブルから
    Key-Value ペアを汎用的に抽出する。

    対応形式:
      (A) <tr><th>資本金</th><td>3,000万円</td></tr>
      (B) <tr><td class="label">資本金</td><td>3,000万円</td></tr>
      (C) <dl><dt>資本金</dt><dd>3,000万円</dd></dl>
    """
    kv: dict[str, str] = {}

    # --- (A) / (B) テーブル形式 ---
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            ths = tr.find_all("th")
            tds = tr.find_all("td")

            if ths and tds:
                # <th>key</th><td>value</td>
                key_text = ths[0].get_text(separator=" ", strip=True)
                val_text = tds[0].get_text(separator=" ", strip=True)
                if key_text and val_text:
                    kv[key_text] = val_text
                    # 2列目 <th><td> のペアも拾う
                    if len(ths) > 1 and len(tds) > 1:
                        key2 = ths[1].get_text(separator=" ", strip=True)
                        val2 = tds[1].get_text(separator=" ", strip=True)
                        if key2 and val2:
                            kv[key2] = val2

            elif len(tds) >= 2:
                # <td class="label/th/key/..."> による疑似ヘッダ判定
                first_td = tds[0]
                td_class = " ".join(first_td.get("class") or []).lower()
                if any(c in td_class for c in ("label", "head", "key", "title", "th", "name")):
                    key_text = first_td.get_text(separator=" ", strip=True)
                    val_text = tds[1].get_text(separator=" ", strip=True)
                    if key_text and val_text:
                        kv[key_text] = val_text

    # --- (C) dl/dt/dd 形式 ---
    for dl in soup.find_all("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        for dt, dd in zip(dts, dds):
            key_text = dt.get_text(separator=" ", strip=True)
            val_text = dd.get_text(separator=" ", strip=True)
            if key_text and val_text:
                kv[key_text] = val_text

    # --- (D) section/div ラベル＋値ペア（よくある独自レイアウト）---
    label_selectors = [
        ("div", ["label", "item-label", "company-label", "profile-label", "col-label"]),
        ("span", ["label", "key", "th"]),
        ("p", ["label", "th"]),
    ]
    for tag, classes in label_selectors:
        for label_el in soup.find_all(tag, class_=lambda c: c and any(cls in " ".join(c) for cls in classes)):
            val_el = label_el.find_next_sibling()
            if val_el:
                key_text = label_el.get_text(separator=" ", strip=True)
                val_text = val_el.get_text(separator=" ", strip=True)
                if key_text and val_text and key_text not in kv:
                    kv[key_text] = val_text

    return kv


def match_kv_to_fields(kv: dict[str, str]) -> dict[str, str]:
    """
    抽出した KV ペアを FIELD_LABEL_MAP に基づいてフィールド名にマッピング。
    先にマッチしたフィールドが優先（ロングマッチ優先）。
    """
    result: dict[str, str] = {}

    for field_name, labels in FIELD_LABEL_MAP.items():
        if field_name in result:
            continue
        for kv_key, kv_val in kv.items():
            kv_key_clean = re.sub(r"[\s　]", "", kv_key)
            for label in labels:
                if label in kv_key_clean:
                    result[field_name] = kv_val
                    break
            if field_name in result:
                break

    return result


# ---------------------------------------------------------------------------
# スクレイピング: ページ取得・会社概要ページ探索
# ---------------------------------------------------------------------------

def _make_headers(
    is_subpage: bool = False,
    referer: Optional[str] = None,
) -> dict[str, str]:
    """
    WAF を回避するためのブラウザ指紋一致ヘッダーセットを返す。

    - UA と Sec-CH-UA を同じプロファイルから選択して整合性を保つ
    - トップページ / サブページで Sec-Fetch-Site を切り替え
    - サブページ取得時は Referer を付与
    """
    ua, ua_extra = random.choice(_UA_PROFILES)

    h: dict[str, str] = {}
    h.update(_COMMON_HEADERS)
    h.update(ua_extra)
    h["User-Agent"] = ua
    h.update(_FETCH_HEADERS_SUBPAGE if is_subpage else _FETCH_HEADERS_TOP)

    if referer:
        h["Referer"] = referer

    return h


def _fetch_html(
    url: str,
    timeout: int,
    session: Optional[requests.Session] = None,
    is_subpage: bool = False,
    referer: Optional[str] = None,
) -> Optional[BeautifulSoup]:
    """
    URL を取得して BeautifulSoup を返す。失敗時は None。

    - session を渡すとセッション（Cookie 保持）で取得（WAF 回避に有効）
    - is_subpage=True のとき Sec-Fetch-Site: same-origin を付与
    - referer を渡すと Referer ヘッダーを付与
    """
    headers = _make_headers(is_subpage=is_subpage, referer=referer)
    client: Any = session if session is not None else requests
    try:
        resp = client.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        resp.raise_for_status()
        # エンコーディング自動検出（iso-8859-1 は誤検知が多い）
        if resp.encoding and resp.encoding.lower() in ("iso-8859-1", "ascii"):
            resp.encoding = resp.apparent_encoding or "utf-8"
        return BeautifulSoup(resp.text, "html.parser")
    except requests.exceptions.HTTPError as e:
        logger.warning("HTTP エラー %s: %s", url, e)
        return None
    except requests.exceptions.RequestException as e:
        logger.warning("取得失敗 %s: %s", url, e)
        return None


def _find_about_page_url(soup: BeautifulSoup, base_url: str) -> Optional[str]:
    """トップページのリンクから「会社概要」ページのURLを探す"""
    base_netloc = urlparse(base_url).netloc
    candidates: list[tuple[int, str]] = []  # (優先度スコア, url)

    for a in soup.find_all("a", href=True):
        href = str(a.get("href", "")).strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue

        full_url = urljoin(base_url, href)
        try:
            parsed = urlparse(full_url)
        except Exception:
            continue

        # 同一ドメインのみ
        if parsed.netloc != base_netloc:
            continue

        href_lower = href.lower()
        link_text = (a.get_text() or "").strip()
        link_text_lower = link_text.lower()

        score = 0
        for kw in ABOUT_PAGE_KEYWORDS:
            if kw in href_lower:
                score += 2
            if kw in link_text_lower:
                score += 3  # テキストマッチを高優先

        if score > 0:
            candidates.append((score, full_url))

    if not candidates:
        return None

    # スコア最大のURLを返す
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def scrape_company_info(
    url: str,
    timeout: int = 15,
    sleep_sec: float = 2.0,
    max_pages: int = 3,
) -> dict[str, str]:
    """
    企業URLから会社概要情報をスクレイピングして生テキスト辞書を返す。

    1. requests.Session でトップページを取得（Cookie を保持し WAF を回避）
    2. 会社概要リンクを探して遷移（Referer ヘッダー付き / 最大 max_pages ページ）
    3. KV テーブル抽出 → フィールドマッピング
    """
    raw_fields: dict[str, str] = {}

    with requests.Session() as session:
        # --- トップページ ---
        time.sleep(sleep_sec * random.uniform(0.8, 1.2))
        top_soup = _fetch_html(url, timeout, session=session, is_subpage=False)
        if top_soup is None:
            return raw_fields

        # トップページでも KV 抽出を試みる
        top_kv = extract_kv_from_table(top_soup)
        top_fields = match_kv_to_fields(top_kv)
        raw_fields.update(top_fields)

        # URL 自体を記録
        if "url" not in raw_fields:
            raw_fields["url"] = url

        # --- 会社概要ページへ遷移 ---
        about_url = _find_about_page_url(top_soup, url)
        visited_urls: set[str] = {url}
        pages_visited = 0
        current_referer = url  # 直前のページURL を Referer に使用

        while about_url and about_url not in visited_urls and pages_visited < max_pages:
            visited_urls.add(about_url)
            pages_visited += 1

            logger.info("  → 会社概要ページ: %s", about_url)
            time.sleep(sleep_sec * random.uniform(0.8, 1.2))
            about_soup = _fetch_html(
                about_url, timeout,
                session=session,
                is_subpage=True,
                referer=current_referer,
            )
            if about_soup is None:
                break

            about_kv = extract_kv_from_table(about_soup)
            about_fields = match_kv_to_fields(about_kv)

            # 概要ページの値を優先（既存フィールドを上書きしない）
            for k, v in about_fields.items():
                if k not in raw_fields or not raw_fields[k]:
                    raw_fields[k] = v

            current_referer = about_url  # 次リクエストの Referer を更新

            # さらに深いリンクを探す（1段階まで）
            if pages_visited < max_pages:
                next_about = _find_about_page_url(about_soup, about_url)
                if next_about and next_about not in visited_urls:
                    about_url = next_about
                else:
                    break
            else:
                break

        # --- テキスト全体からの補完抽出（テーブル不使用ページ対応）---
        _supplement_from_text(top_soup, raw_fields)
        if about_url and about_url in visited_urls and about_url != url:
            _supplement_from_text_by_url(about_url, raw_fields, timeout)

    return raw_fields


def _supplement_from_text(soup: BeautifulSoup, fields: dict[str, str]) -> None:
    """
    テーブル以外のテキストコンテンツから不足フィールドを補完する。
    （テーブルが使われていないページ向けのフォールバック）
    """
    text = re.sub(r"\s+", " ", soup.get_text(separator=" ") or "").strip()

    # 代表者名
    if "representative_name" not in fields or not fields["representative_name"]:
        m = re.search(
            r"(?:代表者|代表取締役(?:社長)?|社長)[：:\s　]*([^\s　\n\r,、。]{2,20})",
            text
        )
        if m:
            fields["representative_name"] = m.group(1).strip()

    # 所在地
    if "headquarters_address" not in fields or not fields["headquarters_address"]:
        m = re.search(
            r"(?:所在地|住所|本社)[：:\s　]*([^\n\r]{5,60}(?:丁目|番地|号|町|村|市|区)[^\n\r]{0,40})",
            text
        )
        if m:
            fields["headquarters_address"] = m.group(1).strip()

    # 資本金
    if "capital_stock" not in fields or not fields["capital_stock"]:
        m = re.search(r"資本金[：:\s　]*([\d,億万千百]+円?)", text)
        if m:
            fields["capital_stock"] = m.group(1).strip()

    # 従業員数
    if "employee_count" not in fields or not fields["employee_count"]:
        m = re.search(r"(?:従業員数?|社員数?)[：:\s　]*([約\d,]+[名人](?:[^（\n]{0,30})?)", text)
        if m:
            fields["employee_count"] = m.group(1).strip()

    # 設立
    if "founded_year" not in fields or not fields["founded_year"]:
        m = re.search(r"設立[：:\s　]*((?:\d{4}年|昭和|平成|令和|大正)[^\n\r,、]{0,20})", text)
        if m:
            fields["founded_year"] = m.group(1).strip()

    # 法人番号
    if "corporate_number" not in fields or not fields["corporate_number"]:
        m = re.search(r"法人番号[：:\s　]*(\d[\d\-]{12,14})", text)
        if m:
            fields["corporate_number"] = m.group(1).strip()


def _supplement_from_text_by_url(
    url: str, fields: dict[str, str], timeout: int
) -> None:
    """会社概要ページのテキストから補完（URL指定版）"""
    soup = _fetch_html(url, timeout)
    if soup:
        _supplement_from_text(soup, fields)


# ---------------------------------------------------------------------------
# 変換チェックログ (stdout へ出力 — stderr のシステムログとは分離)
# ---------------------------------------------------------------------------

def _log_conversion(field_label: str, original: str, parsed: Any) -> None:
    """
    パース処理の「元テキスト → 変換後の値」を標準出力に表示する。
    パース失敗（None）のときも明示的に表示する。
    """
    result_str = str(parsed) if parsed is not None else "None（パース失敗）"
    print(f"[変換チェック] {field_label}: {original!r} -> {result_str}", flush=True)


# ---------------------------------------------------------------------------
# ルールエンジン: 生テキスト辞書 → 型付きデータ変換
# ---------------------------------------------------------------------------

@dataclass
class CompanyRecord:
    """companies テーブル1行分のデータ"""
    corporate_number: Optional[str] = None   # text (13桁)
    name: Optional[str] = None               # text ※必須
    url: Optional[str] = None                # text
    headquarters_address: Optional[str] = None  # text
    representative_name: Optional[str] = None   # text
    founded_year: Optional[int] = None       # integer
    capital_stock: Optional[int] = None      # bigint
    employee_count: Optional[int] = None     # bigint
    revenue: Optional[int] = None            # bigint
    listed: Optional[str] = None             # text
    industry_large: Optional[str] = None     # text

    # 処理メタ情報（CSV には含めない）
    source_url: str = ""
    validation_errors: list[str] = field(default_factory=list)
    is_valid: bool = True


def normalize_record(raw: dict[str, str], source_url: str) -> CompanyRecord:
    """
    生テキスト辞書をルールエンジンで型変換・正規化して CompanyRecord を返す。
    変換の前後を [変換チェック] として標準出力に出力する。
    """
    rec = CompanyRecord(source_url=source_url)

    # --- テキストフィールド ---
    rec.corporate_number = parse_corporate_number(raw.get("corporate_number", ""))
    rec.name = _clean_text(raw.get("name", ""))
    rec.url = _clean_url(raw.get("url", source_url))
    rec.headquarters_address = _clean_address(raw.get("headquarters_address", ""))
    rec.representative_name = _clean_representative(raw.get("representative_name", ""))
    rec.listed = parse_listed_status(raw.get("listed", ""))
    rec.industry_large = _clean_text(raw.get("industry_large", ""))

    # --- 数値フィールド（変換チェックログ付き）---

    # 資本金
    raw_capital = raw.get("capital_stock", "")
    rec.capital_stock = parse_money_to_bigint(raw_capital)
    if raw_capital:
        _log_conversion("資本金", raw_capital, rec.capital_stock)

    # 従業員数
    raw_emp = raw.get("employee_count", "")
    rec.employee_count = parse_count_to_bigint(raw_emp)
    if raw_emp:
        _log_conversion("従業員数", raw_emp, rec.employee_count)

    # 売上高
    raw_rev = raw.get("revenue", "")
    rec.revenue = parse_money_to_bigint(raw_rev)
    if raw_rev:
        _log_conversion("売上高", raw_rev, rec.revenue)

    # 設立年
    raw_year = raw.get("founded_year", "")
    rec.founded_year = parse_founded_year(raw_year)
    if raw_year:
        _log_conversion("設立年", raw_year, rec.founded_year)

    # 法人番号（ゼロ落ち確認ログ）
    if rec.corporate_number:
        _log_conversion("法人番号", raw.get("corporate_number", ""), rec.corporate_number)

    return rec


def _clean_text(text: str) -> Optional[str]:
    if not text:
        return None
    s = re.sub(r"\s+", " ", text).strip()
    return s if s else None


def _clean_url(url: str) -> Optional[str]:
    if not url:
        return None
    s = url.strip()
    try:
        parsed = urlparse(s)
        if parsed.scheme in ("http", "https"):
            return s
    except Exception:
        pass
    return None


def _clean_address(text: str) -> Optional[str]:
    """住所末尾の余分な文字列を除去"""
    if not text:
        return None
    s = re.sub(r"\s+", " ", text).strip()
    # 「Googleマップ」「地図」以降を除去
    for pattern in [r"/地図.*$", r"Google\s*マップ.*$", r"地図を.*$", r"MAP.*$"]:
        s = re.sub(pattern, "", s, flags=re.IGNORECASE).strip()
    return s if s else None


def _clean_representative(text: str) -> Optional[str]:
    """代表者名の前置詞（役職名）を除去"""
    if not text:
        return None
    s = re.sub(
        r"^(代表者|代表取締役(?:社長)?|取締役社長|社長|取締役|代表)[：:\s　]*",
        "",
        text.strip(),
        flags=re.IGNORECASE,
    ).strip()
    # 長さチェック（名前として不自然な長さは除外）
    if len(s) < 2 or len(s) > 50:
        return None
    # 明らかに名前でない文字列を除外
    if re.search(r"(http|www\.|\.co|\.jp|function|Copyright)", s, re.IGNORECASE):
        return None
    return s if s else None


# ---------------------------------------------------------------------------
# データ検証
# ---------------------------------------------------------------------------

def validate_record(rec: CompanyRecord) -> CompanyRecord:
    """
    必須項目・型・値域チェックを行い、エラーがあれば is_valid=False にする。
    """
    errors: list[str] = []

    # 必須チェック
    for col in REQUIRED_COLUMNS:
        val = getattr(rec, col, None)
        if not val:
            errors.append(f"必須項目が空: {col}")

    # 法人番号 13 桁チェック
    if rec.corporate_number and not re.fullmatch(r"\d{13}", rec.corporate_number):
        errors.append(f"法人番号が13桁でない: {rec.corporate_number!r}")

    # 設立年 範囲チェック
    if rec.founded_year is not None:
        if rec.founded_year < 1800 or rec.founded_year > 2100:
            errors.append(f"設立年が範囲外: {rec.founded_year}")
            rec.founded_year = None

    # 資本金 範囲チェック (1円〜999兆円)
    if rec.capital_stock is not None:
        if rec.capital_stock < 1 or rec.capital_stock > 999 * 10**12:
            errors.append(f"資本金が異常値: {rec.capital_stock}")
            rec.capital_stock = None

    # 従業員数 範囲チェック (1〜100万人)
    if rec.employee_count is not None:
        if rec.employee_count < 1 or rec.employee_count > 1_000_000:
            errors.append(f"従業員数が異常値: {rec.employee_count}")
            rec.employee_count = None

    # 売上高 範囲チェック
    if rec.revenue is not None:
        if rec.revenue < 1 or rec.revenue > 999 * 10**12:
            errors.append(f"売上高が異常値: {rec.revenue}")
            rec.revenue = None

    rec.validation_errors = errors
    rec.is_valid = len([e for e in errors if "必須項目" in e]) == 0  # 必須エラーのみ is_valid=False
    return rec


# ---------------------------------------------------------------------------
# CSV 入出力
# ---------------------------------------------------------------------------

def load_input_urls(input_path: str) -> list[dict[str, str]]:
    """
    入力 CSV を読み込み、各行の情報を辞書リストとして返す。

    必須カラム: url
    任意カラム: corporate_number, name （事前にわかっていれば活用）
    """
    try:
        df = pd.read_csv(input_path, dtype=str)
    except FileNotFoundError:
        logger.error("入力ファイルが見つかりません: %s", input_path)
        sys.exit(1)
    except Exception as e:
        logger.error("入力CSV読み込みエラー: %s", e)
        sys.exit(1)

    # カラム名の空白を除去
    df.columns = df.columns.str.strip()

    if "url" not in df.columns:
        logger.error("入力CSVに 'url' カラムが必要です。カラム: %s", list(df.columns))
        sys.exit(1)

    # NaN → 空文字に統一
    df = df.fillna("")

    rows = df.to_dict(orient="records")
    logger.info("入力URL: %d 件", len(rows))
    return rows


def records_to_dataframe(records: list[CompanyRecord]) -> pd.DataFrame:
    """CompanyRecord リストを出力カラム順の DataFrame に変換"""
    rows = []
    for rec in records:
        row = {col: getattr(rec, col, None) for col in OUTPUT_COLUMNS}
        rows.append(row)

    df = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)

    # -----------------------------------------------------------------------
    # [1] corporate_number: 先頭0落ち防止
    #     - pandas が数値型と誤推論しないよう object (Python str) に固定
    #     - 取得できた値は必ず13桁の文字列として正規化（zfill で0埋め）
    #     - None / NaN は空文字列のままにして CSV にも "" として出力
    # -----------------------------------------------------------------------
    def _normalize_corp_num(val: Any) -> Any:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        s = str(val).strip()
        if not s:
            return None
        # 数字のみなら13桁ゼロ埋め（念のため）
        if re.fullmatch(r"\d+", s):
            return s.zfill(13)
        return s

    df["corporate_number"] = df["corporate_number"].apply(_normalize_corp_num).astype(object)

    # -----------------------------------------------------------------------
    # [2] 数値カラム: nullable integer (Int64) にキャスト
    #     NA は CSV では空文字になる（None/NaT とも混同しない）
    # -----------------------------------------------------------------------
    for col in ("founded_year", "capital_stock", "employee_count", "revenue"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    return df


def save_output_csv(df: pd.DataFrame, output_path: str) -> None:
    """
    DataFrame を CSV に保存（UTF-8 BOM 付き）。

    corporate_number の先頭0落ち防止のため、文字列カラムには
    csv.QUOTE_NONNUMERIC でダブルクォートを付与する。
    → "0123456789012" のようにクォートされるため Excel でも数値変換されない。

    数値カラム (Int64) はクォートなしで出力される。

    ⚠️  この CSV を pandas で再読み込みする際は必ず read_output_csv() を使うこと。
        pd.read_csv() のデフォルト型推論は leading-zero を持つ数字列を int に変換するため
        corporate_number のゼロ落ちが発生する。
    """
    df.to_csv(
        output_path,
        index=False,
        encoding="utf-8-sig",
        quoting=csv.QUOTE_NONNUMERIC,  # 文字列列を " で囲む → 先頭0保護
        na_rep="",                      # NA / NaN は空文字列として出力
    )
    logger.info("出力完了: %s (%d 件)", output_path, len(df))


def read_output_csv(csv_path: str) -> pd.DataFrame:
    """
    save_output_csv() が出力した CSV を安全に読み込む。

    corporate_number は必ず str として読み込み、先頭0を保持する。
    DB インポートスクリプトやユニットテストからこの関数を使うこと。

    使用例:
        df = read_output_csv("output_companies_for_db.csv")
        assert df["corporate_number"].iloc[0] == "0123456789012"  # ✓ ゼロ保持
    """
    return pd.read_csv(
        csv_path,
        dtype={"corporate_number": str},   # ← ここが核心: 先頭0落ち防止
        keep_default_na=False,             # 空文字列を NaN に変換しない
    )


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def process_url(
    row: dict[str, str],
    sleep_sec: float,
    timeout: int,
    max_pages: int,
) -> CompanyRecord:
    """1 URL 分を処理して CompanyRecord を返す"""
    url = (row.get("url") or "").strip()
    if not url:
        rec = CompanyRecord(source_url="(empty)")
        rec.validation_errors = ["URL が空です"]
        rec.is_valid = False
        return rec

    # URL スキームが省略されていれば補完
    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    logger.info("処理中: %s", url)

    # スクレイピング実行
    raw_fields = scrape_company_info(url, timeout=timeout, sleep_sec=sleep_sec, max_pages=max_pages)

    # 入力 CSV に事前情報があれば優先的に使用
    for predef_col in ("corporate_number", "name"):
        predef_val = (row.get(predef_col) or "").strip()
        if predef_val and (predef_col not in raw_fields or not raw_fields[predef_col]):
            raw_fields[predef_col] = predef_val

    # 正規化
    rec = normalize_record(raw_fields, source_url=url)

    # 検証
    rec = validate_record(rec)

    if rec.validation_errors:
        logger.warning(
            "  %s: 検証警告 %s",
            url[:60],
            "; ".join(rec.validation_errors),
        )

    return rec


def main() -> None:
    parser = argparse.ArgumentParser(
        description="企業HPスクレイピング → DBスキーマ準拠CSV出力",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--input", default="input_urls.csv",
        help="入力URLリストCSV (default: input_urls.csv)",
    )
    parser.add_argument(
        "--output", default="output_companies_for_db.csv",
        help="出力CSV (default: output_companies_for_db.csv)",
    )
    parser.add_argument(
        "--sleep", type=float, default=2.0,
        help="リクエスト間のスリープ秒数 (default: 2.0)",
    )
    parser.add_argument(
        "--timeout", type=int, default=15,
        help="HTTPリクエストタイムアウト秒数 (default: 15)",
    )
    parser.add_argument(
        "--max-pages", type=int, default=3,
        help="1企業あたりの最大クロールページ数 (default: 3)",
    )
    parser.add_argument(
        "--include-invalid", action="store_true",
        help="検証エラーのレコードも出力CSVに含める",
    )
    args = parser.parse_args()

    # 入力読み込み
    input_rows = load_input_urls(args.input)

    # 処理
    all_records: list[CompanyRecord] = []
    for i, row in enumerate(input_rows, 1):
        logger.info("[%d/%d] %s", i, len(input_rows), row.get("url", "(empty)"))
        rec = process_url(row, args.sleep, args.timeout, args.max_pages)
        all_records.append(rec)

    # 集計
    valid_count = sum(1 for r in all_records if r.is_valid)
    invalid_count = len(all_records) - valid_count
    logger.info("処理完了: 有効=%d件 / 無効=%d件 / 合計=%d件",
                valid_count, invalid_count, len(all_records))

    # 出力フィルタ
    output_records = all_records if args.include_invalid else [r for r in all_records if r.is_valid]
    if not output_records:
        logger.warning("出力対象レコードが0件です。--include-invalid オプションを確認してください。")

    # DataFrame 化 & CSV 出力
    df = records_to_dataframe(output_records)
    save_output_csv(df, args.output)

    # 検証エラーのサマリを表示
    if invalid_count > 0:
        logger.info("=== 検証エラーサマリ ===")
        for rec in all_records:
            if not rec.is_valid:
                logger.info("  [NG] %s → %s", rec.source_url[:60], "; ".join(rec.validation_errors))


if __name__ == "__main__":
    main()
