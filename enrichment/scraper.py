"""
scraper.py - HP本体からの情報抽出ロジック（Step3）

仕様書 7章:
1. 構造化データ優先: JSON-LD (schema.org/Organization)、OpenGraph、hCard
2. 正規表現 + ラベル辞書: 「代表者」「資本金」「設立」等の日本語ラベル辞書
3. HTML構造ヒューリスティック: <table> / <dl> のプロフィールテーブル解析
4. tel:/mailto: リンク抽出（電話番号・メール最優先）

探索ページ: トップ → 会社概要 → お問い合わせ → 採用ページ
"""
from __future__ import annotations

import json
import logging
import re
import time
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup, Tag

from .config import (
    HTTP_TIMEOUT, HTTP_MAX_RETRIES, HTTP_BACKOFF_BASE, HTTP_USER_AGENT,
    DOMAIN_MIN_INTERVAL,
    COMPANY_PAGE_PATHS, COMPANY_PAGE_KEYWORDS,
    CONTACT_PAGE_KEYWORDS, RECRUIT_PAGE_KEYWORDS,
)
from .url_validator import _rate_limit, _can_fetch

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# 日本語ラベル辞書（ラベル → フィールドキー）
# ─────────────────────────────────────────────
LABEL_MAP: Dict[str, str] = {
    # 社名
    "会社名": "company_name_scraped", "商号": "company_name_scraped",
    "法人名": "company_name_scraped",
    # 代表者
    "代表者": "representative_name", "代表取締役": "representative_name",
    "代表取締役社長": "representative_name", "社長": "representative_name",
    "代表者名": "representative_name", "代表者氏名": "representative_name",
    "代表": "representative_name",
    # 所在地
    "所在地": "headquarters_address", "本社所在地": "headquarters_address",
    "住所": "headquarters_address", "本社": "headquarters_address",
    "本社住所": "headquarters_address",
    # 郵便番号
    "郵便番号": "postal_code",
    # 電話番号
    "電話番号": "phone_number", "tel": "phone_number", "ＴＥＬ": "phone_number",
    "電話": "phone_number", "お電話": "phone_number",
    # FAX
    "fax": "fax", "ファックス": "fax", "ＦＡＸ": "fax", "ファクシミリ": "fax",
    # メール
    "メール": "email", "e-mail": "email", "email": "email",
    "メールアドレス": "email",
    # 設立
    "設立": "established_date", "創立": "established_date",
    "設立年月": "established_date", "設立年月日": "established_date",
    "創業": "established_date", "創業年": "established_date",
    # 資本金
    "資本金": "capital_stock",
    # 従業員数
    "従業員数": "employee_count", "従業員": "employee_count",
    "社員数": "employee_count", "スタッフ数": "employee_count",
    "人員": "employee_count",
    # 事業内容
    "事業内容": "business_items", "業種": "business_items",
    "主な事業": "business_items", "主要事業": "business_items",
    "事業": "business_items",
    # 概要
    "会社概要": "overview",
    # 役員
    "役員": "_executives", "役員一覧": "_executives",
    "取締役": "_executives",
    # 拠点数
    "事業所数": "office_count", "営業所数": "office_count",
    "店舗数": "store_count", "工場数": "factory_count",
    # SNS
    "facebook": "facebook", "フェイスブック": "facebook",
    "linkedin": "linkedin", " linkedin（リンクトイン）": "linkedin",
    "wantedly": "wantedly",
}

# ─────────────────────────────────────────────
# HTTP フェッチ
# ─────────────────────────────────────────────

def _fetch_html(url: str) -> Optional[str]:
    """robots.txt + レートリミット付きHTMLフェッチ"""
    if not _can_fetch(url):
        logger.info(f"[scraper] robots.txt blocked: {url}")
        return None
    domain = urlparse(url).netloc
    _rate_limit(domain)
    headers = {
        "User-Agent": HTTP_USER_AGENT,
        "Accept-Language": "ja,en;q=0.5",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    for attempt in range(HTTP_MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT, allow_redirects=True)
            if resp.status_code == 200:
                resp.encoding = resp.apparent_encoding or "utf-8"
                return resp.text
            if 400 <= resp.status_code < 500:
                return None
            wait = HTTP_BACKOFF_BASE ** attempt
            if attempt < HTTP_MAX_RETRIES:
                time.sleep(wait)
        except requests.exceptions.Timeout:
            if attempt < HTTP_MAX_RETRIES:
                time.sleep(HTTP_BACKOFF_BASE ** attempt)
        except requests.exceptions.RequestException:
            return None
    return None


# ─────────────────────────────────────────────
# ページ探索: 会社概要/お問い合わせ/採用ページの発見
# ─────────────────────────────────────────────

def _find_subpages(
    base_url: str, soup: BeautifulSoup
) -> Dict[str, Optional[str]]:
    """
    トップページから会社概要・お問い合わせ・採用ページのURLを発見。
    Returns: {'company': url, 'contact': url, 'recruit': url}
    """
    result: Dict[str, Optional[str]] = {
        "company": None, "contact": None, "recruit": None,
    }
    base_domain = urlparse(base_url).netloc

    # パスから先に試みる
    for path in COMPANY_PAGE_PATHS:
        candidate = urljoin(base_url, path)
        result["company"] = result["company"] or candidate
        break  # パスは最初の1つだけ候補として記録（後でHTTPチェック）

    # リンクテキストで判定
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        text = a.get_text(strip=True)
        if not href or href.startswith(("mailto:", "tel:", "#", "javascript:")):
            continue
        full_url = urljoin(base_url, href)
        if urlparse(full_url).netloc != base_domain:
            continue  # 外部リンクはスキップ

        for kw in COMPANY_PAGE_KEYWORDS:
            if kw.lower() in text.lower() or kw.lower() in href.lower():
                if not result["company"]:
                    result["company"] = full_url
                break
        for kw in CONTACT_PAGE_KEYWORDS:
            if kw.lower() in text.lower() or kw.lower() in href.lower():
                if not result["contact"]:
                    result["contact"] = full_url
                break
        for kw in RECRUIT_PAGE_KEYWORDS:
            if kw.lower() in text.lower() or kw.lower() in href.lower():
                if not result["recruit"]:
                    result["recruit"] = full_url
                break

    return result


# ─────────────────────────────────────────────
# 抽出手法1: JSON-LD (schema.org/Organization)
# ─────────────────────────────────────────────

def _extract_jsonld(soup: BeautifulSoup) -> Dict:
    """JSON-LD から Organization スキーマを抽出"""
    result: Dict = {}
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            # リストの場合も対応
            if isinstance(data, list):
                data = next(
                    (d for d in data if isinstance(d, dict)
                     and d.get("@type") in ("Organization", "LocalBusiness", "Corporation")),
                    None,
                )
            if not data or not isinstance(data, dict):
                continue
            t = data.get("@type", "")
            if t not in ("Organization", "LocalBusiness", "Corporation",
                         "HealthAndBeautyBusiness", "FoodEstablishment",
                         "MedicalOrganization"):
                continue
            # 電話番号
            if data.get("telephone"):
                result["phone_number"] = str(data["telephone"])
            # メール
            if data.get("email"):
                result["email"] = str(data["email"])
            # 住所
            addr = data.get("address", {})
            if isinstance(addr, dict):
                parts = [
                    addr.get("addressRegion", ""),
                    addr.get("addressLocality", ""),
                    addr.get("streetAddress", ""),
                ]
                full_addr = " ".join(p for p in parts if p)
                if full_addr:
                    result["headquarters_address"] = full_addr
                if addr.get("postalCode"):
                    result["postal_code"] = str(addr["postalCode"])
            elif isinstance(addr, str):
                result["headquarters_address"] = addr
            # 会社名
            if data.get("name"):
                result["company_name_scraped"] = str(data["name"])
            # URL
            if data.get("url"):
                result["_jsonld_url"] = str(data["url"])
            # 従業員数
            if data.get("numberOfEmployees"):
                ne = data["numberOfEmployees"]
                if isinstance(ne, dict):
                    result["employee_count"] = str(ne.get("value", ""))
                else:
                    result["employee_count"] = str(ne)
            # SNS
            same_as = data.get("sameAs", [])
            if isinstance(same_as, str):
                same_as = [same_as]
            for s in same_as:
                if "facebook.com" in s:
                    result["facebook"] = s
                elif "linkedin.com" in s:
                    result["linkedin"] = s
                elif "wantedly.com" in s:
                    result["wantedly"] = s
        except Exception as e:
            logger.debug(f"[scraper] JSON-LD parse error: {e}")
    return result


# ─────────────────────────────────────────────
# 抽出手法2: OpenGraph タグ
# ─────────────────────────────────────────────

def _extract_opengraph(soup: BeautifulSoup) -> Dict:
    result: Dict = {}
    og_desc = soup.find("meta", property="og:description")
    if og_desc and og_desc.get("content"):
        result["meta_description"] = str(og_desc["content"])[:500]
    og_site = soup.find("meta", property="og:site_name")
    if og_site and og_site.get("content"):
        result["company_name_scraped"] = str(og_site["content"])
    kw_tag = soup.find("meta", attrs={"name": "keywords"})
    if kw_tag and kw_tag.get("content"):
        result["meta_keywords"] = str(kw_tag["content"])[:300]
    desc_tag = soup.find("meta", attrs={"name": "description"})
    if desc_tag and desc_tag.get("content"):
        result.setdefault("meta_description", str(desc_tag["content"])[:500])
    return result


# ─────────────────────────────────────────────
# 抽出手法3: tel:/mailto: リンク
# ─────────────────────────────────────────────

def _extract_links(soup: BeautifulSoup) -> Dict:
    result: Dict = {}
    for a in soup.find_all("a", href=True):
        href = str(a["href"])
        if href.startswith("tel:") and "phone_number" not in result:
            result["phone_number"] = href[4:].strip()
        elif href.startswith("mailto:") and "email" not in result:
            result["email"] = href[7:].strip().split("?")[0]
    return result


# ─────────────────────────────────────────────
# 抽出手法4: <dl>/<table> プロフィールテーブル
# ─────────────────────────────────────────────

def _extract_dl_tables(soup: BeautifulSoup) -> Dict:
    """<dl>/<table> からラベル-値ペアを抽出"""
    result: Dict = {}

    # --- <dl> パターン ---
    for dl in soup.find_all("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        for dt, dd in zip(dts, dds):
            label = dt.get_text(strip=True)
            value = dd.get_text(" ", strip=True)
            if not value:
                continue
            key = _match_label(label)
            if key:
                result.setdefault(key, value)

    # --- <table> パターン（2列テーブル: th/td or td/td） ---
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) < 2:
                continue
            label = cells[0].get_text(strip=True)
            value = " ".join(
                c.get_text(" ", strip=True) for c in cells[1:]
            ).strip()
            if not value:
                continue
            key = _match_label(label)
            if key:
                result.setdefault(key, value)

    return result


def _match_label(label: str) -> Optional[str]:
    """ラベル辞書でキーにマッチ"""
    label_clean = label.strip().rstrip("：:）)　 ")
    # 完全一致
    if label_clean in LABEL_MAP:
        return LABEL_MAP[label_clean]
    # 部分一致（ラベルがキーを含む）
    label_lower = label_clean.lower()
    for k, v in LABEL_MAP.items():
        if k.lower() in label_lower or label_lower in k.lower():
            return v
    return None


# ─────────────────────────────────────────────
# 抽出手法5: 正規表現ラベル辞書（フォールバック）
# ─────────────────────────────────────────────

def _extract_by_regex(text: str) -> Dict:
    """テキストから正規表現でフィールドを抽出（フォールバック）"""
    result: Dict = {}

    # 設立年月
    m = re.search(
        r"(?:設立|創立|創業)[^\d]{0,4}"
        r"((?:昭和|平成|令和)?[\d]{1,4}年[\d]{1,2}月(?:[\d]{1,2}日)?|"
        r"[\d]{4}[-/年][\d]{1,2}[-/月]?(?:[\d]{1,2}日?)?)",
        text,
    )
    if m:
        result.setdefault("established_date", m.group(1).strip())

    # 資本金
    m = re.search(
        r"資本金[^\d億万円]{0,4}"
        r"([\d,，]+(?:\.[0-9]+)?(?:億|百万|万)?円?)",
        text,
    )
    if m:
        result.setdefault("capital_stock", m.group(1).strip())

    # 従業員数
    m = re.search(
        r"(?:従業員数?|社員数|スタッフ数)[^\d]{0,4}([\d,，]+)\s*(?:名|人|名様)?",
        text,
    )
    if m:
        result.setdefault("employee_count", m.group(1).strip())

    # 郵便番号
    m = re.search(r"〒?\s*(\d{3}[-－]\d{4})", text)
    if m:
        result.setdefault("postal_code", m.group(1).replace("－", "-").replace("ー", "-"))

    # 電話番号（フォールバック）
    if "phone_number" not in result:
        m = re.search(r"(0\d{1,4}[-－ー]?\d{1,4}[-－ー]?\d{3,4})", text)
        if m:
            result.setdefault("phone_number", m.group(1))

    return result


# ─────────────────────────────────────────────
# 役員情報の抽出
# ─────────────────────────────────────────────

def _extract_executives(soup: BeautifulSoup) -> List[Tuple[str, str]]:
    """
    役員名と役職のペアリストを返す。
    Returns: [(name, position), ...]（最大10件）
    """
    executives: List[Tuple[str, str]] = []
    EXEC_TITLE_PAT = re.compile(
        r"(代表取締役|取締役|監査役|常務取締役|専務取締役|"
        r"代表|社長|副社長|会長|副会長|CEO|CFO|COO|CTO|執行役員)"
    )

    # <table> パターン
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"])
            texts = [c.get_text(strip=True) for c in cells]
            if len(texts) >= 2:
                if EXEC_TITLE_PAT.search(texts[0]):
                    executives.append((texts[1], texts[0]))
                elif EXEC_TITLE_PAT.search(texts[1]) and len(texts) >= 3:
                    executives.append((texts[0], texts[1]))

    # <dl> パターン
    for dl in soup.find_all("dl"):
        for dt, dd in zip(dl.find_all("dt"), dl.find_all("dd")):
            dt_text = dt.get_text(strip=True)
            dd_text = dd.get_text(strip=True)
            if EXEC_TITLE_PAT.search(dt_text):
                executives.append((dd_text, dt_text))

    # リスト系パターン（li に役職 + 氏名が入っているケース）
    for li in soup.find_all("li"):
        text = li.get_text(strip=True)
        m = EXEC_TITLE_PAT.search(text)
        if m:
            position = m.group(0)
            name_candidate = text.replace(position, "").strip()
            if 2 <= len(name_candidate) <= 20:
                executives.append((name_candidate, position))

    # 重複除去・上位10件
    seen: Set[str] = set()
    unique: List[Tuple[str, str]] = []
    for name, pos in executives:
        if name and name not in seen:
            seen.add(name)
            unique.append((name, pos))
        if len(unique) >= 10:
            break
    return unique


# ─────────────────────────────────────────────
# ページ1枚の統合抽出
# ─────────────────────────────────────────────

def _extract_from_html(html: str, url: str, priority: int = 5) -> Dict:
    """
    HTMLから全手法を使ってフィールドを抽出。
    priority: 優先度（低いほど上書き優先。1=JSON-LD, 2=tel/mailto, 3=dl/table, 4=OGP, 5=regex）
    """
    soup = BeautifulSoup(html, "html.parser")
    result: Dict = {}

    # 優先度順にマージ（既にある値は上書きしない setdefault）
    # 1. JSON-LD（最優先）
    for k, v in _extract_jsonld(soup).items():
        result.setdefault(k, v)
    # 2. tel:/mailto: リンク（電話・メールは誤抽出が少ない）
    for k, v in _extract_links(soup).items():
        result.setdefault(k, v)
    # 3. dl/table プロフィール
    for k, v in _extract_dl_tables(soup).items():
        result.setdefault(k, v)
    # 4. OpenGraph
    for k, v in _extract_opengraph(soup).items():
        result.setdefault(k, v)
    # 5. 正規表現フォールバック
    page_text = " ".join(soup.stripped_strings)
    for k, v in _extract_by_regex(page_text).items():
        result.setdefault(k, v)

    # 役員情報
    execs = _extract_executives(soup)
    if execs:
        result.setdefault("_executives", execs)

    result["_source_url"] = url
    result["_source_priority"] = priority
    return result


# ─────────────────────────────────────────────
# ページ優先度マージ
# ─────────────────────────────────────────────

def _merge_results(pages: List[Dict]) -> Dict:
    """
    複数ページの抽出結果をマージ。
    仕様書 8章: 構造化データ > 会社概要ページ > トップページ > その他ページ の優先順。
    """
    merged: Dict = {}
    # priority が小さいものを優先
    for page in sorted(pages, key=lambda d: d.get("_source_priority", 99)):
        for k, v in page.items():
            if k.startswith("_"):
                merged.setdefault(k, v)
                continue
            if v:
                merged.setdefault(k, v)
    return merged


# ─────────────────────────────────────────────
# メインエントリポイント
# ─────────────────────────────────────────────

def scrape_company(base_url: str, company_id: str = "?") -> Dict:
    """
    企業HP（base_url）から情報を収集して返す。

    探索順:
    1. トップページ（priority=3）
    2. 会社概要ページ（priority=1）
    3. お問い合わせページ（priority=4）
    4. 採用ページ（priority=5）

    Returns:
        抽出フィールドのdict。未取得フィールドはキーなし。
        役員は "_executives": [(name, position), ...] で返す。
    """
    logger.info(f"[{company_id}] Step3 scrape_company: {base_url}")
    all_pages: List[Dict] = []
    visited: Set[str] = set()

    def fetch_and_extract(url: str, priority: int) -> Optional[Tuple[BeautifulSoup, Dict]]:
        if url in visited:
            return None
        visited.add(url)
        html = _fetch_html(url)
        if not html:
            return None
        extracted = _extract_from_html(html, url, priority)
        logger.debug(
            f"[{company_id}] scraped {url} → fields: "
            f"{[k for k in extracted if not k.startswith('_') and extracted[k]]}"
        )
        all_pages.append(extracted)
        soup = BeautifulSoup(html, "html.parser")
        return soup, extracted

    # 1. トップページ
    ret = fetch_and_extract(base_url, priority=3)
    if ret is None:
        logger.warning(f"[{company_id}] Failed to fetch top page: {base_url}")
        return {}
    top_soup, _ = ret

    # サブページ候補を探索
    subpages = _find_subpages(base_url, top_soup)

    # 2. 会社概要ページ（最優先）
    if subpages.get("company"):
        fetch_and_extract(subpages["company"], priority=1)

    # 3. お問い合わせページ（電話・メール補完）
    if subpages.get("contact"):
        fetch_and_extract(subpages["contact"], priority=4)

    # 4. 採用ページ（従業員数・拠点数補完）
    if subpages.get("recruit"):
        fetch_and_extract(subpages["recruit"], priority=5)

    merged = _merge_results(all_pages)
    logger.info(
        f"[{company_id}] scrape done. Extracted fields: "
        f"{[k for k in merged if not k.startswith('_') and merged[k]]}"
    )
    return merged
