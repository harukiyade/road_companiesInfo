"""
url_finder.py - HP候補の特定ロジック（Step1）

仕様書 5章:
優先度1: 既存カラム（url/urls/homepage_url/website_url/company_url/contact_form_url）
優先度2: gBizINFO API（法人番号がある場合、住所・名称の裏取りに使用）
優先度3&4: Web検索API（Google Custom Search / Bing Search）でHP候補を抽出

HTTPリクエストは指数バックオフ付きリトライ（最大3回）。
"""
from __future__ import annotations

import logging
import re
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests

from .config import (
    GBIZ_INFO_API_BASE, GBIZ_INFO_API_KEY,
    GOOGLE_CSE_API_KEY, GOOGLE_CSE_CX,
    BING_API_KEY,
    HTTP_TIMEOUT, HTTP_MAX_RETRIES, HTTP_BACKOFF_BASE,
    HTTP_USER_AGENT,
    EXCLUDED_DOMAINS, URL_CANDIDATE_COLUMNS,
    SEARCH_API_INTERVAL, SEARCH_TIMEOUT, SEARCH_MAX_RETRIES, SEARCH_BACKOFF_BASE,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# ユーティリティ
# ─────────────────────────────────────────────

def is_excluded_domain(url: str) -> bool:
    """SNS・ポータル等の除外ドメインか判定"""
    try:
        netloc = urlparse(url).netloc.lower().lstrip("www.")
        for exc in EXCLUDED_DOMAINS:
            if netloc == exc or netloc.endswith("." + exc):
                return True
    except Exception:
        pass
    return False


def normalize_url(raw: str) -> Optional[str]:
    """URLを正規化。スキームがなければ https:// を付与"""
    if not raw:
        return None
    raw = raw.strip().rstrip("/")
    # 全角→半角
    raw = raw.translate(str.maketrans(
        "ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ"
        "ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ"
        "０１２３４５６７８９．：／",
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "0123456789./:",
    ))
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw
    try:
        parsed = urlparse(raw)
        if parsed.scheme in ("http", "https") and parsed.netloc:
            return raw
    except Exception:
        pass
    return None


def _http_get(
    url: str,
    params: Optional[Dict] = None,
    headers: Optional[Dict] = None,
    timeout: int = HTTP_TIMEOUT,
    max_retries: int = HTTP_MAX_RETRIES,
    backoff_base: float = HTTP_BACKOFF_BASE,
) -> Optional[requests.Response]:
    """
    指数バックオフ付き GET。
    Timeout / ConnectionError / 5xx はリトライ。4xx は即 None。
    """
    _headers = {"User-Agent": HTTP_USER_AGENT}
    if headers:
        _headers.update(headers)
    last_err: Optional[BaseException] = None
    for attempt in range(max_retries + 1):
        try:
            resp = requests.get(
                url, params=params, headers=_headers,
                timeout=timeout, allow_redirects=True,
            )
            if resp.status_code < 400:
                return resp
            if 400 <= resp.status_code < 500:
                logger.debug(f"[url_finder] 4xx {resp.status_code} {url}")
                return None
            wait = backoff_base ** attempt
            logger.warning(
                f"[url_finder] 5xx {resp.status_code} {url} "
                f"→ retry {attempt+1}/{max_retries} in {wait:.1f}s"
            )
            if attempt < max_retries:
                time.sleep(wait)
        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError) as e:
            last_err = e
            wait = backoff_base ** attempt
            logger.warning(
                f"[url_finder] {type(e).__name__}: {e} | {url} "
                f"→ retry {attempt+1}/{max_retries} in {wait:.1f}s"
            )
            if attempt < max_retries:
                time.sleep(wait)
        except requests.exceptions.RequestException as e:
            last_err = e
            wait = backoff_base ** attempt
            logger.warning(
                f"[url_finder] RequestException: {e} | {url} "
                f"→ retry {attempt+1}/{max_retries} in {wait:.1f}s"
            )
            if attempt < max_retries:
                time.sleep(wait)
            else:
                break
    logger.error(f"[url_finder] All retries exhausted for {url} (last={last_err})")
    return None


def _with_search_retry(label: str, fn):
    """
    検索API呼び出し用リトライラッパー。
    Timeout / ConnectionError / 一時障害を捕捉し指数バックオフで再試行。
    全滅しても例外を上げず空リスト相当の結果を返す（呼び出し側が [] を期待）。
    """
    last_err: Optional[BaseException] = None
    for attempt in range(SEARCH_MAX_RETRIES + 1):
        try:
            return fn()
        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                TimeoutError,
                OSError) as e:
            last_err = e
            wait = SEARCH_BACKOFF_BASE ** attempt
            logger.warning(
                f"[url_finder] {label} {type(e).__name__}: {e} "
                f"→ retry {attempt+1}/{SEARCH_MAX_RETRIES} in {wait:.1f}s"
            )
            if attempt < SEARCH_MAX_RETRIES:
                time.sleep(wait)
        except Exception as e:
            # duckduckgo_search 等は独自例外で "timed out" を含むことがある
            msg = str(e).lower()
            is_transient = (
                "timed out" in msg
                or "timeout" in msg
                or "temporar" in msg
                or "connection" in msg
                or "reset" in msg
                or "503" in msg
                or "429" in msg
            )
            last_err = e
            if not is_transient:
                logger.warning(f"[url_finder] {label} non-retryable: {e}")
                return None
            wait = SEARCH_BACKOFF_BASE ** attempt
            logger.warning(
                f"[url_finder] {label} transient error: {e} "
                f"→ retry {attempt+1}/{SEARCH_MAX_RETRIES} in {wait:.1f}s"
            )
            if attempt < SEARCH_MAX_RETRIES:
                time.sleep(wait)
    logger.error(f"[url_finder] {label} all retries exhausted (last={last_err})")
    return None


# ─────────────────────────────────────────────
# 優先度1: 既存カラムから
# ─────────────────────────────────────────────

def find_url_from_existing_columns(row: Dict) -> Optional[str]:
    """既存カラムからHP候補を取得（コスト0）"""
    for col in URL_CANDIDATE_COLUMNS:
        raw = row.get(col)
        if not raw:
            continue
        raw = str(raw).strip()
        if raw in ("", "nan", "None", "null"):
            continue
        # 複数URL が ; や , で区切られている場合は最初の1つ
        for sep in (";", ",", " "):
            if sep in raw:
                raw = raw.split(sep)[0].strip()
                break
        url = normalize_url(raw)
        if url and not is_excluded_domain(url):
            logger.debug(f"[url_finder] existing_column '{col}': {url}")
            return url
    return None


# ─────────────────────────────────────────────
# 優先度2: gBizINFO API
# ─────────────────────────────────────────────

def fetch_gbizinfo(corporate_number: str) -> Optional[Dict]:
    """
    gBizINFO API で法人情報を取得。
    ※URLは持たないため住所・名称の裏取り専用。
    Returns: dict (name, prefecture_name, city_name, post_code, ...) or None
    """
    if not GBIZ_INFO_API_KEY:
        return None
    if not corporate_number:
        return None
    cn = re.sub(r"\D", "", str(corporate_number))
    if len(cn) != 13:
        return None
    endpoint = f"{GBIZ_INFO_API_BASE}/{cn}"
    headers = {"X-hojinInfo-api-token": GBIZ_INFO_API_KEY}
    resp = _http_get(endpoint, headers=headers)
    if resp is None:
        return None
    try:
        data = resp.json()
        items = data.get("hojin-infos", [])
        if not items:
            return None
        h = items[0]
        return {
            "name": h.get("name", ""),
            "prefecture_name": h.get("prefecture_name", ""),
            "city_name": h.get("city_name", ""),
            "street_number": h.get("street_number", ""),
            "post_code": h.get("post_code", ""),
            "corporate_number": h.get("corporate_number", ""),
            "close_cause": h.get("close_cause", ""),
        }
    except Exception as e:
        logger.error(f"[url_finder] gBizINFO parse error for {cn}: {e}")
        return None


# ─────────────────────────────────────────────
# 優先度3&4: Web検索API
# ─────────────────────────────────────────────

def _search_google(company_name: str, prefecture: str) -> List[str]:
    """Google Custom Search JSON API でHP候補URLリストを取得"""
    if not (GOOGLE_CSE_API_KEY and GOOGLE_CSE_CX):
        return []
    query = f"{company_name} {prefecture} 公式サイト".strip()
    params = {
        "key": GOOGLE_CSE_API_KEY,
        "cx": GOOGLE_CSE_CX,
        "q": query,
        "num": 10,
        "hl": "ja",
        "lr": "lang_ja",
    }
    resp = _http_get(
        "https://www.googleapis.com/customsearch/v1",
        params=params,
        timeout=SEARCH_TIMEOUT,
        max_retries=SEARCH_MAX_RETRIES,
        backoff_base=SEARCH_BACKOFF_BASE,
    )
    time.sleep(SEARCH_API_INTERVAL)
    if resp is None:
        return []
    try:
        data = resp.json()
        urls = []
        for item in data.get("items", []):
            url = item.get("link", "")
            if url and not is_excluded_domain(url):
                urls.append(url)
        return urls
    except Exception as e:
        logger.error(f"[url_finder] Google CSE parse error: {e}")
        return []


def _search_bing(company_name: str, prefecture: str) -> List[str]:
    """Bing Search API でHP候補URLリストを取得（タイムアウト耐性付き）"""
    if not BING_API_KEY:
        return []
    query = f"{company_name} {prefecture} 公式".strip()
    headers = {"Ocp-Apim-Subscription-Key": BING_API_KEY}
    params = {"q": query, "count": 10, "mkt": "ja-JP", "setLang": "ja"}
    resp = _http_get(
        "https://api.bing.microsoft.com/v7.0/search",
        params=params,
        headers=headers,
        timeout=SEARCH_TIMEOUT,
        max_retries=SEARCH_MAX_RETRIES,
        backoff_base=SEARCH_BACKOFF_BASE,
    )
    time.sleep(SEARCH_API_INTERVAL)
    if resp is None:
        return []
    try:
        data = resp.json()
        urls = []
        for item in data.get("webPages", {}).get("value", []):
            url = item.get("url", "")
            if url and not is_excluded_domain(url):
                urls.append(url)
        return urls
    except Exception as e:
        logger.error(f"[url_finder] Bing parse error: {e}")
        return []


def _search_duckduckgo(company_name: str, prefecture: str) -> List[str]:
    """
    DuckDuckGo 検索（フォールバック・キー不要）。
    operation timed out 等を捕捉して SEARCH_MAX_RETRIES 回リトライ。
    """
    try:
        from duckduckgo_search import DDGS
    except ImportError:
        logger.debug("[url_finder] duckduckgo_search not installed, skipping")
        return []

    query = f"{company_name} {prefecture} 公式サイト".strip()

    def _do() -> List[str]:
        results: List[str] = []
        try:
            ddgs = DDGS(timeout=SEARCH_TIMEOUT)
        except TypeError:
            ddgs = DDGS()
        try:
            with ddgs:
                for r in ddgs.text(query, max_results=8, region="jp-jpn"):
                    url = r.get("href", "")
                    if url and not is_excluded_domain(url):
                        results.append(url)
        except TypeError:
            for r in ddgs.text(query, max_results=8, region="jp-jpn"):
                url = r.get("href", "")
                if url and not is_excluded_domain(url):
                    results.append(url)
        return results

    result = _with_search_retry("DuckDuckGo", _do)
    time.sleep(SEARCH_API_INTERVAL)
    return result if isinstance(result, list) else []
# ─────────────────────────────────────────────
# メインエントリポイント
# ─────────────────────────────────────────────

def find_hp_candidates(row: Dict) -> Tuple[List[str], str, Optional[Dict]]:
    """
    企業行データから HP候補URL リストと特定手法名を返す。

    Args:
        row: companies テーブルの1行（dict）

    Returns:
        (candidate_urls, source_method, gbiz_info)
        - candidate_urls: 候補URLリスト（先頭優先、最大5件）
        - source_method: 'existing_column' / 'web_search' / 'none'
        - gbiz_info: gBizINFOから取得した法人情報（None の場合あり）
    """
    company_id = str(row.get("id", "?"))
    company_name = str(row.get("company_name", row.get("name", ""))).strip()
    prefecture = str(row.get("prefecture", row.get("prefecture_name", ""))).strip()
    corporate_number = str(row.get("corporate_number", "")).strip()

    gbiz_info: Optional[Dict] = None

    # --- 優先度1: 既存カラム ---
    existing_url = find_url_from_existing_columns(row)
    if existing_url:
        logger.info(f"[{company_id}] Step1 existing_column → {existing_url}")
        # gBizINFO は裏取りのためバックグラウンドで取得
        if corporate_number and GBIZ_INFO_API_KEY:
            gbiz_info = fetch_gbizinfo(corporate_number)
        return [existing_url], "existing_column", gbiz_info

    # --- 優先度2: gBizINFO ---
    if corporate_number and GBIZ_INFO_API_KEY:
        gbiz_info = fetch_gbizinfo(corporate_number)
        if gbiz_info:
            logger.debug(
                f"[{company_id}] gBizINFO fetched: {gbiz_info.get('prefecture_name','')} "
                f"{gbiz_info.get('city_name','')}"
            )

    # --- 優先度3&4: Web検索 ---
    if not company_name:
        logger.warning(f"[{company_id}] No company_name, cannot web search")
        return [], "none", gbiz_info

    # Google → Bing → DuckDuckGo の順にフォールバック
    candidates: List[str] = []
    if GOOGLE_CSE_API_KEY and GOOGLE_CSE_CX:
        candidates = _search_google(company_name, prefecture)
    if not candidates and BING_API_KEY:
        candidates = _search_bing(company_name, prefecture)
    if not candidates:
        candidates = _search_duckduckgo(company_name, prefecture)

    if candidates:
        logger.info(
            f"[{company_id}] Step1 web_search → {len(candidates)} candidates "
            f"(first: {candidates[0]})"
        )
        return candidates[:5], "web_search", gbiz_info

    logger.info(f"[{company_id}] Step1 → no candidates found")
    return [], "none", gbiz_info
