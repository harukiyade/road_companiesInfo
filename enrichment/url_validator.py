"""
url_validator.py - HP候補の検証・確信度スコアリング（Step2）

仕様書 6章:
- robots.txt を必ず確認し禁止パスは除外
- 会社名一致・電話番号一致・住所一致・法人番号一致・SSL等でスコアリング
- 確信度: high(>=70) / medium(>=40) / low(>0) / not_found
- 同一URLが別法人に紐づいていないかチェック（used_urls set）
- 指数バックオフ付きHTTPリクエスト
"""
from __future__ import annotations

import logging
import re
import time
from threading import Lock
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup

from .config import (
    HTTP_TIMEOUT, HTTP_MAX_RETRIES, HTTP_BACKOFF_BASE, HTTP_USER_AGENT,
    SCORE_NAME_EXACT, SCORE_NAME_PARTIAL, SCORE_NAME_SHORT,
    SCORE_PHONE_MATCH, SCORE_PHONE_MISMATCH,
    SCORE_ADDRESS_MATCH, SCORE_ADDRESS_MISMATCH,
    SCORE_CORPORATE_NUMBER, SCORE_SSL, SCORE_GBIZ_PREF_MATCH,
    CONFIDENCE_HIGH_THRESHOLD, CONFIDENCE_MEDIUM_THRESHOLD,
    DOMAIN_MIN_INTERVAL,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# robots.txt キャッシュ（グローバル）
# ─────────────────────────────────────────────
_robots_cache: Dict[str, RobotFileParser] = {}
_robots_lock = Lock()

# ─────────────────────────────────────────────
# ドメインレートリミッタ（最終アクセス時刻の記録）
# ─────────────────────────────────────────────
_domain_last_access: Dict[str, float] = {}
_domain_lock = Lock()


def _rate_limit(domain: str) -> None:
    """同一ドメインへのアクセス間隔を DOMAIN_MIN_INTERVAL 秒以上に保つ"""
    with _domain_lock:
        last = _domain_last_access.get(domain, 0.0)
        elapsed = time.monotonic() - last
        wait = DOMAIN_MIN_INTERVAL - elapsed
        if wait > 0:
            time.sleep(wait)
        _domain_last_access[domain] = time.monotonic()


def _can_fetch(url: str) -> bool:
    """robots.txt を確認してクロール許可か判定（キャッシュ付き）"""
    parsed = urlparse(url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    with _robots_lock:
        if robots_url not in _robots_cache:
            rp = RobotFileParser(robots_url)
            try:
                rp.read()
            except Exception:
                # robots.txt 取得失敗 → 許可とみなす
                pass
            _robots_cache[robots_url] = rp
        return _robots_cache[robots_url].can_fetch(HTTP_USER_AGENT, url)


def _fetch_html(url: str) -> Optional[str]:
    """
    robots.txt チェック + レートリミット付きページ取得。
    Returns: HTML文字列 or None
    """
    if not _can_fetch(url):
        logger.info(f"[url_validator] robots.txt blocked: {url}")
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
                logger.debug(f"[url_validator] 4xx {resp.status_code}: {url}")
                return None
            wait = HTTP_BACKOFF_BASE ** attempt
            logger.warning(
                f"[url_validator] 5xx {resp.status_code}: {url} → retry {attempt+1} in {wait}s"
            )
            if attempt < HTTP_MAX_RETRIES:
                time.sleep(wait)
        except requests.exceptions.Timeout:
            wait = HTTP_BACKOFF_BASE ** attempt
            logger.warning(f"[url_validator] Timeout: {url} → retry {attempt+1} in {wait}s")
            if attempt < HTTP_MAX_RETRIES:
                time.sleep(wait)
        except requests.exceptions.RequestException as e:
            logger.error(f"[url_validator] RequestException {url}: {e}")
            return None
    return None


# ─────────────────────────────────────────────
# スコアリング補助関数
# ─────────────────────────────────────────────

def _normalize_phone(phone: str) -> str:
    """電話番号を正規化（全角→半角、記号除去）"""
    # maketrans の from/to は文字数が一致している必要がある
    # 全角数字10 + ハイフン類3 + 括弧2 + 全角スペース1 = 16
    phone = phone.translate(str.maketrans(
        "０１２３４５６７８９ー－―（）　",
        "0123456789---() ",
    ))
    return re.sub(r"[^\d]", "", phone)


def _extract_page_phones(html_text: str) -> List[str]:
    """ページから電話番号を抽出（tel: リンク優先）"""
    soup = BeautifulSoup(html_text, "html.parser")
    phones: List[str] = []
    # tel: リンク（最優先）
    for a in soup.find_all("a", href=re.compile(r"^tel:")):
        tel = re.sub(r"^tel:", "", a["href"])
        phones.append(_normalize_phone(tel))
    # テキストから正規表現抽出
    text = soup.get_text(" ")
    for m in re.finditer(r"(0\d{1,4}[-－ー]?\d{1,4}[-－ー]?\d{3,4})", text):
        phones.append(_normalize_phone(m.group(1)))
    return phones


def _extract_page_text(html_text: str, max_chars: int = 8000) -> str:
    """スクリプト/スタイル除去後のプレーンテキストを返す"""
    soup = BeautifulSoup(html_text, "html.parser")
    for tag in soup(["script", "style", "noscript", "head"]):
        tag.decompose()
    return " ".join(soup.stripped_strings)[:max_chars]


def _extract_title(html_text: str) -> str:
    soup = BeautifulSoup(html_text, "html.parser")
    if soup.title and soup.title.string:
        return soup.title.string.strip()
    return ""


def _strip_legal_form(name: str) -> str:
    """会社名から法人格を除去して短縮名を返す"""
    pattern = (
        r"(株式会社|有限会社|合同会社|合資会社|合名会社|"
        r"社団法人|財団法人|医療法人|NPO法人|"
        r"一般社団法人|公益財団法人|一般財団法人|公益社団法人|"
        r"学校法人|宗教法人|特定非営利活動法人)\s*"
    )
    return re.sub(pattern, "", name).strip()


# ─────────────────────────────────────────────
# スコアリング本体
# ─────────────────────────────────────────────

def score_url(
    url: str,
    row: Dict,
    gbiz_info: Optional[Dict] = None,
    used_urls: Optional[Set[str]] = None,
) -> Tuple[int, str, str]:
    """
    URLを検証してスコアと確信度を計算。

    Args:
        url: 検証対象URL
        row: companies行データ
        gbiz_info: gBizINFOから取得した法人情報（None可）
        used_urls: 既に他社に紐付け済みのURLセット（重複防止）

    Returns:
        (score, confidence, reason_detail)
        confidence: 'high' | 'medium' | 'low' | 'not_found'
    """
    reasons: List[str] = []

    # 重複紐付けチェック
    if used_urls and url in used_urls:
        logger.debug(f"[url_validator] URL already used: {url}")
        return 0, "not_found", "already_used_by_another_company"

    # ページ取得
    html = _fetch_html(url)
    if html is None:
        return 0, "not_found", "page_fetch_failed"

    title = _extract_title(html)
    page_text = _extract_page_text(html)
    page_phones = _extract_page_phones(html)

    company_name = str(row.get("company_name", row.get("name", ""))).strip()
    existing_phone = str(row.get("phone_number", row.get("tel", ""))).strip()
    prefecture = str(row.get("prefecture", row.get("prefecture_name", ""))).strip()
    corporate_number = re.sub(r"\D", "", str(row.get("corporate_number", "")))

    score = 0

    # ── 会社名マッチ ──
    if company_name:
        if company_name in title:
            score += SCORE_NAME_EXACT
            reasons.append(f"name_in_title+{SCORE_NAME_EXACT}")
        elif company_name in page_text:
            score += SCORE_NAME_PARTIAL
            reasons.append(f"name_in_text+{SCORE_NAME_PARTIAL}")
        else:
            short = _strip_legal_form(company_name)
            if short and len(short) >= 2:
                if short in title:
                    score += SCORE_NAME_PARTIAL
                    reasons.append(f"shortname_in_title+{SCORE_NAME_PARTIAL}")
                elif short in page_text:
                    score += SCORE_NAME_SHORT
                    reasons.append(f"shortname_in_text+{SCORE_NAME_SHORT}")

    # ── 電話番号マッチ ──
    if existing_phone:
        existing_norm = _normalize_phone(existing_phone)
        if existing_norm and any(p == existing_norm for p in page_phones):
            score += SCORE_PHONE_MATCH
            reasons.append(f"phone_match+{SCORE_PHONE_MATCH}")
        elif existing_norm and page_phones:
            # 電話番号がページにあるが一致しない
            score += SCORE_PHONE_MISMATCH
            reasons.append(f"phone_mismatch{SCORE_PHONE_MISMATCH}")

    # ── 都道府県（住所）マッチ ──
    if prefecture:
        if prefecture in page_text:
            score += SCORE_ADDRESS_MATCH
            reasons.append(f"pref_match+{SCORE_ADDRESS_MATCH}")
        else:
            score += SCORE_ADDRESS_MISMATCH
            reasons.append(f"pref_mismatch{SCORE_ADDRESS_MISMATCH}")

    # ── 法人番号マッチ ──
    if len(corporate_number) == 13:
        text_digits = re.sub(r"\D", "", page_text)
        if corporate_number in text_digits:
            score += SCORE_CORPORATE_NUMBER
            reasons.append(f"corporate_number_match+{SCORE_CORPORATE_NUMBER}")

    # ── SSL ──
    if url.startswith("https://"):
        score += SCORE_SSL
        reasons.append(f"ssl+{SCORE_SSL}")

    # ── gBizINFO都道府県補完検証 ──
    if gbiz_info:
        gbiz_pref = gbiz_info.get("prefecture_name", "")
        if gbiz_pref and gbiz_pref in page_text:
            score += SCORE_GBIZ_PREF_MATCH
            reasons.append(f"gbiz_pref_match+{SCORE_GBIZ_PREF_MATCH}")

    # ── 確信度判定 ──
    if score >= CONFIDENCE_HIGH_THRESHOLD:
        confidence = "high"
    elif score >= CONFIDENCE_MEDIUM_THRESHOLD:
        confidence = "medium"
    elif score > 0:
        confidence = "low"
    else:
        confidence = "not_found"

    reason_str = ", ".join(reasons)
    logger.debug(
        f"[url_validator] score={score} ({confidence}) {url} | {reason_str}"
    )
    return score, confidence, reason_str


def find_best_url(
    candidates: List[str],
    row: Dict,
    gbiz_info: Optional[Dict] = None,
    used_urls: Optional[Set[str]] = None,
) -> Tuple[Optional[str], str, str]:
    """
    複数候補URLから最高スコアのURLを選択。

    Returns:
        (best_url, confidence, reason)
    """
    if not candidates:
        return None, "not_found", "no_candidates"

    best_url: Optional[str] = None
    best_score = -1
    best_confidence = "not_found"
    best_reason = "no_candidates"

    for url in candidates:
        sc, conf, reason = score_url(url, row, gbiz_info, used_urls)
        logger.info(
            f"[url_validator] [{row.get('id','?')}] {url} → score={sc} ({conf})"
        )
        if sc > best_score:
            best_score = sc
            best_url = url
            best_confidence = conf
            best_reason = reason

    if best_confidence == "not_found":
        best_url = None

    return best_url, best_confidence, best_reason
