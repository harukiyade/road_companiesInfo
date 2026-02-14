#!/usr/bin/env python3
"""
EDINET API 生存確認用最小スクリプト

403 の原因特定のため、最もシンプルな GET で書類一覧を取得し、
User-Agent を切り替えてどの組み合わせが通るか試行する。

使い方:
  python scripts/edinet_api_ping.py
  EDINET_API_KEY=xxx python scripts/edinet_api_ping.py
"""

import os
import sys
from datetime import datetime, timedelta

try:
    import requests
except ImportError:
    print("エラー: pip install requests")
    sys.exit(1)

EDINET_API_BASE = "https://disclosure.edinet-fsa.go.jp/api/v2"

USER_AGENTS = {
    "Chrome": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Safari": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/17.0 Safari/605.1.15"
    ),
    "Python-requests": "python-requests/2.31.0",
}


def main():
    api_key = os.getenv("EDINET_API_KEY", "")
    date_str = (datetime.now().date() - timedelta(days=1)).strftime("%Y-%m-%d")
    url = f"{EDINET_API_BASE}/documents.json"
    params = {"date": date_str, "type": "2"}
    if api_key:
        params["Subscription-Key"] = api_key

    print("=" * 60)
    print("EDINET API 生存確認 (Version 2)")
    print("=" * 60)
    print(f"URL: {url}")
    print(f"params: date, type, Subscription-Key={'セット済み' if api_key else '未設定'}")
    if api_key:
        print(f"  (先頭8文字: {api_key[:8]}...)")
    print()

    headers_base = {
        "Accept": "application/json",
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    }

    results = {}
    success_data = None

    for name, ua in USER_AGENTS.items():
        headers = {**headers_base, "User-Agent": ua}
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=30)
            results[name] = resp.status_code
            if resp.status_code == 200:
                success_data = resp.json()
        except Exception as e:
            results[name] = str(e)

    for name, status in results.items():
        mark = "✅" if status == 200 else "❌"
        print(f"  {mark} {name}: {status}")

    print()
    if success_data:
        meta = success_data.get("metadata", {})
        count = meta.get("resultset", {}).get("count", "?")
        print(f"成功レスポンス: count = {count}")
        print(f"metadata 一部: {str(meta)[:300]}...")
    else:
        print("全ての User-Agent で 403 またはエラーとなりました。")
        sys.exit(1)


if __name__ == "__main__":
    main()
