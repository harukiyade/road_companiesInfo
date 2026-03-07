import requests
import os
import time

# 城山さんの実際のEDINET APIキーを設定済みです
API_KEY = "3a1cd8b3d52748bd922e43ad78fa73be" 

# 保存先ディレクトリの作成
SAVE_DIR = "edinet_data"
os.makedirs(SAVE_DIR, exist_ok=True)

def get_doc_ids(target_date):
    """指定した日付の有価証券報告書（docTypeCode=120）のdocIDリストを取得"""
    url = "https://disclosure.edinet-fsa.go.jp/api/v2/documents.json"
    params = {
        "date": target_date,
        "type": 2,
        "Subscription-Key": API_KEY
    }
    
    print(f"[{target_date}] の書類一覧を取得中...")
    response = requests.get(url, params=params)
    
    if response.status_code != 200:
        print(f"一覧取得エラー: HTTP {response.status_code}")
        return []
        
    data = response.json()
    doc_ids = []
    
    # 結果から有価証券報告書（120）のみを抽出
    if "results" in data:
        for doc in data["results"]:
            if doc.get("docTypeCode") == "120":
                doc_ids.append(doc["docID"])
                
    return doc_ids

def download_zip(doc_id):
    """docIDを指定してZIPファイルをダウンロード"""
    url = f"https://disclosure.edinet-fsa.go.jp/api/v2/documents/{doc_id}"
    params = {
        "type": 1, # 1=ZIPファイル取得
        "Subscription-Key": API_KEY
    }
    
    save_path = os.path.join(SAVE_DIR, f"{doc_id}.zip")
    
    # 既にダウンロード済みの場合はスキップ（途中で止まっても再開可能にするため）
    if os.path.exists(save_path):
        print(f"{doc_id}.zip は取得済みのためスキップします。")
        return
        
    print(f"{doc_id} のZIPデータをダウンロード中...")
    response = requests.get(url, params=params, stream=True)
    
    if response.status_code == 200:
        with open(save_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"保存完了: {save_path}")
    else:
        print(f"ダウンロードエラー ({doc_id}): HTTP {response.status_code}")

if __name__ == "__main__":
    print("【テストモード】指定した1日分の有価証券報告書をダウンロードします...")
    
    # テストとして、提出が集中する「2025年6月27日」の1日分だけ実行
    target_date = "2025-06-27"
    
    # 1. docIDリストの取得
    target_doc_ids = get_doc_ids(target_date)
    print(f"\n[{target_date}] 対象となる有価証券報告書の数: {len(target_doc_ids)}件\n")
    
    # 2. ZIPファイルのダウンロード
    for doc_id in target_doc_ids:
        download_zip(doc_id)
        # EDINETサーバーへの負荷を考慮し、リクエスト間に1秒の待機を入れる
        time.sleep(1)
        
    print("\nテスト用ダウンロード処理が完了しました。")