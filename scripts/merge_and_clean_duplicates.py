#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/merge_and_clean_duplicates.py
重複レコード（同名企業）を検知し、UUIDレコードから数値IDレコードへ
不足データをマージしたのち、UUIDレコードを削除するスクリプト。
※大量の出力は logs/ フォルダ内のログファイルに保存されます。
"""

import os
import psycopg2
import logging
from datetime import datetime

# --- 設定 ---
DB_HOST = os.getenv("POSTGRES_HOST", "34.84.189.233")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "Legatus2000/")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")

# ★安全装置：最初は True（表示のみ）に設定。結果を見て問題なければ False に変更して実行します。
DRY_RUN = False

# --- ログファイルの設定 ---
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)  # logsフォルダがなければ自動作成
log_filename = os.path.join(log_dir, f"merge_duplicates_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# loggerの基本設定（ファイルにのみ詳細を出力する）
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

def is_empty(val):
    """値が空（NULL または 空文字）か判定"""
    if val is None:
        return True
    if isinstance(val, str) and str(val).strip() == "":
        return True
    return False

def main():
    print(f"処理を開始します。")
    print(f"詳細なマージ内容はログファイルに出力されます: {log_filename}")
    logger.info(f"=== 重複データ マージ＆クレンジング処理 {'(テストモード: DB更新なし)' if DRY_RUN else '(本番実行: DB更新あり)'} ===")
    
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER,
            password=DB_PASSWORD, dbname=DB_NAME, sslmode=DB_SSLMODE
        )
        cur = conn.cursor()
        
        # 1. 同名の企業が複数存在する「企業名」をリストアップ
        cur.execute("""
            SELECT name 
            FROM companies 
            WHERE name IS NOT NULL AND name != ''
            GROUP BY name 
            HAVING COUNT(*) > 1;
        """)
        duplicate_names = [row[0] for row in cur.fetchall()]
        
        if not duplicate_names:
            msg = "重複している企業名は見つかりませんでした。"
            print(msg)
            logger.info(msg)
            return
            
        total_duplicates = len(duplicate_names)
        msg = f"重複の可能性がある企業名: {total_duplicates} 件検出"
        print(msg)
        logger.info(msg)

        processed_count = 0

        # 2. 企業名ごとに処理
        for index, name in enumerate(duplicate_names, 1):
            # ターミナル用：100件ごとに進捗を表示
            if index % 100 == 0 or index == total_duplicates:
                print(f"進捗: {index} / {total_duplicates} 件をチェック中...")

            cur.execute("SELECT * FROM companies WHERE name = %s;", (name,))
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
            records = [dict(zip(colnames, row)) for row in rows]
            
            # 正レコード（マスター）の条件：IDが数字のみ ＆ 法人番号が入っている
            masters = [r for r in records if str(r['id']).isdigit() and not is_empty(r['corporate_number'])]
            # 副レコード（削除対象）の条件：IDにハイフンが含まれる（UUID形式）
            uuids = [r for r in records if '-' in str(r['id'])]
            
            # マスターが1件で、UUIDレコードが1件以上ある場合のみ自動処理を行う
            if len(masters) == 1 and len(uuids) > 0:
                master = masters[0]
                master_id = master['id']
                updates = {}
                
                # UUIDレコードから不足データをかき集める
                for u_rec in uuids:
                    for col in colnames:
                        # 以下のカラムはマージ対象外（上書きしない）
                        if col in ('id', 'created_at', 'updated_at', 'corporate_number'):
                            continue
                            
                        # マスターが空で、UUID側に値がある場合
                        if is_empty(master[col]) and not is_empty(u_rec[col]):
                            updates[col] = u_rec[col]
                            # 他のUUIDレコードで上書きしないよう、ローカルのマスター状態も更新しておく
                            master[col] = u_rec[col]
                
                delete_ids = [u['id'] for u in uuids]
                
                # --- ログファイルへの書き出し ---
                logger.info("-" * 60)
                logger.info(f"【対象】{name}")
                logger.info(f"  [残すマスターID] {master_id} (法人番号: {master['corporate_number']})")
                logger.info(f"  [削除されるUUID] {', '.join(map(str, delete_ids))}")
                
                if updates:
                    logger.info("  [マージされるデータ (不足分を補完)]")
                    for k, v in updates.items():
                        disp_v = (str(v)[:60] + '...') if len(str(v)) > 60 else str(v)
                        logger.info(f"    - {k}: {disp_v}")
                else:
                    logger.info("  [マージされるデータ] なし（補完すべき不足項目はありません）")

                # --- 本番実行（DB更新） ---
                if not DRY_RUN:
                    # 1. マスターレコードへの UPDATE (マージ)
                    if updates:
                        set_clause = ", ".join([f"{k} = %s" for k in updates.keys()])
                        set_clause += ", updated_at = NOW()"
                        values = list(updates.values()) + [master_id]
                        
                        update_sql = f"UPDATE companies SET {set_clause} WHERE id = %s"
                        cur.execute(update_sql, values)
                    
                    # 2. 用済みとなったUUIDレコードの DELETE (クレンジング)
                    delete_sql = "DELETE FROM companies WHERE id = ANY(%s)"
                    cur.execute(delete_sql, (delete_ids,))
                    
                processed_count += 1

        # DBに変更を反映（本番モードのみ）
        if not DRY_RUN:
            conn.commit()
            msg = f"\n完了: {processed_count} 件の企業データをマージ＆クレンジングしました。"
            print(msg)
            logger.info(msg)
        else:
            print("\n---------------------------------------------------")
            print(f"テスト完了: 自動マージの対象となる企業は {processed_count} 件でした。")
            print(f"どのようなデータがマージされるかは {log_filename} を開いて確認してください。")
            print("問題なければ、スクリプト内の DRY_RUN = False に変更して再実行してください。")
            print("---------------------------------------------------")

    except Exception as e:
        if conn and not DRY_RUN:
            conn.rollback()
        print(f"エラーが発生しました: {e}")
        logger.error(f"エラーが発生しました: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    main()