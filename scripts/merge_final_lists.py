import pandas as pd
import os

def merge_lists():
    # 入力ファイル
    FILE_SMART = "out/final_smart_target.csv"    # 質重視
    FILE_RECENCY = "out/final_target_companies.csv" # 鮮度重視
    OUTPUT_FILE = "out/merged_master_list.csv"

    print("リストを読み込み中...")
    
    # 1. Smartリスト（優先度高）
    # こちらには「employee_count」が入っているので、こちらを正にします
    try:
        df_smart = pd.read_csv(FILE_SMART, dtype=str)
        print(f"  Smartリスト: {len(df_smart):,} 件")
    except:
        print("Error: Smartリストが見つかりません")
        return

    # 2. Recencyリスト（補充用）
    try:
        df_recency = pd.read_csv(FILE_RECENCY, dtype=str)
        print(f"  Recencyリスト: {len(df_recency):,} 件")
    except:
        print("Error: Recencyリストが見つかりません")
        return

    # --- 合体処理 ---
    print("突合（マージ）中...")
    
    # Smartリストを上に、Recencyリストを下に結合
    # Smartリストにある企業を優先（keep='first'）して、重複する法人番号を削除します
    merged_df = pd.concat([df_smart, df_recency], ignore_index=True)
    
    # 重複排除（法人番号が同じなら、Smartリストの情報を残す）
    final_df = merged_df.drop_duplicates(subset='corporate_number', keep='first')
    
    # --- 統計情報 ---
    total = len(final_df)
    smart_only = len(df_smart)
    added_new = total - smart_only
    
    print("-" * 40)
    print(f"【合体完了】")
    print(f"最終件数: {total:,} 社")
    print(f"  - Smartリスト由来: {smart_only:,} 社")
    print(f"  - Recencyリストから補充（純新規）: {added_new:,} 社")
    print("-" * 40)
    
    # 出力
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    final_df.to_csv(OUTPUT_FILE, index=False)
    print(f"保存完了: {OUTPUT_FILE}")
    print("★ このファイルをDBインポートに使用してください。")

if __name__ == "__main__":
    merge_lists()