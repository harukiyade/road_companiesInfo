import os

# --- 設定 ---
OUTPUT_SQL = 'sqlResultFile/check_all_columns_usage.sql'

def main():
    # 出力先フォルダの作成
    os.makedirs(os.path.dirname(OUTPUT_SQL), exist_ok=True)

    with open(OUTPUT_SQL, 'w', encoding='utf-8') as f:
        f.write("-- 246個の全カラムを対象としたデータ充足率調査用SQL\n")
        f.write("-- DBeaver等のDBツールで実行し、結果をCSVで保存してください。\n\n")
        
        # PostgreSQLのシステムカタログからカラム名を取得してカウントするSQL
        f.write("DO $$\n")
        f.write("DECLARE\n")
        f.write("    col_record RECORD;\n")
        f.write("    sql_text TEXT := 'SELECT count(*) as total_rows';\n")
        f.write("BEGIN\n")
        f.write("    -- companiesテーブルの全カラムをループで回してSQLを組み立てる\n")
        f.write("    FOR col_record IN \n")
        f.write("        SELECT column_name \n")
        f.write("        FROM information_schema.columns \n")
        f.write("        WHERE table_name = 'companies'\n")
        f.write("    LOOP\n")
        f.write("        sql_text := sql_text || ', COUNT(NULLIF(CAST(' || quote_ident(col_record.column_name) || ' AS TEXT), '''')) ' ||\n")
        f.write("                    'FILTER (WHERE CAST(' || quote_ident(col_record.column_name) || ' AS TEXT) NOT IN (''[]'', ''{{}}'')) ' ||\n")
        f.write("                    'AS ' || quote_ident(col_record.column_name || '_count');\n")
        f.write("    END LOOP;\n")
        f.write("    \n")
        f.write("    sql_text := sql_text || ' FROM companies';\n")
        f.write("    \n")
        f.write("    -- 組み立てたSQLを表示（メッセージログに出力されるので、それをコピーして実行できます）\n")
        f.write("    RAISE NOTICE '%', sql_text;\n")
        f.write("END $$;\n")

    print(f"独立版の調査用メタSQLを生成しました: {OUTPUT_SQL}")
    print("※このSQLをDBツールで実行すると、メッセージ（出力）欄に実際の調査用SQLが表示されます。")

if __name__ == "__main__":
    main()