import psycopg2
import os

# --- 設定 ---
CSV_PATH = 'out/merged_master_list.csv'

# DB接続設定
DB_CONFIG = {
    'host': '127.0.0.1',
    'port': '5432',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'Legatus2000/'
}

def import_data():
    conn = None
    try:
        print("1. DB接続中...")
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # --- 1.1. idカラムのNULLエラー対策 (UUIDデフォルト値設定) ---
        print("1.1. idカラムの自動生成設定(UUID)を適用中...")
        cur.execute("""
            -- UUID生成用の拡張機能を有効化
            CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

            DO $$
            BEGIN
                -- idカラムにデフォルト値がない場合、UUIDを自動生成するように設定
                -- 型変更(bigint化)をせず、文字列のまま自動採番を解決します
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'companies' AND column_name = 'id' 
                    AND column_default IS NOT NULL
                ) THEN
                    ALTER TABLE companies ALTER COLUMN id SET DEFAULT uuid_generate_v4()::text;
                END IF;
            END $$;
        """)
        conn.commit()

        # --- 1.2. 重複データのマージ ---
        print("1.2. 重複データ間の情報をマージ中...")
        cur.execute("""
            UPDATE companies target
            SET 
                name = COALESCE(NULLIF(target.name, ''), source.name),
                address = COALESCE(NULLIF(target.address, ''), source.address),
                representative_name = COALESCE(NULLIF(target.representative_name, ''), source.representative_name),
                company_url = COALESCE(NULLIF(target.company_url, ''), source.company_url),
                capital_stock = COALESCE(target.capital_stock, source.capital_stock),
                employee_count = COALESCE(target.employee_count, source.employee_count),
                founded_year = COALESCE(target.founded_year, source.founded_year)
            FROM companies source
            WHERE target.corporate_number = source.corporate_number
              AND target.id != source.id
              AND target.updated_at >= source.updated_at;
        """)
        conn.commit()

        # --- 1.3. 重複削除 ---
        print("1.3. 不要になった重複レコードを削除中...")
        cur.execute("""
            DELETE FROM companies
            WHERE id IN (
                SELECT id
                FROM (
                    SELECT id,
                    ROW_NUMBER() OVER (PARTITION BY corporate_number ORDER BY updated_at DESC) as rnum
                    FROM companies
                    WHERE corporate_number IS NOT NULL AND corporate_number != ''
                ) t
                WHERE t.rnum > 1
            );
        """)
        conn.commit()

        # --- 1.5. カラム追加 & ユニーク制約 ---
        print("1.5. カラム追加 & ユニーク制約の設定...")
        cur.execute("""
            ALTER TABLE companies ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE;
            ALTER TABLE companies ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW();
            ALTER TABLE companies ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();
            
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'unique_corporate_number') THEN
                    ALTER TABLE companies ADD CONSTRAINT unique_corporate_number UNIQUE (corporate_number);
                END IF;
            END $$;
        """)
        conn.commit()

        # --- A. ステージングテーブル作成 ---
        cur.execute("DROP TABLE IF EXISTS staging_active;")
        cur.execute("""
            CREATE TABLE staging_active (
                corporate_number TEXT,
                name TEXT,
                address TEXT,
                representative_name TEXT,
                capital_stock TEXT,
                employee_count TEXT,
                founded_year TEXT,
                company_url TEXT,
                update_date TEXT
            );
        """)
        conn.commit()

        # --- B. 高速ロード ---
        print(f"3. CSVロード開始: {CSV_PATH}")
        with open(CSV_PATH, 'r', encoding='utf-8') as f:
            next(f) 
            cur.copy_expert(sql="COPY staging_active FROM STDIN WITH CSV DELIMITER ',' QUOTE '\"' NULL ''", file=f)
        conn.commit()

        # --- C. 本番テーブルへの反映 ---
        print("4. companiesテーブルへのMerge(穴埋め)実行...")
        
        sql_merge = """
        INSERT INTO companies (
            corporate_number, name, address, representative_name, 
            capital_stock, employee_count, founded_year, 
            company_url, prefecture, legal_form, is_active, 
            created_at, updated_at
        )
        SELECT
            s.corporate_number,
            s.name,
            s.address,
            s.representative_name,
            NULLIF(regexp_replace(s.capital_stock, '[^0-9]', '', 'g'), '')::numeric,
            NULLIF(regexp_replace(s.employee_count, '[^0-9]', '', 'g'), '')::integer,
            NULLIF(regexp_replace(substring(s.founded_year, 1, 4), '[^0-9]', '', 'g'), '')::integer,
            s.company_url,
            substring(s.address, '^(.{2,3}?[都道府県])'),
            CASE 
                WHEN s.name LIKE '%株式会社%' THEN '株式会社'
                WHEN s.name LIKE '%有限会社%' THEN '有限会社'
                WHEN s.name LIKE '%合同会社%' THEN '合同会社'
                WHEN s.name LIKE '%合名会社%' THEN '合名会社'
                WHEN s.name LIKE '%合資会社%' THEN '合資会社'
                ELSE 'その他'
            END,
            true,
            NOW(),
            NOW()
        FROM staging_active s
        ON CONFLICT (corporate_number) 
        DO UPDATE SET
            name = COALESCE(NULLIF(companies.name, ''), EXCLUDED.name),
            address = COALESCE(NULLIF(companies.address, ''), EXCLUDED.address),
            representative_name = COALESCE(NULLIF(companies.representative_name, ''), EXCLUDED.representative_name),
            capital_stock = COALESCE(companies.capital_stock, EXCLUDED.capital_stock),
            employee_count = COALESCE(companies.employee_count, EXCLUDED.employee_count),
            founded_year = COALESCE(companies.founded_year, EXCLUDED.founded_year),
            company_url = COALESCE(NULLIF(companies.company_url, ''), EXCLUDED.company_url),
            prefecture = COALESCE(NULLIF(companies.prefecture, ''), EXCLUDED.prefecture),
            legal_form = COALESCE(NULLIF(companies.legal_form, ''), EXCLUDED.legal_form),
            is_active = true,
            updated_at = NOW();
        """
        
        cur.execute(sql_merge)
        print("SUCCESS: データの穴埋め更新が完了しました！")

        cur.execute("DROP TABLE staging_active;")
        conn.commit()

    except Exception as e:
        print(f"ERROR: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    import_data()