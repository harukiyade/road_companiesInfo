-- 246個の全カラムを対象としたデータ充足率調査用SQL
-- DBeaver等のDBツールで実行し、結果をCSVで保存してください。

DO $$
DECLARE
    col_record RECORD;
    sql_text TEXT := 'SELECT count(*) as total_rows';
BEGIN
    -- companiesテーブルの全カラムをループで回してSQLを組み立てる
    FOR col_record IN 
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'companies'
    LOOP
        sql_text := sql_text || ', COUNT(NULLIF(CAST(' || quote_ident(col_record.column_name) || ' AS TEXT), '''')) ' ||
                    'FILTER (WHERE CAST(' || quote_ident(col_record.column_name) || ' AS TEXT) NOT IN (''[]'', ''{{}}'')) ' ||
                    'AS ' || quote_ident(col_record.column_name || '_count');
    END LOOP;
    
    sql_text := sql_text || ' FROM companies';
    
    -- 組み立てたSQLを表示（メッセージログに出力されるので、それをコピーして実行できます）
    RAISE NOTICE '%', sql_text;
END $$;
