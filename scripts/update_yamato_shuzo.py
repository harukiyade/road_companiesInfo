#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import psycopg2

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "34.84.189.233"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "Legatus2000/"),
        dbname=os.getenv("POSTGRES_DB", "postgres"),
        sslmode="disable"
    )

def main():
    conn = get_db_connection()
    cur = conn.cursor()

    company_id = "1766038915360006975"

    # 役員（取締役）のカラム名が環境によって異なる可能性があるため、自動探索
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'companies'")
    columns = [row[0] for row in cur.fetchall()]
    
    director_col = None
    for col in ['directors', 'board_members', 'officers']:
        if col in columns:
            director_col = col
            break

    # UPDATE文の構築
    sql = """
        UPDATE companies 
        SET 
            capital = %s,
            latest_revenue = %s,
            latest_profit = %s,
            established = %s,
            representative_birthday = NULL,
            representative_address = NULL
    """
    
    # 変更する値（資本金, 売上, 利益, 設立日）
    params = [5000000, 404000000, 21000, '2014-04-01']

    # 役員カラムが存在すればNULLで上書き
    if director_col:
        sql += f", {director_col} = NULL"

    # IDで対象を絞り込み
    sql += " WHERE id = %s"
    params.append(company_id)

    try:
        cur.execute(sql, params)
        if cur.rowcount > 0:
            print(f"Success: Updated company ID {company_id} perfectly.")
        else:
            print(f"Warning: Company ID {company_id} not found in DB.")
        
        conn.commit()
    except Exception as e:
        print(f"Error occurred: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()