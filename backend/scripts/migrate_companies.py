#!/usr/bin/env python3
"""
Firestore companies_new コレクションから Cloud SQL (PostgreSQL) へのデータ移行スクリプト

機能:
- ページネーション処理（1000件ずつ）
- 再開機能（最後に処理したIDを記録）
- バッチINSERT（executemany使用）
- エラーハンドリングとログ出力
"""

import os
import sys
import json
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, firestore
import psycopg2
from psycopg2.extras import execute_batch
from psycopg2 import sql

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migrate_companies.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# 環境変数から設定を取得
FIREBASE_CREDENTIALS_PATH = os.getenv('FIREBASE_CREDENTIALS_PATH', 'serviceAccountKey.json')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'companies_db')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'password')

# 再開用ファイルパス
RESUME_FILE = 'migrate_companies_resume.txt'
BATCH_SIZE = 1000  # 1回のクエリで取得する件数
INSERT_BATCH_SIZE = 500  # 1回のINSERTで処理する件数


def init_firebase():
    """Firebase Admin SDKを初期化"""
    try:
        if not firebase_admin._apps:
            cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
            firebase_admin.initialize_app(cred)
        return firestore.client()
    except Exception as e:
        logger.error(f"Firebase初期化エラー: {e}")
        raise


def init_postgres():
    """PostgreSQL接続を初期化"""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        return conn
    except Exception as e:
        logger.error(f"PostgreSQL接続エラー: {e}")
        raise


def load_resume_point() -> Optional[str]:
    """再開ポイントを読み込む"""
    if os.path.exists(RESUME_FILE):
        with open(RESUME_FILE, 'r') as f:
            last_id = f.read().strip()
            if last_id:
                logger.info(f"再開ポイントを読み込み: {last_id}")
                return last_id
    return None


def save_resume_point(last_id: str):
    """再開ポイントを保存"""
    with open(RESUME_FILE, 'w') as f:
        f.write(last_id)
    logger.debug(f"再開ポイントを保存: {last_id}")


def convert_timestamp(timestamp) -> Optional[datetime]:
    """Firestore TimestampをPython datetimeに変換"""
    if timestamp is None:
        return None
    if hasattr(timestamp, 'timestamp'):
        return timestamp
    return None


def convert_value(value: Any) -> Any:
    """Firestoreの値をPostgreSQL用に変換"""
    if value is None:
        return None
    
    # Timestamp変換
    if hasattr(value, 'timestamp'):
        return convert_timestamp(value)
    
    # リストはそのまま（配列として扱う）
    if isinstance(value, list):
        return value
    
    # 辞書はJSONBとして扱う
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, default=str)
    
    return value


def prepare_company_data(doc_id: str, doc_data: Dict[str, Any]) -> Dict[str, Any]:
    """FirestoreドキュメントをPostgreSQL用のデータ形式に変換"""
    # フィールド名をスネークケースに変換するマッピング
    field_mapping = {
        'companyId': 'company_id',
        'nameEn': 'name_en',
        'corporateNumber': 'corporate_number',
        'corporationType': 'corporation_type',
        'nikkeiCode': 'nikkei_code',
        'createdAt': 'created_at',
        'updatedAt': 'updated_at',
        'updateDate': 'update_date',
        'updateCount': 'update_count',
        'changeCount': 'change_count',
        'qualificationGrade': 'qualification_grade',
        'headquartersAddress': 'headquarters_address',
        'postalCode': 'postal_code',
        'departmentLocation': 'department_location',
        'phoneNumber': 'phone_number',
        'contactPhoneNumber': 'contact_phone_number',
        'companyUrl': 'company_url',
        'contactFormUrl': 'contact_form_url',
        'representativeName': 'representative_name',
        'representativeKana': 'representative_kana',
        'representativeTitle': 'representative_title',
        'representativeBirthDate': 'representative_birth_date',
        'representativePhone': 'representative_phone',
        'representativePostalCode': 'representative_postal_code',
        'representativeHomeAddress': 'representative_home_address',
        'representativeRegisteredAddress': 'representative_registered_address',
        'representativeAlmaMater': 'representative_alma_mater',
        'executiveName1': 'executive_name1',
        'executiveName2': 'executive_name2',
        'executiveName3': 'executive_name3',
        'executiveName4': 'executive_name4',
        'executiveName5': 'executive_name5',
        'executiveName6': 'executive_name6',
        'executiveName7': 'executive_name7',
        'executiveName8': 'executive_name8',
        'executiveName9': 'executive_name9',
        'executiveName10': 'executive_name10',
        'executivePosition1': 'executive_position1',
        'executivePosition2': 'executive_position2',
        'executivePosition3': 'executive_position3',
        'executivePosition4': 'executive_position4',
        'executivePosition5': 'executive_position5',
        'executivePosition6': 'executive_position6',
        'executivePosition7': 'executive_position7',
        'executivePosition8': 'executive_position8',
        'executivePosition9': 'executive_position9',
        'executivePosition10': 'executive_position10',
        'industryLarge': 'industry_large',
        'industryMiddle': 'industry_middle',
        'industrySmall': 'industry_small',
        'industryDetail': 'industry_detail',
        'industryCategories': 'industry_categories',
        'businessDescriptions': 'business_descriptions',
        'businessItems': 'business_items',
        'businessSummary': 'business_summary',
        'demandProducts': 'demand_products',
        'specialNote': 'special_note',
        'capitalStock': 'capital_stock',
        'latestRevenue': 'latest_revenue',
        'latestProfit': 'latest_profit',
        'revenueFromStatements': 'revenue_from_statements',
        'operatingIncome': 'operating_income',
        'totalAssets': 'total_assets',
        'totalLiabilities': 'total_liabilities',
        'netAssets': 'net_assets',
        'issuedShares': 'issued_shares',
        'marketSegment': 'market_segment',
        'latestFiscalYearMonth': 'latest_fiscal_year_month',
        'fiscalMonth': 'fiscal_month',
        'fiscalMonth1': 'fiscal_month1',
        'fiscalMonth2': 'fiscal_month2',
        'fiscalMonth3': 'fiscal_month3',
        'fiscalMonth4': 'fiscal_month4',
        'fiscalMonth5': 'fiscal_month5',
        'employeeCount': 'employee_count',
        'employeeNumber': 'employee_number',
        'factoryCount': 'factory_count',
        'officeCount': 'office_count',
        'storeCount': 'store_count',
        'averageAge': 'average_age',
        'averageYearsOfService': 'average_years_of_service',
        'averageOvertimeHours': 'average_overtime_hours',
        'averagePaidLeave': 'average_paid_leave',
        'femaleExecutiveRatio': 'female_executive_ratio',
        'dateOfEstablishment': 'date_of_establishment',
        'foundingYear': 'founding_year',
        'bankCorporateNumber': 'bank_corporate_number',
        'departmentName1': 'department_name1',
        'departmentName2': 'department_name2',
        'departmentName3': 'department_name3',
        'departmentName4': 'department_name4',
        'departmentName5': 'department_name5',
        'departmentName6': 'department_name6',
        'departmentName7': 'department_name7',
        'departmentAddress1': 'department_address1',
        'departmentAddress2': 'department_address2',
        'departmentAddress3': 'department_address3',
        'departmentAddress4': 'department_address4',
        'departmentAddress5': 'department_address5',
        'departmentAddress6': 'department_address6',
        'departmentAddress7': 'department_address7',
        'departmentPhone1': 'department_phone1',
        'departmentPhone2': 'department_phone2',
        'departmentPhone3': 'department_phone3',
        'departmentPhone4': 'department_phone4',
        'departmentPhone5': 'department_phone5',
        'departmentPhone6': 'department_phone6',
        'departmentPhone7': 'department_phone7',
        'companyDescription': 'company_description',
        'businessDescriptions': 'business_descriptions_text',
        'salesNotes': 'sales_notes',
        'profileUrl': 'profile_url',
        'externalDetailUrl': 'external_detail_url',
        'metaKeywords': 'meta_keywords',
    }
    
    # 基本データ構造
    data = {'id': doc_id}
    
    # 各フィールドを変換
    for key, value in doc_data.items():
        # フィールド名をスネークケースに変換
        pg_key = field_mapping.get(key, key.lower())
        
        # 値を変換
        converted_value = convert_value(value)
        data[pg_key] = converted_value
    
    return data


def get_insert_columns() -> List[str]:
    """INSERT文で使用するカラムリストを取得"""
    return [
        'id', 'company_id', 'name', 'name_en', 'kana', 'corporate_number',
        'corporation_type', 'nikkei_code', 'badges', 'tags', 'created_at',
        'updated_at', 'update_date', 'update_count', 'change_count',
        'qualification_grade', 'prefecture', 'address', 'headquarters_address',
        'postal_code', 'location', 'department_location', 'phone_number',
        'contact_phone_number', 'fax', 'email', 'company_url', 'contact_form_url',
        'representative_name', 'representative_kana', 'representative_title',
        'representative_birth_date', 'representative_phone', 'representative_postal_code',
        'representative_home_address', 'representative_registered_address',
        'representative_alma_mater', 'executives', 'executive_name1', 'executive_name2',
        'executive_name3', 'executive_name4', 'executive_name5', 'executive_name6',
        'executive_name7', 'executive_name8', 'executive_name9', 'executive_name10',
        'executive_position1', 'executive_position2', 'executive_position3',
        'executive_position4', 'executive_position5', 'executive_position6',
        'executive_position7', 'executive_position8', 'executive_position9',
        'executive_position10', 'industry', 'industry_large', 'industry_middle',
        'industry_small', 'industry_detail', 'industries', 'industry_categories',
        'business_descriptions', 'business_items', 'business_summary', 'specialties',
        'demand_products', 'special_note', 'capital_stock', 'revenue', 'latest_revenue',
        'latest_profit', 'revenue_from_statements', 'operating_income', 'total_assets',
        'total_liabilities', 'net_assets', 'issued_shares', 'financials', 'listing',
        'market_segment', 'latest_fiscal_year_month', 'fiscal_month', 'fiscal_month1',
        'fiscal_month2', 'fiscal_month3', 'fiscal_month4', 'fiscal_month5',
        'revenue1', 'revenue2', 'revenue3', 'revenue4', 'revenue5',
        'profit1', 'profit2', 'profit3', 'profit4', 'profit5',
        'employee_count', 'employee_number', 'factory_count', 'office_count',
        'store_count', 'average_age', 'average_years_of_service',
        'average_overtime_hours', 'average_paid_leave', 'female_executive_ratio',
        'established', 'date_of_establishment', 'founding', 'founding_year',
        'acquisition', 'clients', 'suppliers', 'subsidiaries', 'affiliations',
        'shareholders', 'banks', 'bank_corporate_number', 'department_name1',
        'department_name2', 'department_name3', 'department_name4', 'department_name5',
        'department_name6', 'department_name7', 'department_address1',
        'department_address2', 'department_address3', 'department_address4',
        'department_address5', 'department_address6', 'department_address7',
        'department_phone1', 'department_phone2', 'department_phone3',
        'department_phone4', 'department_phone5', 'department_phone6',
        'department_phone7', 'overview', 'company_description',
        'business_descriptions_text', 'sales_notes', 'urls', 'profile_url',
        'external_detail_url', 'facebook', 'linkedin', 'wantedly', 'youtrust',
        'meta_keywords'
    ]


def insert_companies_batch(conn, companies_data: List[Dict[str, Any]]):
    """バッチで企業データをINSERT"""
    if not companies_data:
        return
    
    columns = get_insert_columns()
    placeholders = ', '.join(['%s'] * len(columns))
    column_names = ', '.join(columns)
    
    # ON CONFLICTで更新（UPSERT）
    update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'id'])
    
    query = f"""
        INSERT INTO companies ({column_names})
        VALUES ({placeholders})
        ON CONFLICT (id) DO UPDATE SET {update_clause}
    """
    
    # データをタプルのリストに変換
    values_list = []
    for company in companies_data:
        values = tuple(company.get(col) for col in columns)
        values_list.append(values)
    
    try:
        with conn.cursor() as cur:
            execute_batch(cur, query, values_list, page_size=INSERT_BATCH_SIZE)
        conn.commit()
        logger.info(f"  → {len(companies_data)}件をINSERT完了")
    except Exception as e:
        conn.rollback()
        logger.error(f"  → INSERTエラー: {e}")
        raise


def migrate_companies():
    """メインの移行処理"""
    logger.info("=" * 60)
    logger.info("Firestore → PostgreSQL データ移行を開始")
    logger.info("=" * 60)
    
    # 初期化
    db = init_firebase()
    pg_conn = init_postgres()
    
    # 再開ポイントを読み込み
    resume_from_id = load_resume_point()
    
    try:
        # コレクション参照
        companies_ref = db.collection('companies_new')
        
        # クエリ構築
        query = companies_ref.order_by(firestore.FieldPath.documentId()).limit(BATCH_SIZE)
        
        # 再開ポイントがある場合はそこから開始
        if resume_from_id:
            query = query.start_after([resume_from_id])
            logger.info(f"再開ポイントから処理を開始: {resume_from_id}")
        
        total_processed = 0
        total_inserted = 0
        batch_count = 0
        
        while True:
            # Firestoreからデータ取得
            logger.info(f"\nバッチ {batch_count + 1}: Firestoreからデータ取得中...")
            snapshot = query.get()
            
            if not snapshot:
                logger.info("データがなくなりました。移行完了。")
                break
            
            logger.info(f"  → {len(snapshot)}件取得")
            
            # データ変換とバッチ準備
            companies_batch = []
            last_doc_id = None
            
            for doc in snapshot:
                doc_id = doc.id
                doc_data = doc.to_dict()
                
                # データ変換
                company_data = prepare_company_data(doc_id, doc_data)
                companies_batch.append(company_data)
                
                last_doc_id = doc_id
                total_processed += 1
            
            # PostgreSQLにINSERT
            if companies_batch:
                logger.info(f"  → PostgreSQLにINSERT中...")
                insert_companies_batch(pg_conn, companies_batch)
                total_inserted += len(companies_batch)
            
            # 再開ポイントを保存
            if last_doc_id:
                save_resume_point(last_doc_id)
            
            # 次のページのクエリを準備
            if len(snapshot) < BATCH_SIZE:
                logger.info("最後のバッチを処理しました。")
                break
            
            # カーソルを更新
            last_doc = snapshot[-1]
            query = companies_ref.order_by(firestore.FieldPath.documentId()).limit(BATCH_SIZE).start_after([last_doc])
            batch_count += 1
            
            # 進捗表示
            if batch_count % 10 == 0:
                logger.info(f"\n進捗: {total_processed}件処理済み, {total_inserted}件INSERT完了")
        
        logger.info("\n" + "=" * 60)
        logger.info(f"移行完了!")
        logger.info(f"  処理件数: {total_processed}件")
        logger.info(f"  INSERT件数: {total_inserted}件")
        logger.info("=" * 60)
        
        # 再開ファイルを削除（正常終了時）
        if os.path.exists(RESUME_FILE):
            os.remove(RESUME_FILE)
            logger.info("再開ファイルを削除しました。")
    
    except KeyboardInterrupt:
        logger.warning("\n処理が中断されました。再開ポイントを保存しています...")
        if last_doc_id:
            save_resume_point(last_doc_id)
        raise
    
    except Exception as e:
        logger.error(f"移行エラー: {e}", exc_info=True)
        if last_doc_id:
            save_resume_point(last_doc_id)
        raise
    
    finally:
        pg_conn.close()
        logger.info("PostgreSQL接続を閉じました。")


if __name__ == '__main__':
    migrate_companies()
