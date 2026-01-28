#!/usr/bin/env python3
"""
企業検索API - Cloud SQL (PostgreSQL) 版

Firestore版の検索APIをPostgreSQL版に書き換え。
SQLAlchemyを使用してSQLインジェクションを防止。
複合検索条件に対応。
"""

import os
import sys
from typing import Optional, List, Dict, Any
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Integer, BigInteger, Text, Date, Numeric, ARRAY, JSONB, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.dialects.postgresql import ARRAY as PG_ARRAY
from sqlalchemy.sql import and_, or_
import json

# ログ設定
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 環境変数から設定を取得
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'companies_db')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'password')

# データベース接続URL
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# SQLAlchemy設定
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# ============================================
# SQLAlchemyモデル定義
# ============================================
class Company(Base):
    """企業テーブルのORMモデル"""
    __tablename__ = 'companies'
    
    # Primary Key
    id = Column(String(255), primary_key=True)
    
    # 基本情報
    company_id = Column(String(255))
    name = Column(String(500), nullable=False)
    name_en = Column(String(500))
    kana = Column(String(500))
    corporate_number = Column(String(13))
    corporation_type = Column(String(100))
    nikkei_code = Column(String(10))
    badges = Column(PG_ARRAY(Text))
    tags = Column(PG_ARRAY(Text))
    created_at = Column(Date)
    updated_at = Column(Date)
    update_date = Column(Date)
    update_count = Column(Integer, default=0)
    change_count = Column(Integer, default=0)
    qualification_grade = Column(String(100))
    
    # 所在地情報
    prefecture = Column(String(50))
    address = Column(Text)
    headquarters_address = Column(Text)
    postal_code = Column(String(10))
    location = Column(Text)
    department_location = Column(Text)
    
    # 連絡先情報
    phone_number = Column(String(50))
    contact_phone_number = Column(String(50))
    fax = Column(String(50))
    email = Column(String(255))
    company_url = Column(Text)
    contact_form_url = Column(Text)
    
    # 代表者情報
    representative_name = Column(String(200))
    representative_kana = Column(String(200))
    representative_title = Column(String(100))
    representative_birth_date = Column(Date)
    representative_phone = Column(String(50))
    representative_postal_code = Column(String(10))
    representative_home_address = Column(Text)
    representative_registered_address = Column(Text)
    representative_alma_mater = Column(String(200))
    executives = Column(JSONB)
    
    # 役員情報（主要なもののみ）
    executive_name1 = Column(String(200))
    executive_position1 = Column(String(100))
    
    # 業種情報
    industry = Column(String(200))
    industry_large = Column(String(200))
    industry_middle = Column(String(200))
    industry_small = Column(String(200))
    industry_detail = Column(String(200))
    industries = Column(PG_ARRAY(Text))  # 業種タグ配列
    industry_categories = Column(PG_ARRAY(Text))
    business_descriptions = Column(Text)
    business_items = Column(PG_ARRAY(Text))
    business_summary = Column(Text)
    specialties = Column(PG_ARRAY(Text))
    demand_products = Column(PG_ARRAY(Text))
    special_note = Column(Text)
    
    # 財務情報
    capital_stock = Column(BigInteger)
    revenue = Column(BigInteger)
    latest_revenue = Column(BigInteger)
    latest_profit = Column(BigInteger)
    revenue_from_statements = Column(BigInteger)
    operating_income = Column(BigInteger)
    total_assets = Column(BigInteger)
    total_liabilities = Column(BigInteger)
    net_assets = Column(BigInteger)
    issued_shares = Column(BigInteger)
    financials = Column(JSONB)
    listing = Column(String(100))  # 上場区分
    market_segment = Column(String(100))
    latest_fiscal_year_month = Column(String(10))
    fiscal_month = Column(Integer)
    
    # 企業規模・組織
    employee_count = Column(Integer)
    employee_number = Column(String(50))
    factory_count = Column(Integer)
    office_count = Column(Integer)
    store_count = Column(Integer)
    average_age = Column(Numeric(5, 2))
    average_years_of_service = Column(Numeric(5, 2))
    average_overtime_hours = Column(Numeric(5, 2))
    average_paid_leave = Column(Numeric(5, 2))
    female_executive_ratio = Column(Numeric(5, 2))
    
    # 設立・沿革
    established = Column(Date)
    date_of_establishment = Column(Date)
    founding = Column(Date)
    founding_year = Column(Integer)
    acquisition = Column(Text)
    
    # 取引先・関係会社
    clients = Column(JSONB)
    suppliers = Column(JSONB)
    subsidiaries = Column(JSONB)
    affiliations = Column(JSONB)
    shareholders = Column(JSONB)
    banks = Column(JSONB)
    bank_corporate_number = Column(String(13))
    
    # 企業説明
    overview = Column(Text)
    company_description = Column(Text)
    business_descriptions_text = Column(Text)
    sales_notes = Column(Text)
    
    # SNS・外部リンク
    urls = Column(PG_ARRAY(Text))
    profile_url = Column(Text)
    external_detail_url = Column(Text)
    facebook = Column(Text)
    linkedin = Column(Text)
    wantedly = Column(Text)
    youtrust = Column(Text)
    meta_keywords = Column(PG_ARRAY(Text))


# ============================================
# 検索パラメータ型定義
# ============================================
class SearchParams:
    """検索パラメータ"""
    def __init__(
        self,
        prefecture: Optional[str] = None,
        revenue_min: Optional[int] = None,
        revenue_max: Optional[int] = None,
        industry_tags: Optional[List[str]] = None,
        listing: Optional[str] = None,
        capital_stock_min: Optional[int] = None,
        employee_count_min: Optional[int] = None,
        industry: Optional[str] = None,
        industry_large: Optional[str] = None,
        industry_middle: Optional[str] = None,
        name: Optional[str] = None,
        corporate_number: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ):
        self.prefecture = prefecture
        self.revenue_min = revenue_min
        self.revenue_max = revenue_max
        self.industry_tags = industry_tags or []
        self.listing = listing
        self.capital_stock_min = capital_stock_min
        self.employee_count_min = employee_count_min
        self.industry = industry
        self.industry_large = industry_large
        self.industry_middle = industry_middle
        self.name = name
        self.corporate_number = corporate_number
        self.limit = limit
        self.offset = offset


# ============================================
# 検索ロジック
# ============================================
def build_search_query(session: Session, params: SearchParams):
    """検索クエリを構築"""
    query = session.query(Company)
    conditions = []
    
    # 都道府県
    if params.prefecture:
        conditions.append(Company.prefecture == params.prefecture)
    
    # 売上高（最小値）
    if params.revenue_min is not None:
        conditions.append(Company.revenue >= params.revenue_min)
    
    # 売上高（最大値）
    if params.revenue_max is not None:
        conditions.append(Company.revenue <= params.revenue_max)
    
    # 業種タグ（配列検索）
    if params.industry_tags:
        # 複数のタグのうち、いずれかが含まれている場合
        # PostgreSQLの配列検索: tag = ANY(industries)
        from sqlalchemy import text
        tag_conditions = []
        for tag in params.industry_tags:
            # SQLAlchemyで配列検索: パラメータバインディングを使用してSQLインジェクションを防止
            # companies.industries はテーブル名.カラム名の形式で指定
            tag_conditions.append(
                text(":tag = ANY(companies.industries)").bindparams(tag=tag)
            )
        if tag_conditions:
            conditions.append(or_(*tag_conditions))
    
    # 上場区分
    if params.listing:
        conditions.append(Company.listing == params.listing)
    
    # 資本金（最小値）
    if params.capital_stock_min is not None:
        conditions.append(Company.capital_stock >= params.capital_stock_min)
    
    # 従業員数（最小値）
    if params.employee_count_min is not None:
        conditions.append(Company.employee_count >= params.employee_count_min)
    
    # 業種
    if params.industry:
        conditions.append(Company.industry == params.industry)
    
    # 業種（大分類）
    if params.industry_large:
        conditions.append(Company.industry_large == params.industry_large)
    
    # 業種（中分類）
    if params.industry_middle:
        conditions.append(Company.industry_middle == params.industry_middle)
    
    # 企業名（部分一致）
    if params.name:
        conditions.append(Company.name.ilike(f'%{params.name}%'))
    
    # 法人番号
    if params.corporate_number:
        conditions.append(Company.corporate_number == params.corporate_number)
    
    # 全ての条件をANDで結合
    if conditions:
        query = query.filter(and_(*conditions))
    
    # ソート（デフォルト: 企業名）
    query = query.order_by(Company.name)
    
    return query


def search_companies(params: SearchParams) -> Dict[str, Any]:
    """企業を検索"""
    session = SessionLocal()
    
    try:
        # 検索クエリ構築
        query = build_search_query(session, params)
        
        # 総件数を取得
        total_count = query.count()
        
        # ページネーション
        query = query.limit(params.limit).offset(params.offset)
        
        # データ取得
        companies = query.all()
        
        # 結果をJSON形式に変換
        results = []
        for company in companies:
            company_dict = {
                'id': company.id,
                'name': company.name,
                'nameEn': company.name_en,
                'kana': company.kana,
                'corporateNumber': company.corporate_number,
                'prefecture': company.prefecture,
                'address': company.address,
                'headquartersAddress': company.headquarters_address,
                'phoneNumber': company.phone_number,
                'email': company.email,
                'companyUrl': company.company_url,
                'contactFormUrl': company.contact_form_url,
                'representativeName': company.representative_name,
                'industry': company.industry,
                'industryLarge': company.industry_large,
                'industryMiddle': company.industry_middle,
                'industrySmall': company.industry_small,
                'industries': company.industries if company.industries else [],
                'capitalStock': company.capital_stock,
                'revenue': company.revenue,
                'latestRevenue': company.latest_revenue,
                'latestProfit': company.latest_profit,
                'operatingIncome': company.operating_income,
                'totalAssets': company.total_assets,
                'netAssets': company.net_assets,
                'listing': company.listing,
                'marketSegment': company.market_segment,
                'employeeCount': company.employee_count,
                'established': company.established.isoformat() if company.established else None,
                'overview': company.overview,
                'companyDescription': company.company_description,
            }
            
            # JSONBフィールドの処理
            if company.executives:
                if isinstance(company.executives, str):
                    company_dict['executives'] = json.loads(company.executives)
                else:
                    company_dict['executives'] = company.executives
            
            if company.financials:
                if isinstance(company.financials, str):
                    company_dict['financials'] = json.loads(company.financials)
                else:
                    company_dict['financials'] = company.financials
            
            # relatedCompanies（subsidiaries + affiliations）を構築
            related_companies = {}
            if company.subsidiaries:
                if isinstance(company.subsidiaries, str):
                    subsidiaries = json.loads(company.subsidiaries)
                else:
                    subsidiaries = company.subsidiaries
                if isinstance(subsidiaries, dict):
                    related_companies.update(subsidiaries)
            
            if company.affiliations:
                if isinstance(company.affiliations, str):
                    affiliations = json.loads(company.affiliations)
                else:
                    affiliations = company.affiliations
                if isinstance(affiliations, dict):
                    related_companies.update(affiliations)
            
            company_dict['relatedCompanies'] = related_companies if related_companies else None
            
            results.append(company_dict)
        
        # レスポンス形式（Firestore版と互換性を保つ）
        return {
            'companies': results,
            'total': total_count,
            'limit': params.limit,
            'offset': params.offset,
            'hasMore': (params.offset + len(results)) < total_count
        }
    
    finally:
        session.close()


# ============================================
# APIエンドポイント（例: Flask/FastAPI用）
# ============================================
def handle_search_request(request_data: Dict[str, Any]) -> Dict[str, Any]:
    """APIリクエストを処理"""
    try:
        # パラメータをパース
        params = SearchParams(
            prefecture=request_data.get('prefecture'),
            revenue_min=request_data.get('revenueMin'),
            revenue_max=request_data.get('revenueMax'),
            industry_tags=request_data.get('industryTags', []),
            listing=request_data.get('listing'),
            capital_stock_min=request_data.get('capitalStockMin'),
            employee_count_min=request_data.get('employeeCountMin'),
            industry=request_data.get('industry'),
            industry_large=request_data.get('industryLarge'),
            industry_middle=request_data.get('industryMiddle'),
            name=request_data.get('name'),
            corporate_number=request_data.get('corporateNumber'),
            limit=request_data.get('limit', 50),
            offset=request_data.get('offset', 0)
        )
        
        # 検索実行
        result = search_companies(params)
        
        return {
            'success': True,
            'data': result
        }
    
    except Exception as e:
        logger.error(f"検索エラー: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e)
        }


# ============================================
# テスト用メイン関数
# ============================================
if __name__ == '__main__':
    # テスト用の検索パラメータ
    test_params = SearchParams(
        prefecture='東京都',
        revenue_min=100000000,
        industry_tags=['IT', '通信'],
        listing='プライム',
        limit=10,
        offset=0
    )
    
    print("検索を実行中...")
    result = search_companies(test_params)
    
    print(f"\n検索結果:")
    print(f"  総件数: {result['total']}")
    print(f"  取得件数: {len(result['companies'])}")
    print(f"\n最初の3件:")
    for i, company in enumerate(result['companies'][:3], 1):
        print(f"  {i}. {company['name']} ({company['prefecture']}) - 売上: {company['revenue']}")
