#!/usr/bin/env python3
"""
富山技研興業 専用：決算書PDF → マテリアルExcel 抽出・転記スクリプト

- Step1: LLM Vision (Gemini) でPDFから1円単位の数値を抽出
- Step2: 設計書に基づく静的マッピングでテンプレートに値のみ書き込み（数式・表示形式は変更しない）

使い方:
  # 本番（3つのPDFをGeminiで解析 → Excel出力）
  export GEMINI_API_KEY=your_key
  python scripts/toyama_giken_material_fill.py

  # 転記のみ（抽出済みJSONからExcel生成。API不要）
  python scripts/toyama_giken_material_fill.py --from-json data/output/toyama_giken_extracted_sample.json

出力:
  - data/output/【マテリアル】_富山技研興業.xlsx
  - API使用時は data/output/toyama_giken_extracted.json も保存

セル番地の調整: 本ファイル先頭の PL_MAPPING / BS_*_MAPPING 定数を編集してください。
"""

import json
import os
import re
import sys
from pathlib import Path

# プロジェクトルートをパスに追加
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# -----------------------------------------------------------------------------
# 設定・パス（必要に応じて変更）
# -----------------------------------------------------------------------------
TEMPLATE_PATH = REPO_ROOT / "data/template/【マテリアル】_デビス㈱.xlsx"
OUTPUT_DIR = REPO_ROOT / "data/output"
PDFS = [
    (REPO_ROOT / "data/template/kessansho/１９：決算書一式（令和３年度）富山技研興業.pdf", "令和3年度"),
    (REPO_ROOT / "data/template/kessansho/２０：決算書一式（令和４年度）富山技研興業.pdf", "令和4年度"),
    (REPO_ROOT / "data/template/kessansho/２１：決算書一式（令和５年度）富山技研興業.pdf", "令和5年度"),
]
OUTPUT_EXCEL_NAME = "【マテリアル】_富山技研興業.xlsx"

# 基本情報（転記用固定値；PDFから取得できない場合のフォールバック）
COMPANY_NAME = "富山技研興業株式会社"
# 各年度の決算末日（ヘッダー年月用）
FISCAL_END_R3 = "2022-03-31"
FISCAL_END_R4 = "2023-03-31"
FISCAL_END_R5 = "2025-03-31"

# -----------------------------------------------------------------------------
# セルマッピング定数（設計書に基づく。ズレがあればここだけ修正する）
# openpyxl は 1-based の row, column を使用。売上高=E8、現金預金=F9 等、設計図通り。
# -----------------------------------------------------------------------------

# PL: 令和3→E(5), 令和4→G(7), 令和5→I(9)
PL_COL_R3, PL_COL_R4, PL_COL_R5 = 5, 7, 9
# BS: 令和3→F(6), 令和4→G(7), 令和5→H(8)
BS_COL_R3, BS_COL_R4, BS_COL_R5 = 6, 7, 8

# (JSONキー, シート名, 行, 令和3の列, 令和4の列, 令和5の列)
PL_MAPPING = [
    ("売上高", "PL", 8, PL_COL_R3, PL_COL_R4, PL_COL_R5),   # E8,G8,I8
    ("期首商品棚卸高", "PL", 10, PL_COL_R3, PL_COL_R4, PL_COL_R5),
    ("仕入高", "PL", 11, PL_COL_R3, PL_COL_R4, PL_COL_R5),
    ("期末商品棚卸高", "PL", 12, PL_COL_R3, PL_COL_R4, PL_COL_R5),
    ("受取利息", "PL", 17, PL_COL_R3, PL_COL_R4, PL_COL_R5),
    ("為替差益", "PL", 18, PL_COL_R3, PL_COL_R4, PL_COL_R5),
    ("雑収入", "PL", 19, PL_COL_R3, PL_COL_R4, PL_COL_R5),
    ("支払利息", "PL", 21, PL_COL_R3, PL_COL_R4, PL_COL_R5),
    ("雑損失", "PL", 22, PL_COL_R3, PL_COL_R4, PL_COL_R5),
    ("固定資産売却益", "PL", 25, PL_COL_R3, PL_COL_R4, PL_COL_R5),
    ("負債調整勘定戻入", "PL", 26, PL_COL_R3, PL_COL_R4, PL_COL_R5),
    ("固定資産除却損", "PL", 28, PL_COL_R3, PL_COL_R4, PL_COL_R5),
    ("投資有価証券評価損", "PL", 29, PL_COL_R3, PL_COL_R4, PL_COL_R5),
    ("仮払金評価損", "PL", 30, PL_COL_R3, PL_COL_R4, PL_COL_R5),
    ("在庫評価引当金繰入", "PL", 31, PL_COL_R3, PL_COL_R4, PL_COL_R5),
    ("子会社清算損", "PL", 32, PL_COL_R3, PL_COL_R4, PL_COL_R5),
    ("不良債権", "PL", 33, PL_COL_R3, PL_COL_R4, PL_COL_R5),
    ("法人税住民税及び事業税", "PL", 36, PL_COL_R3, PL_COL_R4, PL_COL_R5),
]

BS_ASSET_MAPPING = [
    ("現金預金", "BS", 9, BS_COL_R3, BS_COL_R4, BS_COL_R5),   # F9,G9,H9
    ("売掛金", "BS", 10, BS_COL_R3, BS_COL_R4, BS_COL_R5),
    ("棚卸資産", "BS", 11, BS_COL_R3, BS_COL_R4, BS_COL_R5),
    ("貸付金", "BS", 12, BS_COL_R3, BS_COL_R4, BS_COL_R5),
    ("前払費用", "BS", 13, BS_COL_R3, BS_COL_R4, BS_COL_R5),
    ("仮払金", "BS", 14, BS_COL_R3, BS_COL_R4, BS_COL_R5),
    ("仮払税金", "BS", 15, BS_COL_R3, BS_COL_R4, BS_COL_R5),
    ("建物建物附属設備", "BS", 30, BS_COL_R3, BS_COL_R4, BS_COL_R5),
    ("機械装置", "BS", 31, BS_COL_R3, BS_COL_R4, BS_COL_R5),
    ("車両運搬具", "BS", 32, BS_COL_R3, BS_COL_R4, BS_COL_R5),
    ("器具備品", "BS", 33, BS_COL_R3, BS_COL_R4, BS_COL_R5),
    ("土地", "BS", 34, BS_COL_R3, BS_COL_R4, BS_COL_R5),
    ("電話加入権", "BS", 42, BS_COL_R3, BS_COL_R4, BS_COL_R5),
    ("ソフトウェア", "BS", 43, BS_COL_R3, BS_COL_R4, BS_COL_R5),
    ("のれん", "BS", 44, BS_COL_R3, None, None),  # 令和3のみセルあり
    ("投資有価証券", "BS", 49, BS_COL_R3, BS_COL_R4, BS_COL_R5),
    ("保証金", "BS", 50, BS_COL_R3, BS_COL_R4, BS_COL_R5),
    ("長期前払費用", "BS", 51, BS_COL_R3, BS_COL_R4, BS_COL_R5),
]
BS_LIAB_MAPPING = [
    ("買掛金", "BS", 76, BS_COL_R3, BS_COL_R4, BS_COL_R5),
    ("借入金", "BS", 77, BS_COL_R3, BS_COL_R4, BS_COL_R5),
    ("前受金", "BS", 78, BS_COL_R3, BS_COL_R4, BS_COL_R5),
    ("仮受金", "BS", 79, BS_COL_R3, BS_COL_R4, BS_COL_R5),
    ("在庫評価引当金", "BS", 80, BS_COL_R3, BS_COL_R4, BS_COL_R5),
    ("貸倒引当金", "BS", 81, BS_COL_R3, BS_COL_R4, BS_COL_R5),
    ("退職給付引当金", "BS", 82, BS_COL_R3, BS_COL_R4, BS_COL_R5),
    ("未払法人税等", "BS", 83, BS_COL_R3, BS_COL_R4, BS_COL_R5),
    ("長期借入金", "BS", 96, BS_COL_R3, BS_COL_R4, BS_COL_R5),
    ("負債調整勘定", "BS", 97, BS_COL_R3, BS_COL_R4, BS_COL_R5),
    ("預り保証金", "BS", 98, BS_COL_R3, BS_COL_R4, None),  # H98は無し
]
BS_EQ_MAPPING = [
    ("資本金", "BS", 112, BS_COL_R3, BS_COL_R4, BS_COL_R5),
    ("繰越利益剰余金", "BS", 121, BS_COL_R3, BS_COL_R4, BS_COL_R5),
    ("当期純利益", "BS", 122, BS_COL_R3, BS_COL_R4, BS_COL_R5),
]

# SGA: 内訳書の科目を**個別に**該当行へ転記。雑費への丸投げは禁止。
# (JSONキー, シート名, 行, 期1列, 期2列, 期3列)。複数キーは同じ行にマッピング可（別名）。
SGA_COL_R3, SGA_COL_R4, SGA_COL_R5 = 5, 7, 9
SGA_MAPPING = [
    ("役員報酬", "SGA", 8, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("給料手当", "SGA", 9, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),  # 先に書く（次で上書き可）
    ("給与・賞与・退職金", "SGA", 9, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("法定福利費", "SGA", 10, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("退職給付引当金繰入", "SGA", 11, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("運賃", "SGA", 13, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("海上保険料", "SGA", 14, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("船積関連費用", "SGA", 15, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("銀行手数料", "SGA", 16, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("荷造包装費", "SGA", 17, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("保管料", "SGA", 18, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("車両運送料", "SGA", 19, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("販売手数料", "SGA", 20, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("補償費", "SGA", 21, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("輸出関連諸雑費", "SGA", 22, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("広告宣伝費", "SGA", 23, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("通信費", "SGA", 24, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("旅費交通費", "SGA", 25, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("接待交際費", "SGA", 26, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("事務用品費", "SGA", 27, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("見本費", "SGA", 28, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("サービス料", "SGA", 29, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("水道光熱費", "SGA", 30, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("開発費", "SGA", 31, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("支払手数料", "SGA", 32, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("租税公課", "SGA", 33, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("保険料", "SGA", 34, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("顧問料", "SGA", 35, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("地代家賃", "SGA", 36, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),  # 先に書く
    ("賃借料", "SGA", 36, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("修繕費", "SGA", 37, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("減価償却費", "SGA", 38, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("管理費", "SGA", 39, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("寄付金", "SGA", 40, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("貸倒引当金繰入", "SGA", 41, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
    ("雑費", "SGA", 42, SGA_COL_R3, SGA_COL_R4, SGA_COL_R5),
]
SGA_START_ROW = 8
SGA_END_ROW = 42

# JSONキーの別名（PDF表記ゆれ用）
KEY_ALIASES = {
    "法人税、住民税及び事業税": "法人税住民税及び事業税",
    "建物・建物附属設備": "建物建物附属設備",
}
# BS: 借入金＝短期借入金（流動負債）。表に「短期借入金」「借入金」のどちらかで記載される
SGA_KEY_ALIASES = {
    "給料手当": "給与・賞与・退職金",
    "地代家賃": "賃借料",
}


def _normalize_key(k: str) -> str:
    if not k:
        return k
    s = k.strip().replace(" ", "").replace("　", "").replace("・", "")
    return KEY_ALIASES.get(k.strip(), s)


def _parse_number(v) -> int | None:
    """文字列や数値から1円単位の整数に変換。マイナス表記（△、( )）に対応"""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return int(v)
    if isinstance(v, str):
        v = v.replace(",", "").replace(" ", "").strip()
        if v in ("", "-", "－", "―", "—"):
            return 0
        if v.startswith("△") or v.startswith("(") or "－" in v[:1] or (v.startswith("-") and len(v) > 1):
            v = v.replace("△", "").replace("(", "").replace(")", "").replace("－", "-").strip()
            try:
                n = re.sub(r"[^0-9]", "", v)
                return -int(n) if n else 0
            except ValueError:
                pass
        try:
            return int(re.sub(r"[^0-9-]", "", v) or "0")
        except ValueError:
            return None
    return None


def _get_value(data: dict, key: str):
    """data から key または別名で値を取得。数値は int に統一（円単位）"""
    v = data.get(key)
    if v is None:
        v = data.get(KEY_ALIASES.get(key, key))
    if v is None:
        for alias, canonical in KEY_ALIASES.items():
            if canonical == key:
                v = data.get(alias)
                break
    if v is None:
        return None
    return _parse_number(v)


# -----------------------------------------------------------------------------
# Step1: PDF → 全ページを1ページずつ Gemini Vision で解析（スキップなし）
# -----------------------------------------------------------------------------
# 表の構造・単位を正確に読み取り、桁を絶対に間違えないよう厳格に指示
PROMPT_ONE_PAGE = """あなたは日本の決算書（スキャン画像）の1ページを読み取る専門家です。
この画像は決算書一式の**1ページ**です。

【最重要：単位と桁の厳守】
- まず表の**単位**を必ず確認すること。表の隅・表頭に「単位：千円」「（千円）」「単位：円」等の記載がある。**「千円」とあれば、表に書かれた数値に×1000して円に換算した整数を出力する。**「円」とあればそのままの整数で出力する。単位を誤ると桁がずれるので絶対に確認すること。
- **桁を絶対に間違えないこと。** 売上高は通常数百万円〜数億円オーダー。1円単位の整数のみ出力。小数点や丸め禁止。
- 表の**構造**を確認すること。行ラベル（売上高・現金預金等）と列（前年・当期など）を正しく対応させる。**当期**（この書類の年度）の列の数値だけを採用する。

【やること】
1. このページに「損益計算書」「貸借対照表」「販売費及び一般管理費の内訳書」のいずれかが含まれているか判定する。
2. 含まれていれば、上記の単位・桁ルールに従い、**当期**列の数値を**1円単位の整数**で抽出する。
3. **販管費内訳書**の場合は、役員報酬・給料手当・法定福利費・賃借料・減価償却費など**科目ごとに個別**で抽出。合計を雑費にまとめることは禁止。
4. 該当する表が無い場合は空のJSON {} を返す。

【必須で探す項目】
- 損益計算書: 売上高, 期首商品棚卸高, 仕入高, 期末商品棚卸高, 受取利息, 為替差益, 雑収入, 支払利息, 雑損失, 固定資産売却益, 負債調整勘定戻入, 固定資産除却損, 投資有価証券評価損, 仮払金評価損, 在庫評価引当金繰入, 子会社清算損, 不良債権, 法人税、住民税及び事業税, 当期純利益, 販売費及び一般管理費
- 貸借対照表: 現金預金, 売掛金, 棚卸資産, 貸付金, 前払費用, 仮払金, 仮払税金, 建物・建物附属設備, 機械装置, 車両運搬具, 器具備品, 土地, 電話加入権, ソフトウェア, のれん, 投資有価証券, 保証金, 長期前払費用, 買掛金, 借入金（短期借入金）, 前受金, 仮受金, 在庫評価引当金, 貸倒引当金, 退職給付引当金, 未払法人税等, 長期借入金, 負債調整勘定, 預り保証金, 資本金, 繰越利益剰余金, 当期純利益
- 販管費内訳: 役員報酬, 給与・賞与・退職金, 給料手当, 法定福利費, 退職給付引当金繰入, 運賃, 海上保険料, 船積関連費用, 銀行手数料, 荷造包装費, 保管料, 車両運送料, 販売手数料, 補償費, 輸出関連諸雑費, 広告宣伝費, 通信費, 旅費交通費, 接待交際費, 事務用品費, 見本費, サービス料, 水道光熱費, 開発費, 支払手数料, 租税公課, 保険料, 顧問料, 賃借料, 地代家賃, 修繕費, 減価償却費, 管理費, 寄付金, 貸倒引当金繰入, 雑費

抽出できた項目だけをキーにしたJSONで出力。見つからなかったキーは含めない。説明文は不要。JSONのみ。
"""


def pdf_to_images(pdf_path: Path, max_pages: int = 99):
    """PDFを画像バイトのリストに変換（PyMuPDF）。全ページ取得するためデフォルト99。"""
    try:
        import fitz
    except ImportError:
        raise ImportError("PyMuPDF が必要です: pip install pymupdf")
    doc = fitz.open(pdf_path)
    n = min(len(doc), max_pages)
    images = []
    for i in range(n):
        page = doc[i]
        pix = page.get_pixmap(dpi=200, alpha=False)
        images.append(pix.tobytes("png"))
    doc.close()
    return images


def _parse_extraction_response(text: str, year_label: str, phase: str) -> dict:
    """Gemini応答テキストからJSONを抽出し、数値に統一したdictを返す。"""
    json_match = re.search(r"\{[\s\S]*\}", text)
    if not json_match:
        return {}
    try:
        data = json.loads(json_match.group(0))
    except json.JSONDecodeError:
        return {}
    out = {}
    for k, v in data.items():
        if v is None:
            out[k] = None
        elif isinstance(v, (int, float)):
            out[k] = int(v)
        else:
            out[k] = _get_value(data, k)
    return out


def _merge_page_into(base: dict, page_data: dict) -> None:
    """page_data の非null値を base に上書き（base が null のときだけ）。"""
    for k, v in page_data.items():
        if v is not None and (base.get(k) is None):
            base[k] = v


def extract_with_gemini(pdf_path: Path, year_label: str) -> dict:
    """PDFを全ページ画像化し、1ページずつGemini Visionで解析。全ページスキップなし。結果をマージして返す。"""
    import google.generativeai as genai
    import io
    import time
    from PIL import Image

    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY が設定されていません")

    genai.configure(api_key=api_key)
    model_name = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
    model = genai.GenerativeModel(model_name)

    images = pdf_to_images(pdf_path, max_pages=99)
    if not images:
        return {}

    image_pil_list = [Image.open(io.BytesIO(b)) for b in images]
    merged = {}

    for page_idx, img in enumerate(image_pil_list):
        parts = [PROMPT_ONE_PAGE, img]
        try:
            response = model.generate_content(parts)
            text = (response.text or "").strip()
            page_data = _parse_extraction_response(text, year_label, f"p{page_idx+1}")
            _merge_page_into(merged, page_data)
            filled = len([v for v in page_data.values() if v is not None])
            if filled > 0:
                print(f"    ページ {page_idx + 1}/{len(image_pil_list)}: {filled} 項目取得")
        except Exception as e:
            print(f"  [WARN] ページ {page_idx + 1} 解析エラー: {e}")
        time.sleep(0.3)

    return merged


def extract_all_pdfs(from_json_path: Path | None = None) -> dict:
    """3つのPDFを順に処理し、{ "令和3年度": {...}, "令和4年度": {...}, "令和5年度": {...} } を返す。
    from_json_path が指定されている場合はそのJSONを読み、APIは呼ばない。"""
    if from_json_path and from_json_path.exists():
        print(f"[読み込み] 抽出結果JSON: {from_json_path}")
        with open(from_json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {
            "令和3年度": data.get("令和3年度", data.get("r3", {})),
            "令和4年度": data.get("令和4年度", data.get("r4", {})),
            "令和5年度": data.get("令和5年度", data.get("r5", {})),
        }

    results = {}
    for pdf_path, year_label in PDFS:
        if not pdf_path.exists():
            print(f"[SKIP] ファイルがありません: {pdf_path}")
            results[year_label] = {}
            continue
        print(f"[抽出] {year_label}: {pdf_path.name}")
        try:
            results[year_label] = extract_with_gemini(pdf_path, year_label)
            print(f"  -> 項目数: {len([v for v in results[year_label].values() if v is not None])}")
        except Exception as e:
            print(f"  [ERROR] {e}")
            results[year_label] = {}
    return results


# -----------------------------------------------------------------------------
# Step2: 抽出データをExcelに転記（値のみ・数式は触らない）
# -----------------------------------------------------------------------------
def apply_to_excel(extracted: dict, template_path: Path, output_path: Path) -> None:
    import openpyxl
    from datetime import datetime

    wb = openpyxl.load_workbook(template_path)
    r3, r4, r5 = extracted.get("令和3年度", {}), extracted.get("令和4年度", {}), extracted.get("令和5年度", {})

    def write_cell(sheet_name: str, row: int, col: int, value):
        """値セルにのみ value を書き込む。数式セルは触らない。value=None の場合は空白でクリア（デビス残存を消す）。"""
        if col is None:
            return
        try:
            sheet = wb[sheet_name]
            cell = sheet.cell(row=row, column=col)
            if isinstance(cell.value, str) and cell.value.strip().startswith("="):
                return  # 数式は保護
            cell.value = value  # 抽出値または None（空白で上書き）
        except Exception as e:
            print(f"  [WARN] {sheet_name}!{openpyxl.utils.get_column_letter(col)}{row} = {value} でエラー: {e}")

    def get_val(data: dict, key: str):
        v = data.get(key)
        if v is None:
            v = data.get(key.replace("・", "").replace(" ", ""))
        if v is None and "法人税" in key and "住民税" in key:
            v = data.get("法人税、住民税及び事業税")
        if v is None and key == "借入金":
            v = data.get("短期借入金")
        if v is None:
            v = data.get("建物・建物附属設備") if "建物" in key else None
        if v is None and key in SGA_KEY_ALIASES:
            v = data.get(SGA_KEY_ALIASES[key])
        if v is not None:
            return _parse_number(v)
        return _get_value(data, key)

    # 基本情報（常に上書き：富山技研興業用）
    try:
        ws = wb["基本情報"]
        ws["D5"] = FISCAL_END_R5   # 決算期（令和5年度末日）
        ws["D9"] = COMPANY_NAME    # 商号
    except Exception as e:
        print(f"  [WARN] 基本情報 書き込み: {e}")

    # 各シートの列ヘッダー（年月）を決算期に合わせて上書き（Excelのセルアドレスで直接指定）
    # PL: 5行目 E5,G5,I5 / BS: 6行目 F6,G6,H6 / SGA・製造原価: 5行目 E5,G5,I5
    header_updates = [
        ("PL", [("E5", FISCAL_END_R3), ("G5", FISCAL_END_R4), ("I5", FISCAL_END_R5)]),
        ("BS", [("F6", FISCAL_END_R3), ("G6", FISCAL_END_R4), ("H6", FISCAL_END_R5)]),
        ("SGA", [("E5", FISCAL_END_R3), ("G5", FISCAL_END_R4), ("I5", FISCAL_END_R5)]),
        ("製造原価", [("E5", FISCAL_END_R3), ("G5", FISCAL_END_R4), ("I5", FISCAL_END_R5)]),
    ]
    for sheet_name, cell_values in header_updates:
        if sheet_name not in wb.sheetnames:
            continue
        ws = wb[sheet_name]
        for cell_addr, value in cell_values:
            try:
                ws[cell_addr] = value
            except Exception:
                pass

    # PL: 抽出値があればそのまま、なければ None でクリア（デビス残存を消す）
    for key, sheet, row, c3, c4, c5 in PL_MAPPING:
        for col, data in [(c3, r3), (c4, r4), (c5, r5)]:
            if col is None:
                continue
            v = get_val(data, key) or get_val(data, key.replace(" ", ""))
            write_cell(sheet, row, col, v)

    # BS: 同様に抽出値 or None で上書き
    for key, sheet, row, c3, c4, c5 in BS_ASSET_MAPPING + BS_LIAB_MAPPING + BS_EQ_MAPPING:
        for col, data in [(c3, r3), (c4, r4), (c5, r5)]:
            if col is None:
                continue
            v = get_val(data, key) or get_val(data, key.replace("・", ""))
            write_cell(sheet, row, col, v)

    # SGA: 内訳科目を**個別に**該当行へ転記。雑費への丸投げは禁止。全行いったんクリア後に科目ごと書き込み。
    for r in range(SGA_START_ROW, SGA_END_ROW + 1):
        for col in [5, 7, 9]:
            write_cell("SGA", r, col, None)
    for key, sheet, row, c3, c4, c5 in SGA_MAPPING:
        for col, data in [(c3, r3), (c4, r4), (c5, r5)]:
            if col is None:
                continue
            v = get_val(data, key)
            if v is not None:
                write_cell(sheet, row, col, v)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    wb.save(output_path)
    print(f"[保存] {output_path}")


def _cell_value(ws, addr) -> int | float | None:
    v = ws[addr].value
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return v
    return None


def self_check_sales(wb_path: Path) -> bool:
    """PLのE8,G8,I8（期1・期2・期3の売上高）に実数（百万円以上）が入っているか確認。"""
    import openpyxl
    wb = openpyxl.load_workbook(wb_path, read_only=True, data_only=True)
    try:
        ws = wb["PL"]
        cells = [("E8", _cell_value(ws, "E8")), ("G8", _cell_value(ws, "G8")), ("I8", _cell_value(ws, "I8"))]
    finally:
        wb.close()
    min_expected = 1_000_000
    ok = True
    for addr, val in cells:
        if val is None or (isinstance(val, (int, float)) and float(val) < min_expected):
            print(f"[セルフチェック NG] PL!{addr} = {val} （Noneまたは{min_expected:,}円未満は不可）")
            ok = False
        else:
            print(f"[セルフチェック OK] PL!{addr} = {val}")
    return ok


def main():
    import argparse
    parser = argparse.ArgumentParser(description="富山技研興業 マテリアル転記")
    parser.add_argument("--from-json", type=Path, default=None, help="抽出済みJSON（APIを使わず転記のみ）")
    args = parser.parse_args()

    print("富山技研興業 マテリアル転記スクリプト")
    print("テンプレート:", TEMPLATE_PATH)
    if not TEMPLATE_PATH.exists():
        print("エラー: テンプレートが見つかりません")
        sys.exit(1)

    # PDFから再抽出する場合は既存JSONを破棄（精度不良データを使わない）
    if args.from_json is None:
        json_path = OUTPUT_DIR / "toyama_giken_extracted.json"
        if json_path.exists():
            json_path.unlink()
            print(f"[削除] 既存の抽出JSONを破棄しました: {json_path}")

    extracted = extract_all_pdfs(from_json_path=args.from_json)
    # APIで抽出した場合はJSON保存（再実行時に --from-json で転記のみ可能）
    if args.from_json is None and any(extracted.get(y) for y in ("令和3年度", "令和4年度", "令和5年度")):
        json_path = OUTPUT_DIR / "toyama_giken_extracted.json"
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(extracted, f, ensure_ascii=False, indent=2)
        print(f"[保存] 抽出結果JSON: {json_path}")

    out_path = OUTPUT_DIR / OUTPUT_EXCEL_NAME
    apply_to_excel(extracted, TEMPLATE_PATH, out_path)
    if not self_check_sales(out_path):
        print("エラー: 売上高（E8,G8,I8）に実数が入っていません。抽出ロジックを確認してください。")
        sys.exit(1)
    print("完了:", out_path)


if __name__ == "__main__":
    main()
