import os
import zipfile
import re
import csv
from bs4 import BeautifulSoup

# 設定
DATA_DIR = "edinet_data"
OUTPUT_FILE = "relations_raw.csv"

def get_edinet_code(filename, html_path):
    """
    ファイル名またはHTMLのパスからEDINETコード（EXXXXX）を抽出する
    例: S100W75T.zip や 0101010_..._E01098-000_...htm
    """
    match = re.search(r'E\d{5}', html_path)
    if match:
        return match.group(0)
    return "UNKNOWN"

def extract_from_zip(zip_path):
    relations = []
    doc_id = os.path.splitext(os.path.basename(zip_path))[0]
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            htm_files = [f for f in z.namelist() if f.startswith('XBRL/PublicDoc/') and f.endswith('.htm')]
            
            for htm_file in htm_files:
                if "header" in htm_file or "audit" in htm_file:
                    continue
                
                edinet_code = get_edinet_code(doc_id, htm_file)
                
                with z.open(htm_file) as f:
                    soup = BeautifulSoup(f.read(), 'html.parser')
                
                target_pattern = re.compile(r"(AffiliatedEntities|ConsolidatedSubsidiaries|UnconsolidatedSubsidiaries)", re.IGNORECASE)
                blocks = soup.find_all(attrs={"name": target_pattern})
                
                for block in blocks:
                    text_content = block.get_text(separator=' ', strip=True).replace('　', ' ')
                    pattern = r"([^\s、。，．「」『』【】]+(?:株式会社|有限会社|合同会社|有限公司|Inc\.|Co\.,\s*Ltd\.|Corp\.|Company|GmbH|B\.V\.)[^\s、。，．「」『』【】]*)"
                    matches = re.findall(pattern, text_content, re.IGNORECASE)
                    
                    for m in matches:
                        clean_name = re.sub(r"（注\d*）|\(注\d*\)", "", m)
                        noise_words = ["であります", "の決算", "及び", "など", "等の", "は、", "との", "に対する", "に該当", "の提出"]
                        for noise in noise_words:
                            clean_name = clean_name.split(noise)[0]
                        
                        if len(clean_name) > 3 and "非連結" not in clean_name and "主要な" not in clean_name and "提出" not in clean_name:
                            relations.append({
                                "parent_edinet_code": edinet_code,
                                "child_company_name": clean_name,
                                "source_doc_id": doc_id
                            })
    except Exception as e:
        print(f"Error processing {zip_path}: {e}")
        
    return relations

if __name__ == "__main__":
    zip_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.zip')]
    total_files = len(zip_files)
    all_data = []

    print(f"全 {total_files} 件のZIPファイルの解析を開始します...")

    for i, zip_file in enumerate(zip_files):
        zip_path = os.path.join(DATA_DIR, zip_file)
        results = extract_from_zip(zip_path)
        all_data.extend(results)
        
        if (i + 1) % 10 == 0 or (i + 1) == total_files:
            print(f"進捗: {i + 1}/{total_files} 件完了")

    # 重複排除（同じ書類から同じ会社名が複数出た場合をまとめる）
    unique_data = list({(d['parent_edinet_code'], d['child_company_name'], d['source_doc_id']): d for d in all_data}.values())

    # CSV出力
    with open(OUTPUT_FILE, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["parent_edinet_code", "child_company_name", "source_doc_id"])
        writer.writeheader()
        writer.writerows(unique_data)

    print(f"\n完了！ {len(unique_data)} 件のリレーションを {OUTPUT_FILE} に出力しました。")