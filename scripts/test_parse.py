import os
import zipfile
import re
from bs4 import BeautifulSoup

DATA_DIR = "edinet_data"

def extract_companies_from_html(zip_path):
    print(f"解析対象: {os.path.basename(zip_path)}")
    
    with zipfile.ZipFile(zip_path, 'r') as z:
        # ZIP内からメインのHTMファイルを取得
        htm_files = [f for f in z.namelist() if f.startswith('XBRL/PublicDoc/') and f.endswith('.htm')]
        
        if not htm_files:
            return False

        found_companies = False
        
        for htm_file in htm_files:
            if "header" in htm_file or "audit" in htm_file:
                continue
                
            with z.open(htm_file) as f:
                html_content = f.read()
                
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 「関係会社の状況」などが書かれたテキストブロックをすべて取得
            target_pattern = re.compile(r"(AffiliatedEntities|ConsolidatedSubsidiaries|UnconsolidatedSubsidiaries)", re.IGNORECASE)
            blocks = soup.find_all(attrs={"name": target_pattern})
            
            for block in blocks:
                # ブロック内のテキストを抽出（タグを除去して純粋な文字列にする）
                text_content = block.get_text(separator=' ', strip=True).replace('　', ' ')
                
                # 法人格を示すキーワードを含む文字列を正規表現で抽出
                pattern = r"([^\s、。，．「」『』【】]+(?:株式会社|有限会社|合同会社|有限公司|Inc\.|Co\.,\s*Ltd\.|Corp\.|Company|GmbH|B\.V\.)[^\s、。，．「」『』【】]*)"
                matches = re.findall(pattern, text_content, re.IGNORECASE)
                
                valid_companies = []
                for m in matches:
                    # 1. 末尾の（注）などの不要な文字を削除
                    clean_name = re.sub(r"（注\d*）|\(注\d*\)", "", m)
                    
                    # 2. 抽出時にくっついてしまう日本語の助詞やノイズをカット（クリーニング処理）
                    noise_words = ["であります", "の決算", "及び", "など", "等の", "は、", "との", "に対する", "に該当", "の提出"]
                    for noise in noise_words:
                        clean_name = clean_name.split(noise)[0]
                    
                    # 3. 一般名詞（ノイズ）を除外してリストに追加
                    if len(clean_name) > 3 and "非連結" not in clean_name and "主要な" not in clean_name and "提出" not in clean_name:
                        valid_companies.append(clean_name)
                
                if valid_companies:
                    if not found_companies:
                        print(f"読み込み成功: {os.path.basename(htm_file)}")
                        print("【抽出された関係会社（テキスト解析）】")
                        found_companies = True
                        
                    # 重複を除外してリスト表示
                    for comp in set(valid_companies):
                        print(f"- {comp}")

        if found_companies:
            print("\n")
            return True
        else:
            print("関係会社を示すテキストが見つかりませんでした。\n")
            return False

if __name__ == "__main__":
    zip_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.zip')]
    
    if not zip_files:
        print("ZIPファイルが見つかりません。")
    else:
        print(f"全 {len(zip_files)} 件のZIPファイルから、HTML内のテキストデータを解析します...\n")
        
        for zip_file in zip_files:
            test_zip_path = os.path.join(DATA_DIR, zip_file)
            is_success = extract_companies_from_html(test_zip_path)
            
            if is_success:
                print("✅ HTMLテキストからの正規表現抽出ロジック（クリーニング済）に大成功しました！")
                break