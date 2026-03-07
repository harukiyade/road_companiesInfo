-- 1. 中間テーブルへのデータ投入

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '大英産業株式会社' AND c.name = '大英リビングサポート株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社進和' AND c.name = '煙台三拓進和撹拌設備維修有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社進和' AND c.name = '進和（天津）自動化控制設備有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社進和' AND c.name = '那欧雅進和（上海）貿易有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社進和' AND c.name = '煙台進和接合技術有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ヤマトホールディングス株式会社' AND c.name = 'MEDICAおよびヤマトエナジーマネジメント株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ヤマトホールディングス株式会社' AND c.name = '現YDM株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ヤマトホールディングス株式会社' AND c.name = 'Transport株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ヤマトホールディングス株式会社' AND c.name = 'ヤマトダイアログ＆メディア株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ヤマトホールディングス株式会社' AND c.name = '現アートセッティングデリバリー株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ヤマトホールディングス株式会社' AND c.name = 'ヤマトホームコンビニエンス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ヤマトホールディングス株式会社' AND c.name = 'RH株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ムサシ' AND c.name = '社連結子会社の名称武蔵エンジニアリング株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ムサシ' AND c.name = 'エム株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ムサシ' AND c.name = '武蔵興産株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ムサシ' AND c.name = '株式会社武蔵エンタープライズ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ムサシ' AND c.name = 'イメージ情報株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ムサシ' AND c.name = 'エス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ムサシ' AND c.name = 'テクノ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ムサシ' AND c.name = 'サポート株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ムサシ' AND c.name = '社非連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ムサシ' AND c.name = '社持分法を適用した関係会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ムサシ' AND c.name = '社持分法を適用していない非連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社マルゼン' AND c.name = '連結子会社の数2社主要な連結子会社の名称マルゼン工業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社マルゼン' AND c.name = '非連結子会社の名称等非連結子会社台湾丸善股份有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社マルゼン' AND c.name = '持分法を適用しない非連結子会社の台湾丸善股份有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '大同工業株式会社' AND c.name = '非連結子会社名株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '大同工業株式会社' AND c.name = '翔研工業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '大同工業株式会社' AND c.name = '大同テクノダイド建設株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '大同工業株式会社' AND c.name = '及び関連会社４社（株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '大同工業株式会社' AND c.name = '翔研工業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '大同工業株式会社' AND c.name = '非連結子会社２社（ダイド建設株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '大同工業株式会社' AND c.name = 'F2テクノ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ジャムコ' AND c.name = 'オレンジジャムコ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ジャムコ' AND c.name = '徳島ジャムコ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ジャムコ' AND c.name = 'ジャムコエアロテック株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ジャムコ' AND c.name = 'INC.当社の連結子会社であった株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ジャムコ' AND c.name = '社当該連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ジャムコ' AND c.name = '宮崎ジャムコ及び株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ジャムコ' AND c.name = '年４月１日付で株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ジャムコ' AND c.name = '同じく連結子会社である株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ジャムコ' AND c.name = 'ジャムコエアクラフトインテリアズ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ジャムコ' AND c.name = 'Japan株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社タカショー' AND c.name = 'トーコー資材株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社タカショー' AND c.name = '当社の連結子会社であった株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社タカショー' AND c.name = '九江高秀園芸製品有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社タカショー' AND c.name = '江西高秀進出口貿易有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社タカショー' AND c.name = '社連結子会社の名称ガーデンクリエイト株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社タカショー' AND c.name = 'グリーン情報株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社タカショー' AND c.name = 'Limited香港高秀集團有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社タカショー' AND c.name = 'Corp.株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社タカショー' AND c.name = '佛山市南方高秀電子科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社タカショー' AND c.name = '浙江正特高秀園芸建材有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社タカショー' AND c.name = '持分法を適用しない関連会社株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社タカショー' AND c.name = '上海高秀園芸建材有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社タカショー' AND c.name = '及び満洲里高秀木業有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社大和' AND c.name = '金沢都市開発株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社大和' AND c.name = '％以下を自己の計算において所有しているにもかかわらず関連会社としなかった主要な会社等の名称総曲輪シテイ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社大和' AND c.name = 'オタヤ開発株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ケー・エフ・シー' AND c.name = 'アールシーアイ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ケー・エフ・シー' AND c.name = '唐山日翔建材科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ケー・エフ・シー' AND c.name = '唐山日翔建材科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社マナック・ケミカル・パートナーズ' AND c.name = 'マナック上海貿易有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'フルサト・マルカホールディングス株式会社' AND c.name = 'C.V.上海丸嘉貿易有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'フルサト・マルカホールディングス株式会社' AND c.name = '広州丸嘉貿易有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'フルサト・マルカホールディングス株式会社' AND c.name = '上海丸嘉貿易有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '東京エレクトロン株式会社' AND c.name = '東京エレクトロン九州株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '東京エレクトロン株式会社' AND c.name = '東京エレクトロンＦＥ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '東京エレクトロン株式会社' AND c.name = '東京エレクトロン宮城株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '東京エレクトロン株式会社' AND c.name = 'テクノロジーソリューションズ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '東京エレクトロン株式会社' AND c.name = 'デバイス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社メタリアル' AND c.name = 'Limited蘇州宏樹視覚芸術有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社サトー商会' AND c.name = 'の数……1社株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社サトー商会' AND c.name = 'サトーサービス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社サトー商会' AND c.name = '非連結子会社……3社株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社サトー商会' AND c.name = 'サトー食肉サービス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社サトー商会' AND c.name = 'サトーサービス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社サトー商会' AND c.name = '持分法適用関連会社……1社株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社サトー商会' AND c.name = 'サトー食肉サービス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社サトー商会' AND c.name = '持分法適用非連結子会社……3社株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ＪＲＣ' AND c.name = 'Ｃ＆Ｍ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ＪＲＣ' AND c.name = '株式会社高橋汽罐工業向井化工機株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ＪＲＣ' AND c.name = '株式会社大成中村自働機械株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ＪＲＣ' AND c.name = '非連結子会社の名称吉艾希商事(瀋陽)貿易有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ＪＲＣ' AND c.name = '持分法を適用しない非連結子会社の名称吉艾希商事(瀋陽)貿易有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社自重堂' AND c.name = '南山自重堂防護科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '東邦ホールディングス株式会社' AND c.name = '当連結会計年度において株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '東邦ホールディングス株式会社' AND c.name = '株式会社清水薬局及び株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '東邦ホールディングス株式会社' AND c.name = '主要な非連結子会社の名称等主要な非連結子会社の名称沖縄東邦株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '東邦ホールディングス株式会社' AND c.name = 'あゆみ製薬株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '東邦ホールディングス株式会社' AND c.name = '社主要な会社等の名称酒井薬品株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '東邦ホールディングス株式会社' AND c.name = 'あゆみ製薬ホールディングス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '東邦ホールディングス株式会社' AND c.name = '持分法適用により生じたあゆみ製薬ホールディングス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '東邦ホールディングス株式会社' AND c.name = '関連会社等の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '東邦ホールディングス株式会社' AND c.name = '持分法を適用していない非連結子会社及び関連会社等の状況主要な非連結子会社の名称沖縄東邦株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社遠藤製作所' AND c.name = '社連結子会社の名称エポンゴルフ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社遠藤製作所' AND c.name = '社持分法適用関連会社の名称セブンシックス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = 'カピタルバリアブレ偉福科技工業(中山)有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '株式会社九州エフテック株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '社フクダエンジニアリング株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '偉福科技工業(武漢)有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '偉福(広州)汽車技術開発有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '煙台福研模具有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = 'インドネシア城南武漢科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '株式会社城南九州製作所城南佛山科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = 'カピタルバリアブレ偉福科技工業(中山)有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '株式会社九州エフテック株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '社フクダエンジニアリング株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '偉福科技工業(武漢)有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '偉福(広州)汽車技術開発有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '煙台福研模具有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = 'カピタルバリアブレ偉福科技工業(中山)有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '株式会社九州エフテック株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '社フクダエンジニアリング株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '偉福科技工業(武漢)有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '偉福(広州)汽車技術開発有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '煙台福研模具有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = 'インドネシア城南武漢科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '株式会社城南九州製作所城南佛山科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社アクセル' AND c.name = 'ax株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社アクセル' AND c.name = 'aimRage株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'こころネット株式会社' AND c.name = 'ハートライン株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'こころネット株式会社' AND c.name = 'フルール株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'こころネット株式会社' AND c.name = 'ベトナム有限会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'こころネット株式会社' AND c.name = 'Ｗedding株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'こころネット株式会社' AND c.name = '北関東互助センター株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'こころネット株式会社' AND c.name = '社連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'こころネット株式会社' AND c.name = 'トレーディング株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'こころネット株式会社' AND c.name = '天津万里石石材有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'こころネット株式会社' AND c.name = 'より商号変更しております。日本エンディングパートナーズ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'こころネット株式会社' AND c.name = '関連会社の名称天津万里石石材有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'こころネット株式会社' AND c.name = '年５月16日付けで天津中建万里石石材有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社大林組' AND c.name = 'ツクシ工業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社大林組' AND c.name = 'ＭｉＴＡＳＵＮ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社大林組' AND c.name = 'ＰＦＩ京大桂物理系研究棟株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '東亜建設工業株式会社' AND c.name = '東亜機械工業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '東亜建設工業株式会社' AND c.name = '信幸建設株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '東亜建設工業株式会社' AND c.name = 'かずさまごころサービス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '東亜建設工業株式会社' AND c.name = '持分法の適用に関する事項非連結子会社(かずさまごころサービス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '東亜建設工業株式会社' AND c.name = 'ほか)及び関連会社(浅間山開発株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '広島ガス株式会社' AND c.name = '有)広島エルピージー配送センター東部エルピージーセンター株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'カナデビア株式会社' AND c.name = '鎮江中船日立造船機械有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'カナデビア株式会社' AND c.name = '上海康恒昱造環境技術有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '高田工業所株式会社' AND c.name = '渡部工業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '高田工業所株式会社' AND c.name = '高田サービス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '高田工業所株式会社' AND c.name = '高田プラント建設株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '石原産業株式会社' AND c.name = 'ホクサン株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '石原産業株式会社' AND c.name = '新たに設立したＭＦマテリアル株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '石原産業株式会社' AND c.name = 'ホクサン株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社タカギセイコー' AND c.name = '高木精工香港有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社タカギセイコー' AND c.name = '高和精工上海有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社タカギセイコー' AND c.name = '佛山市南海華達高木模具有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社タカギセイコー' AND c.name = '高木汽車部件佛山有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社タカギセイコー' AND c.name = '武漢高木汽車部件有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社タカギセイコー' AND c.name = '大連大顕高木模具有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'リケンＮＰＲ株式会社' AND c.name = 'サイアムリケン社シュリラムピストンアンドリング社南京理研動力系統零部件有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'リケンＮＰＲ株式会社' AND c.name = '聖龍理研新能源寧波有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '木徳神糧株式会社' AND c.name = '有限会社末長一番保険サービス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ＣＫサンエツ' AND c.name = '株式会社サンエツ商事三越金属上海有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ＣＫサンエツ' AND c.name = '台湾三越股份有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社トーホー' AND c.name = 'Seafood（S）Pte.Ltd.'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社トーホー' AND c.name = '社（株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社トーホー' AND c.name = '昭和物産株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社トーホー' AND c.name = '関東食品株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ｆａｎｔａｓｉｓｔａ' AND c.name = 'NSアセットマネジメント合同会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '旭化学工業株式会社' AND c.name = '旭日塑料制品（昆山）有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '上村工業株式会社' AND c.name = '上村化学（上海）有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '上村工業株式会社' AND c.name = '上村（香港）有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '上村工業株式会社' AND c.name = '上村工業（深圳）有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '上村工業株式会社' AND c.name = '社連結子会社名台湾上村股份有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '上村工業株式会社' AND c.name = '韓国上村株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'テクミラホールディングス株式会社' AND c.name = '創世訊聯科技深圳有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社アイ・エス・ビー' AND c.name = '株式取得により株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社アイ・エス・ビー' AND c.name = '当社は連結子会社であった株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社プラザホールディングス' AND c.name = '年８月30日付で株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社オオバ' AND c.name = 'オオバ調査測量株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社オオバ' AND c.name = '日本都市整備株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社オオバ' AND c.name = '東北都市整備株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社オオバ' AND c.name = '社（2）連結子会社の名称近畿都市整備株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エイチ・アイ・エス' AND c.name = '同じく連結子会社である九州産交ランドマーク株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エイチ・アイ・エス' AND c.name = 'INC.に商号変更しております。当社の連結子会社であった九州産交カード株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エイチ・アイ・エス' AND c.name = 'Mobile株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エイチ・アイ・エス' AND c.name = 'トラベル及びH.I.S.エネルギーホールディングス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エイチ・アイ・エス' AND c.name = 'Eホールディングス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エイチ・アイ・エス' AND c.name = '重要性が増したため連結の範囲に含めております。当社の連結子会社であった株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エイチ・アイ・エス' AND c.name = '当社の持分法適用関連会社であったLY-HISトラベル株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エイチ・アイ・エス' AND c.name = '清算手続き結了により持分法の適用から除外しております。当社の持分法適用関連会社であった株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エイチ・アイ・エス' AND c.name = 'ヴィソンホテルマネジメント株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エイチ・アイ・エス' AND c.name = '電力株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エイチ・アイ・エス' AND c.name = '清算手続き結了等により連結の範囲から除外しております。当社の連結子会社であったＨＴＢエナジー株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エイチ・アイ・エス' AND c.name = '同社を連結の範囲から除外しております。当社の連結子会社であったハウステンボス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エイチ・アイ・エス' AND c.name = '株式の取得により連結の範囲に含めております。当社の連結子会社であった株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エイチ・アイ・エス' AND c.name = '株式の売却に伴い連結の範囲から除外しております。当社の連結子会社であった肥後リカー株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エイチ・アイ・エス' AND c.name = '清算手続き結了により連結の範囲から除外しております。当社の連結子会社であった株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エイチ・アイ・エス' AND c.name = '同じく連結子会社である九州産交リテール株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エイチ・アイ・エス' AND c.name = '持分法適用の範囲に含めております。当社の持分法適用関連会社であったH.I.F.株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エイチ・アイ・エス' AND c.name = '株式の取得により連結の範囲に含めております。株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エイチ・アイ・エス' AND c.name = '重要性が増したため連結の範囲に含めております。当社の連結子会社であったＨＴＢクルーズ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エイチ・アイ・エス' AND c.name = '新たに設立したため連結の範囲に含めております。株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エイチ・アイ・エス' AND c.name = '連結の範囲から除外しております。当社の連結子会社であったH.I.F.株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エイチ・アイ・エス' AND c.name = '清算手続き結了により連結の範囲から除外しております。当社の連結子会社であった洛碁中華大飯店股份有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エイチ・アイ・エス' AND c.name = 'H.I.F.株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ワッツ' AND c.name = '全ての子会社を連結しております。連結子会社の数5社連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ワッツ' AND c.name = '社主な会社等の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '日東工業株式会社' AND c.name = 'テンパール工業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '日東工業株式会社' AND c.name = '南海電設株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '日東工業株式会社' AND c.name = 'サンテレホン株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '日東工業株式会社' AND c.name = '社主要な連結子会社名株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '日東工業株式会社' AND c.name = '北川工業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '日東工業株式会社' AND c.name = '日東工業(中国)有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '日東工業株式会社' AND c.name = 'ＥＭソリューションズ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '日東工業株式会社' AND c.name = '持分法を適用しない非連結子会社及び関連会社のうち主要な会社等の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '日東工業株式会社' AND c.name = '府中テンパール寺下工業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '日新商事株式会社' AND c.name = '社竹鶴石油株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '日新商事株式会社' AND c.name = 'NSM諏訪ソーラーエナジー合同会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '日新商事株式会社' AND c.name = '日新レジン株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '日新商事株式会社' AND c.name = '社日新興産株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '日新商事株式会社' AND c.name = 'Ｊリーフ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'アビックス株式会社' AND c.name = '社連結子会社の名称デジタルプロモーション株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'アビックス株式会社' AND c.name = 'Lab.株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'アビックス株式会社' AND c.name = '会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'アビックス株式会社' AND c.name = '年４月１日付で株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'アビックス株式会社' AND c.name = '当連結会計年度中に当社が新たに株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'アビックス株式会社' AND c.name = '当連結会計年度から株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社マサル' AND c.name = '株式会社マサルファシリティーズ空気設備工業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社マサル' AND c.name = '空気設備工業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'トレイダーズホールディングス株式会社' AND c.name = 'FleGrowth耐科斯托普軟件大連有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社オプトラン' AND c.name = '光馳科技上海有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社オプトラン' AND c.name = '光馳科技股份有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社オプトラン' AND c.name = '台湾光馳上海商貿有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社オプトラン' AND c.name = 'Oy光馳半導体技術上海有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社オプトラン' AND c.name = '浙江晶馳光電科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社オプトラン' AND c.name = '安徽繁楓新能源科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社オプトラン' AND c.name = '年7月に上海繁楓真空科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社インフォマート' AND c.name = '当連結会計年度から株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エヌ・シー・エヌ' AND c.name = '社連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エヌ・シー・エヌ' AND c.name = 'HOUSE株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エヌ・シー・エヌ' AND c.name = '木構造デザイン株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エヌ・シー・エヌ' AND c.name = '社会社名株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エヌ・シー・エヌ' AND c.name = '重要性が増した株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エヌ・シー・エヌ' AND c.name = 'ＳＥ住宅ローンサービス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エヌ・シー・エヌ' AND c.name = 'S開発株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社サカイホールディングス' AND c.name = '社連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社サカイホールディングス' AND c.name = 'エスケーアイ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社サカイホールディングス' AND c.name = 'セントラルパートナーズエスケーアイマネージメント株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社サカイホールディングス' AND c.name = 'エスケーアイ開発株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '三洋工業株式会社' AND c.name = 'フジオカエアータイト株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '三洋工業株式会社' AND c.name = '及びスワン商事株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '三洋工業株式会社' AND c.name = '三洋ＵＤ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '三洋工業株式会社' AND c.name = '三洋ＵＤ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ＮＯＫ株式会社' AND c.name = '社。主要な持分法適用関連会社：イーグル工業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ＮＯＫ株式会社' AND c.name = '平和オイルシール工業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ハリマ化成グループ株式会社' AND c.name = '杭州杭化哈利瑪化工有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ハリマ化成グループ株式会社' AND c.name = '三好化成工業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ハリマ化成グループ株式会社' AND c.name = '新日本油化株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ハリマ化成グループ株式会社' AND c.name = '秋田十條化成株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ヤマダコーポレーション' AND c.name = 'ヤマダタイランドＣＯ．，ＬＴＤ．株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ヤマダコーポレーション' AND c.name = 'ヤマダプロダクツサービス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ヤマダコーポレーション' AND c.name = '社連結子会社名ヤマダアメリカＩＮＣ．ヤマダヨーロッパＢ．Ｖ．ヤマダ上海ポンプ貿易有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ニチレイ' AND c.name = 'Ltd.株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ニチレイ' AND c.name = '１社）株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ニチレイ' AND c.name = 'ミーニュー（株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ニチレイ' AND c.name = '持分法適用会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ピックルスホールディングス' AND c.name = '株式会社ベジパル有限会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '片倉コープアグリ株式会社' AND c.name = '片倉上海農業科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ダイセル株式会社' AND c.name = 'パイクリスタル株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ダイセル株式会社' AND c.name = 'ＤＭノバフォーム株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ダイセル株式会社' AND c.name = 'ノバセル株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ダイセル株式会社' AND c.name = 'エボニック株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ダイセル株式会社' AND c.name = 'AG他)および関連会社(豊科フイルム株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ダイセル株式会社' AND c.name = '連結の範囲に含めております。またダイセルパイロテクニクス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ダイセル株式会社' AND c.name = '関係会社の状況」に記載しているため省略しております。株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ダイセル株式会社' AND c.name = 'エボニック株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ダイセル株式会社' AND c.name = 'Ltd.)および関連会社(豊科フイルム株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社フェリシモ' AND c.name = '当社の連結子会社であった株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社フェリシモ' AND c.name = '社連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社フェリシモ' AND c.name = '社持分法適用の関連会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'フタバ産業株式会社' AND c.name = 'FMIオートモーティブコンポーネンツ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'フタバ産業株式会社' AND c.name = 'フタバマニュファクチャリングUK株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'フタバ産業株式会社' AND c.name = 'フタバインダストリアルグジャラート株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'フタバ産業株式会社' AND c.name = 'フタバインディアナアメリカ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'フタバ産業株式会社' AND c.name = 'FIOオートモーティブカナダ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'フタバ産業株式会社' AND c.name = '社国内連結子会社名株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'フタバ産業株式会社' AND c.name = 'フタバインダストリアルテキサス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'フタバ産業株式会社' AND c.name = 'FICアメリカ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'フタバ産業株式会社' AND c.name = '社関連会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'フタバ産業株式会社' AND c.name = '協祥機械工業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'フタバ産業株式会社' AND c.name = '持分法を適用しない関連会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社クリーマ' AND c.name = 'FANTIST可利瑪股份有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社平和' AND c.name = 'ゴルフホールディングス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社平和' AND c.name = 'ＰＧＭプロパティーズ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社平和' AND c.name = 'オリンピアパシフィックゴルフマネージメント株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社平和' AND c.name = 'Investments株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社平和' AND c.name = '社主要な連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社平和' AND c.name = '持分法を適用していない関連会社（株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カナデン' AND c.name = '髙島電機株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カナデン' AND c.name = '科拿電国際貿易（上海）有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カナデン' AND c.name = '科拿電（香港）有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カナデン' AND c.name = 'テクノクリエイト株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カナデン' AND c.name = '当社の連結子会社であった株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カナデン' AND c.name = '関連会社（菱神電子エンジニアリング株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社クラレ' AND c.name = '株式会社岡山臨港及び岡山臨港倉庫運輸株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社クラレ' AND c.name = '社）(主要な会社等の名称)禾欣可楽麗超繊皮(嘉興)有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社クラレ' AND c.name = '株式会社岡山臨港及び岡山臨港倉庫運輸株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'クルーズ株式会社' AND c.name = 'ランク王株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'クルーズ株式会社' AND c.name = 'SHOPLIST株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'クルーズ株式会社' AND c.name = 'Ada株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'クルーズ株式会社' AND c.name = '当連結会計年度からAda株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'クルーズ株式会社' AND c.name = 'ワールドリンク株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'クルーズ株式会社' AND c.name = '社主要な連結子会社の名称496株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'クルーズ株式会社' AND c.name = 'Partners株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'クルーズ株式会社' AND c.name = '社会社等の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'クルーズ株式会社' AND c.name = 'カタリストキャピタル株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ニッピ' AND c.name = '大鳳商事株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ニッピ' AND c.name = '日皮胶原蛋白(唐山)有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ニッピ' AND c.name = '日皮(上海)貿易有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ニッピ' AND c.name = '大倉フーズ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ニッピ' AND c.name = '社主要な連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ニッピ' AND c.name = '日本皮革株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ニッピ' AND c.name = '非連結子会社名ニッピ都市開発株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ニッピ' AND c.name = '社会社等の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ニッピ' AND c.name = '日本皮革株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ニッピ' AND c.name = '持分法を適用しない非連結子会社及び関連会社のうち主要な会社等の名称ニッピ都市開発株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社小糸製作所' AND c.name = 'コイト電工株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社小糸製作所' AND c.name = '竹田サンテック株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'メディアスホールディングス株式会社' AND c.name = '石川医療器株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カヤック' AND c.name = 'コミュニケーションズ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カヤック' AND c.name = 'サンネット株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カヤック' AND c.name = '当社の連結子会社であった株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カヤック' AND c.name = 'の数及び名称連結子会社の数18社連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カヤック' AND c.name = '株式会社琉球カヤックスタジオ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カヤック' AND c.name = '鎌倉自宅葬儀社鎌倉R不動産株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カヤック' AND c.name = 'カヤックポラリス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カヤック' AND c.name = 'SANKO株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カヤック' AND c.name = 'en-zin英治出版株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カヤック' AND c.name = '配信技術研究所株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カヤック' AND c.name = '連結の範囲から除外しております。配信技術研究所株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カヤック' AND c.name = 'eSP株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カヤック' AND c.name = 'アスラフィルムラゾ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カヤック' AND c.name = 'カヤックアキバスタジオ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カヤック' AND c.name = '英治出版株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カヤック' AND c.name = 'カヤックボンド株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カヤック' AND c.name = '株式会社アスラフィルム及びラゾ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カヤック' AND c.name = '当社の連結子会社であるGLOE株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カヤック' AND c.name = 'カヤックゼロ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カヤック' AND c.name = 'プラコレ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カヤック' AND c.name = 'GLOE株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カヤック' AND c.name = 'ゲムトレ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カヤック' AND c.name = '琉球フットボールクラブ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カヤック' AND c.name = 'Picasso株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社トランザクション' AND c.name = 'トレードワークス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社トランザクション' AND c.name = 'トランス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社トランザクション' AND c.name = 'クラフトワーク株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社トランザクション' AND c.name = 'Limited上海多来多貿易有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社トランザクション' AND c.name = '社主な連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '人・夢・技術グループ株式会社' AND c.name = 'rporation台湾長大顧問有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社コア' AND c.name = 'システム株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社コア' AND c.name = 'レゾナ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社コア' AND c.name = 'ギガコアネットインタナショナル株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社コア' AND c.name = 'プロネットコア興産株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社コア' AND c.name = '社連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社コア' AND c.name = '株式会社ラムダシステムズ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社コア' AND c.name = '１社非連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社コア' AND c.name = '社持分法適用会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社コア' AND c.name = '持分法を適用しない非連結子会社及び関連会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社コア' AND c.name = '医療福祉工学研究所株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社コア' AND c.name = '東北情報センター株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ジェイホールディングス' AND c.name = '株式会社ジェイクレスト合同会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社三井E&amp;S' AND c.name = '新日本海重工業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ジェイテック' AND c.name = '社主要な連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ベネフィットジャパン' AND c.name = 'ライフスタイルウォーター株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ベネフィットジャパン' AND c.name = '非連結子会社の名称等非連結子会社株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ベネフィットジャパン' AND c.name = '持分法を適用していない非連結子会社及び関連会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ベネフィットジャパン' AND c.name = '持分法を適用しない関連会社であった株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ベネフィットジャパン' AND c.name = '当連結会計年度において株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ヤマト' AND c.name = '連結範囲の変更）持分法適用関連会社であった上毛建設株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ヤマト' AND c.name = '社関連会社の名称上毛建設株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ヤマト' AND c.name = '持分法適用の範囲の変更）持分法適用関連会社であった上毛建設株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社大和' AND c.name = '金沢都市開発株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社大和' AND c.name = '％以下を自己の計算において所有しているにもかかわらず関連会社としなかった主要な会社等の名称総曲輪シテイ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社大和' AND c.name = 'オタヤ開発株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社石川製作所' AND c.name = '関東航空計器株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社マーキュリアホールディングス' AND c.name = 'Limited互金蘇州投資管理有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ＨＳホールディングス株式会社' AND c.name = 'Kyrgyzkommertsbank)株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'サンユー建設株式会社' AND c.name = '社連結子会社の名称行方建設株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'サンユー建設株式会社' AND c.name = '非連結子会社の名称サンユーエステート株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'サンユー建設株式会社' AND c.name = 'サンユーテクノ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'サンユー建設株式会社' AND c.name = 'サンユーエステート株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ライト工業株式会社' AND c.name = 'らいとケア株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ライト工業株式会社' AND c.name = '西日本リアライズ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ライト工業株式会社' AND c.name = '非連結子会社の名称等株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ライト工業株式会社' AND c.name = 'タフアース株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ライト工業株式会社' AND c.name = '持分法非適用の非連結子会社（株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社大盛工業' AND c.name = '井口建設株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社大盛工業' AND c.name = '港シビル株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社イトーキ' AND c.name = '富士リビング工業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社イトーキ' AND c.name = 'であった株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社イトーキ' AND c.name = 'イトーキ東光製作所イトーキマルイ工業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社イトーキ' AND c.name = '株式会社ダルトン株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社イトーキ' AND c.name = '株式会社イトーキマーケットスペース株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社イトーキ' AND c.name = '社主要な連結子会社の名称伊藤喜オールスチール株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社イトーキ' AND c.name = 'イトーキシェアードバリュー新日本システック株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社イトーキ' AND c.name = '三幸ファシリティーズ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社イトーキ' AND c.name = 'スタッフ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社イトーキ' AND c.name = 'Japan株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社イトーキ' AND c.name = 'Japan株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社光陽社' AND c.name = '株式会社ニコモ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ヤマザキ' AND c.name = '３．持分法の適用に関する事項(1）持分法を適用しない関連会社の名称等HYテクノロジーズ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'イーグル工業株式会社' AND c.name = '重要性が増したため新潟イーグル株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'イーグル工業株式会社' AND c.name = 'JAPAN株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'イーグル工業株式会社' AND c.name = 'JAPAN株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'グロースエクスパートナーズ株式会社' AND c.name = 'アーキテクチャ＆チームス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社石川製作所' AND c.name = '関東航空計器株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'アゼアス株式会社' AND c.name = '株式会社阿茲阿斯大連紡織服飾有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'アゼアス株式会社' AND c.name = '大連保税区日里貿易有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'フロイント産業株式会社' AND c.name = 'Pvt.Ltd.'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ユアサ・フナショク株式会社' AND c.name = 'エフ物流株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ユアサ・フナショク株式会社' AND c.name = 'エージェンシー株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ユアサ・フナショク株式会社' AND c.name = '日本畜産振興株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'アイコム株式会社' AND c.name = 'アイコム情報機器株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'アイコム株式会社' AND c.name = '和歌山アイコム株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'アイコム株式会社' AND c.name = 'ポジション株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シード' AND c.name = '上海実瞳光学科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シード' AND c.name = 'SA（スイス）上海実瞳健康科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シード' AND c.name = '中国）香港実瞳健康科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シード' AND c.name = '上海実瞳商務咨詢有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シード' AND c.name = '上海実瞳視光医療科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シード' AND c.name = '香港実瞳光学科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シード' AND c.name = '横浜近視予防研究所株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シード' AND c.name = '上海実瞳商務咨詢有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シード' AND c.name = '上海実瞳視光医療科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シード' AND c.name = '香港実瞳光学科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シード' AND c.name = '横浜近視予防研究所株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社アイスタイル' AND c.name = 'アイスタイルトレーディング株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社アイスタイル' AND c.name = 'アイスタイルリテール株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社アイスタイル' AND c.name = 'Border株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社アイスタイル' AND c.name = 'me株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社アイスタイル' AND c.name = 'アイスタイルキャリア株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社アイスタイル' AND c.name = 'アイスタイルプロダクツアイスタイルデータコンサルティング株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社アイスタイル' AND c.name = 'グローブ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社アイスタイル' AND c.name = 'ISパートナーズ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社アイスタイル' AND c.name = '新規設立によりアイスタイルデータコンサルティング株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社アイスタイル' AND c.name = 'トレンダーズ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社アイスタイル' AND c.name = 'LiME株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社アイスタイル' AND c.name = '新たに株式を取得したことにより株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ニッスイ' AND c.name = '及び株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ニッスイ' AND c.name = 'GDホールディングス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ニッスイ' AND c.name = 'グルメデリカとの合併に伴い日本クッカリー株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ヤクルト本社' AND c.name = '大船渡ヤクルト販売株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ヤクルト本社' AND c.name = '従来連結子会社であった北京ヤクルト販売株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ヤクルト本社' AND c.name = 'スペインヤクルト株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ヤクルト本社' AND c.name = 'ヤクルトチャイルドサポート株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ヤクルト本社' AND c.name = '韓国ヤクルト株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ヤクルト本社' AND c.name = '持分法を適用していない関連会社の香川ヤクルト販売株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '江崎グリコ株式会社' AND c.name = '当社の持分法適用関連会社であった株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '江崎グリコ株式会社' AND c.name = '江栄商事株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '江崎グリコ株式会社' AND c.name = '非連結子会社（江栄商事株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '江崎グリコ株式会社' AND c.name = '持分法を適用していない非連結子会社（江栄商事株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '江崎グリコ株式会社' AND c.name = '他１社）及び関連会社（関東フローズン株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '森永製菓株式会社' AND c.name = 'バクテクス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '正栄食品工業株式会社' AND c.name = '青島秀愛食品有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '正栄食品工業株式会社' AND c.name = '香港正栄国際貿易有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '正栄食品工業株式会社' AND c.name = '前連結会計年度において連結子会社でありました株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '正栄食品工業株式会社' AND c.name = '筑波乳業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '正栄食品工業株式会社' AND c.name = '延吉秀愛食品有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '正栄食品工業株式会社' AND c.name = '上海秀愛国際貿易有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '正栄食品工業株式会社' AND c.name = '近藤製粉株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '正栄食品工業株式会社' AND c.name = '社主要な会社等の名称近藤製粉株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社紀文食品' AND c.name = '１社株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社紀文食品' AND c.name = '非連結子会社の名称等非連結子会社株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社紀文食品' AND c.name = '持分法を適用していない非連結子会社（株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ハークスレイ' AND c.name = 'マネジメント株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ハークスレイ' AND c.name = 'ほっかほっか亭京滋地区本部株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ハークスレイ' AND c.name = 'C稲葉ピーナツ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ハークスレイ' AND c.name = '味工房スイセン株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ハークスレイ' AND c.name = '株式会社谷貝食品株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ハークスレイ' AND c.name = 'TRNシティパートナーズ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ハークスレイ' AND c.name = 'アイファクトリー株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ハークスレイ' AND c.name = 'ほっかほっか亭総本部株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ハークスレイ' AND c.name = 'メイト店舗流通ネット株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ハークスレイ' AND c.name = 'Management株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ハークスレイ' AND c.name = 'Career株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ハークスレイ' AND c.name = '株式会社鹿児島食品サービス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ハークスレイ' AND c.name = 'トーヨー株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'クワザワホールディングス株式会社' AND c.name = 'ステアーズ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'クワザワホールディングス株式会社' AND c.name = 'クワザワ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'クワザワホールディングス株式会社' AND c.name = 'クワザワ工業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'クワザワホールディングス株式会社' AND c.name = '住まいのクワザワ丸三商事株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'クワザワホールディングス株式会社' AND c.name = '社主要な連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'クワザワホールディングス株式会社' AND c.name = '和光クリーン株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'クワザワホールディングス株式会社' AND c.name = '非連結子会社名日桑建材株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'クワザワホールディングス株式会社' AND c.name = '恵庭アサノコンクリート株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'クワザワホールディングス株式会社' AND c.name = '社会社等の名称北海道管材株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'クワザワホールディングス株式会社' AND c.name = '和光クリーン株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'クワザワホールディングス株式会社' AND c.name = '恵庭アサノコンクリート株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'クワザワホールディングス株式会社' AND c.name = '大野アサノコンクリート株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'クワザワホールディングス株式会社' AND c.name = '持分法を適用しない非連結子会社及び関連会社のうち主要な会社等の名称日桑建材株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社フェイスネットワーク' AND c.name = 'の数1社連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社フェイスネットワーク' AND c.name = 'フェイスFPサロン株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社フェイスネットワーク' AND c.name = '非連結子会社名FAITHアセットマネジメント株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社フェイスネットワーク' AND c.name = 'フェイスプロパティーズ合同会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社フェイスネットワーク' AND c.name = 'フェイスプロパティーズ合同会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社フェイスネットワーク' AND c.name = 'フェイスFPサロン株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社フェイスネットワーク' AND c.name = '持分法を適用しない非連結子会社非連結子会社の名称FAITHアセットマネジメント株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'フジ住宅株式会社' AND c.name = '雄健建設株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'フジ住宅株式会社' AND c.name = 'アメニティサービス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'フジ住宅株式会社' AND c.name = '雄健建設株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'フジ住宅株式会社' AND c.name = 'アメニティサービス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'フジ住宅株式会社' AND c.name = '関西電設工業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社キャンディル' AND c.name = '連結の範囲に関する事項すべての子会社を連結しております。連結子会社の数4社連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '京阪ホールディングス株式会社' AND c.name = '中之島高速鉄道株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '京阪ホールディングス株式会社' AND c.name = '株式会社京阪ビジネスマネジメント等非連結子会社及び株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '積水化学工業株式会社' AND c.name = '四積化工株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '積水化学工業株式会社' AND c.name = '甲府積水産業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '積水化学工業株式会社' AND c.name = '積水ソーラーフィルム株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '積水化学工業株式会社' AND c.name = '東積加工株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '積水化学工業株式会社' AND c.name = 'セキスイハイムクリエイト株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '積水化学工業株式会社' AND c.name = '社主要な会社名積水化成品工業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '積水化学工業株式会社' AND c.name = '持分法を適用しない主要な会社名等持分法非適用の非連結子会社（セキスイハイムクリエイト株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '積水化学工業株式会社' AND c.name = '他）及び関連会社（株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社フージャースホールディングス' AND c.name = 'フージャースコーポレーション株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社フージャースホールディングス' AND c.name = 'フージャースリビングサービス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社フージャースホールディングス' AND c.name = 'フージャースケアデザイン株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社フージャースホールディングス' AND c.name = 'フージャースアセットマネジメント株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社フージャースホールディングス' AND c.name = '５社主要な非連結子会社の名称新富士見ＰＦＩ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社フージャースホールディングス' AND c.name = '原山公園ＰＦＩ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社フージャースホールディングス' AND c.name = '大津学校給食ＰＦＩ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社フージャースホールディングス' AND c.name = '原山公園ＰＦＩ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社フージャースホールディングス' AND c.name = '新富士見ＰＦＩ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社フージャースホールディングス' AND c.name = '大津学校給食ＰＦＩ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社フージャースホールディングス' AND c.name = '湖北斎場ＰＦＩ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '平和不動産株式会社' AND c.name = '社連結子会社の名称平和不動産プロパティマネジメント株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '平和不動産株式会社' AND c.name = '株式会社東京証券会館東京日比谷ホテル株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '平和不動産株式会社' AND c.name = '東京日本橋兜町ホテル株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '平和不動産株式会社' AND c.name = '平和不動産アセットマネジメント株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '平和不動産株式会社' AND c.name = 'ハウジングサービス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社テーオーシー' AND c.name = 'TORアセットインベストメント株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社テーオーシー' AND c.name = 'テーオーリネンサプライ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社テーオーシー' AND c.name = 'テーオーシーサプライ星製薬株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社テーオーシー' AND c.name = 'TOCディレクション株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社テーオーシー' AND c.name = '社連結子会社名株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社テーオーシー' AND c.name = '株式会社I-TINK株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社テーオーシー' AND c.name = '非連結子会社の名称等株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社テーオーシー' AND c.name = '社会社の名称大崎再開発ビル株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社テーオーシー' AND c.name = '持分法を適用しない非連結子会社及び関連会社のうち主要な会社等の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エリアクエスト' AND c.name = '子会社は全て連結しております。当該連結子会社は株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エリアクエスト' AND c.name = 'エリアクエスト不動産コンサルティング及び株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ピアラ' AND c.name = '比智(杭州)商貿有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ピアラ' AND c.name = '前連結会計年度において連結子会社でありました台湾比智商貿股フン有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ピアラ' AND c.name = '前連結会計年度において連結子会社でありました株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ピアラ' AND c.name = 'move株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社北の達人コーポレーション' AND c.name = '年７月31日付で株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社北の達人コーポレーション' AND c.name = '同社が株式を保有していた株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社北の達人コーポレーション' AND c.name = 'であった株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社中電工' AND c.name = '非連結子会社名は次のとおり。株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社中電工' AND c.name = 'PFI学校空調東広島株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社中電工' AND c.name = 'Cインベストメント株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社中電工' AND c.name = 'OCソーラー株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社中電工' AND c.name = '幸栄電設株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社中電工' AND c.name = 'PFI学校空調東広島株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社中電工' AND c.name = 'PFI学校空調やまぐち株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社中電工' AND c.name = '三和電気工事株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社中電工' AND c.name = 'PFI学校空調周南株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社中電工' AND c.name = 'PFI学校空調三原株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ダイセキ' AND c.name = '株式会社ダイセキ環境ソリューション株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ダイセキ' AND c.name = '社連結子会社の名称北陸ダイセキ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ダイセキ' AND c.name = '株式会社グリーンアローズ中部株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ダイセキ' AND c.name = '杉本商事有限会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ダイセキ' AND c.name = 'ダイセキＭＣＲシステム機工株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ダイセキ' AND c.name = 'グリーンアローズ九州株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ダイセキ' AND c.name = '２．持分法の適用に関する事項持分法を適用していない関連会社(株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ミダックホールディングス' AND c.name = 'ミダックライナー株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ミダックホールディングス' AND c.name = 'ミダックこなん遠州砕石株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ミダックホールディングス' AND c.name = 'ミダック株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ミダックホールディングス' AND c.name = '三晃株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ミダックホールディングス' AND c.name = '社連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ミダックホールディングス' AND c.name = 'ＮＥＩＧＨＢＯＲ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ミダックホールディングス' AND c.name = '持分法適用の関連会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ミダックホールディングス' AND c.name = 'ＮＥＩＧＨＢＯＲ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ゼット株式会社' AND c.name = '株式会社ジャスプロ広州捷多商貿有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社南陽' AND c.name = 'ピイ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社南陽' AND c.name = 'AQUAPASS株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社南陽' AND c.name = '南陽レンテック株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社南陽' AND c.name = '株式会社戸髙製作所株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社南陽' AND c.name = '南陽重車輌共栄通信工業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社南陽' AND c.name = '浜村南央国際貿易（上海）有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社南陽' AND c.name = '持分法を適用した非連結子会社名及び関連会社名建南和股份有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ジェイエスエス' AND c.name = '年５月31日に株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ダブルエー' AND c.name = '江蘇京海服装貿易有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社トミタ' AND c.name = '広州富田貿易有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社トミタ' AND c.name = 'トミタファミリー有限会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社トミタ' AND c.name = '非連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社トミタ' AND c.name = '持分法を適用しない非連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社トミタ' AND c.name = 'トミタファミリー有限会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エス・エム・エス' AND c.name = '社関連会社の名称エムスリーキャリア株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社IC' AND c.name = '社連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社IC' AND c.name = 'ラボラトリ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '神鋼商事株式会社' AND c.name = '連結の範囲に含めております。神商精密株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '神鋼商事株式会社' AND c.name = '省略しております。日本グラニュレーター株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '神鋼商事株式会社' AND c.name = '大阪精工株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '神鋼商事株式会社' AND c.name = '日本スタッドウェルディング株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '神鋼商事株式会社' AND c.name = '社主要な会社名アジア化工株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社モルフォ' AND c.name = 'PUX株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ミクニ' AND c.name = '当連結会計年度では連結子会社であった成都三国機械電子有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ミクニ' AND c.name = 'ケイ精密株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ミクニ' AND c.name = 'ケイ精密株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ミクニ' AND c.name = '持分法を適用していない非連結子会社（株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ミクニ' AND c.name = 'ミクニザイマス）及び関連会社（三國リビングサービス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社トクヤマ' AND c.name = '省略しております。前連結会計年度まで連結子会社であった新第一塩ビ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社トクヤマ' AND c.name = '愛研徳医療器械（蘇州）有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社トクヤマ' AND c.name = 'トックス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社トクヤマ' AND c.name = '三井化学東セロ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社トクヤマ' AND c.name = '年4月1日付でアールエム東セロ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社トクヤマ' AND c.name = '持分法を適用していない非連結子会社（愛研徳医療器械（蘇州）有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社トクヤマ' AND c.name = 'および関連会社（大分鉱業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ソニーフィナンシャルグループ株式会社' AND c.name = 'コミュニケーションズ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ソニーフィナンシャルグループ株式会社' AND c.name = 'プラウドライフ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ソニーフィナンシャルグループ株式会社' AND c.name = '社会社名ソニー生命保険株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ソニーフィナンシャルグループ株式会社' AND c.name = 'ライフケアデザイン株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ソニーフィナンシャルグループ株式会社' AND c.name = 'ソニー損害保険株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ソニーフィナンシャルグループ株式会社' AND c.name = 'ソニーフィナンシャルベンチャーズ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ソニーフィナンシャルグループ株式会社' AND c.name = 'ライフケア株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ソニーフィナンシャルグループ株式会社' AND c.name = 'ソニー銀行株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ソニーフィナンシャルグループ株式会社' AND c.name = 'ソニーペイメントサービス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ソニーフィナンシャルグループ株式会社' AND c.name = 'ETCソリューションズ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ソニーフィナンシャルグループ株式会社' AND c.name = 'ホールディング株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '長瀬産業株式会社' AND c.name = '非連結子会社の名称等長興株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '長瀬産業株式会社' AND c.name = '無錫澄泓微電子材料有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '長瀬産業株式会社' AND c.name = '長興株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '長瀬産業株式会社' AND c.name = '長瀬欧積有色化学（上海）有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社伸和ホールディングス' AND c.name = '社連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '清水建設 株式会社' AND c.name = '非連結子会社（丸彦商事株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社メイコー' AND c.name = '名幸電子(広州南沙)有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社メイコー' AND c.name = 'メイコーエレクマニュファクチャー株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社メイコー' AND c.name = '広州市斯皮徳貿易有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社メイコー' AND c.name = '山形メイコー株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社メイコー' AND c.name = '宮城メイコー株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社メイコー' AND c.name = '名幸電子香港有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社メイコー' AND c.name = '名幸電子(武漢)有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社メイコー' AND c.name = 'メイコーテック株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社メイコー' AND c.name = '社連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社メイコー' AND c.name = 'メイコーテクノメイコーエレクディベロップ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社インサイト' AND c.name = 'ＭＫデルタ山田プライド株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社インサイト' AND c.name = 'インベスト株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社インサイト' AND c.name = '１）連結子会社の数5社（２）連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社インサイト' AND c.name = 'ＭＫガンマ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '明和産業株式会社' AND c.name = '持分法適用会社数はクミ化成株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '明和産業株式会社' AND c.name = '持分法を適用した関連会社数3社主要な会社等の名称クミ化成株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '明和産業株式会社' AND c.name = '株式会社鈴裕化学クミ化成株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '明和産業株式会社' AND c.name = '当該６社の損益をクミ化成株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社テクノフレックス' AND c.name = 'ニトックス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社テクノフレックス' AND c.name = '天孚真空機器軟管（上海）有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社テクノフレックス' AND c.name = '天津天富軟管工業有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社テクノフレックス' AND c.name = '社主要な連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社丸山製作所' AND c.name = 'LTD.株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社丸山製作所' AND c.name = 'ファインバブルテック株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社丸山製作所' AND c.name = 'LTD.株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社丸山製作所' AND c.name = 'ファインバブルテック株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社丸山製作所' AND c.name = 'LTD.株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社丸山製作所' AND c.name = 'ファインバブルテック株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社丸山製作所' AND c.name = 'LTD.株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社丸山製作所' AND c.name = 'ファインバブルテック株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社オプティム' AND c.name = 'デジタルコンストラクション株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社オプティム' AND c.name = 'バンクテクノロジーズ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社オプティム' AND c.name = '社主要な連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社オプティム' AND c.name = 'ユラスコア株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社オプティム' AND c.name = 'デジタルトランスフォーメーションファンド投資事業有限責任組合第１号株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社オプティム' AND c.name = 'TechnologyDXGoGo株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社オプティム' AND c.name = '社ディピューラメディカルソリューションズ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社 協和コンサルタンツ' AND c.name = 'FSK人材育成株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社モリタホールディングス' AND c.name = '康鴻森田香港有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社モリタホールディングス' AND c.name = '南京晨光森田環保科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '藤井産業株式会社' AND c.name = 'ショーエイ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '藤井産業株式会社' AND c.name = '藤和コンクリート圧送株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '藤井産業株式会社' AND c.name = 'コマツ栃木株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '藤井産業株式会社' AND c.name = 'タロトデンキ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '藤井産業株式会社' AND c.name = '社栃木小松フォークリフト株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カクヤス' AND c.name = '明和物産株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シンシア' AND c.name = '社連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シンシア' AND c.name = 'Ltd.新視野光學股份有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シンシア' AND c.name = '株式会社ジェネリックコーポレーション株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社フジックス' AND c.name = '常州英富紡織有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社フジックス' AND c.name = '富士克國際(香港)有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社フジックス' AND c.name = '上海福拓線貿易有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社フジックス' AND c.name = '上海富士克制線有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社フジックス' AND c.name = '上海新富士克制線有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'インフォメティス株式会社' AND c.name = '持分法を適用した関連会社数1社会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '大同工業株式会社' AND c.name = '非連結子会社名株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '大同工業株式会社' AND c.name = '翔研工業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '大同工業株式会社' AND c.name = '大同テクノダイド建設株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '大同工業株式会社' AND c.name = '及び関連会社４社（株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '大同工業株式会社' AND c.name = '翔研工業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '大同工業株式会社' AND c.name = '非連結子会社２社（ダイド建設株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '大同工業株式会社' AND c.name = 'F2テクノ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '日本光電工業株式会社' AND c.name = 'アドテック株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '日本光電工業株式会社' AND c.name = '社日本光電富岡株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '日本光電工業株式会社' AND c.name = '取得による企業結合によりニューロアドバンスド株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社テンダ' AND c.name = '及び株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社テンダ' AND c.name = '三友テクノロジー株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社テンダ' AND c.name = '株式会社テンダゲームス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社テンダ' AND c.name = 'の数6社連結子会社の名称大連天達科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社テンダ' AND c.name = 'であったリーサコンサルティング株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社テンダ' AND c.name = 'インテリジェントシステムズ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社テンダ' AND c.name = 'Skyartsインテリジェントシステムズ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社テンダ' AND c.name = '沈陽邦友科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社テンダ' AND c.name = '非連結子会社名沈陽邦友科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社テンダ' AND c.name = '２．持分法の適用に関する事項持分法を適用しない非連結子会社の名称沈陽邦友科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社テンダ' AND c.name = '持分法を適用しない理由沈陽邦友科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ナガセ' AND c.name = '社主要な連結子会社名株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ナガセ' AND c.name = '永瀬商貿（上海）有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ナガセ' AND c.name = 'INC.）及び関連会社（株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ナガセ' AND c.name = '私立学校奨学支援保険サービス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社　長谷工コーポレーション' AND c.name = '株式会社長谷工コミュニティ九州株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社　長谷工コーポレーション' AND c.name = '不二建設株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社　長谷工コーポレーション' AND c.name = 'Inc.株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社　長谷工コーポレーション' AND c.name = '総合地所株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社　長谷工コーポレーション' AND c.name = '長谷工コミュニティ西日本株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社　長谷工コーポレーション' AND c.name = '株式会社長谷工ナヴィエ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社　長谷工コーポレーション' AND c.name = '持分法を適用しない非連結子会社及び関連会社のうち主要な会社名持分法非適用の主要な非連結子会社株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社　長谷工コーポレーション' AND c.name = '長谷工ナヴィエ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社日水コン' AND c.name = 'Consultants株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社日水コン' AND c.name = '社連結子会社の名称砂防エンジニアリング株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社日水コン' AND c.name = '１社非連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社日水コン' AND c.name = '社会社等の名称瀾寧管道（上海）有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社日水コン' AND c.name = '持分法を適用していない非連結子会社（株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '富士電機株式会社' AND c.name = '富士電機ＩＴセンター株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '富士電機株式会社' AND c.name = '非連結子会社（富士グリーンパワー株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '富士電機株式会社' AND c.name = '富士ファーマナイト株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '富士電機株式会社' AND c.name = 'メタウォーター株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '富士電機株式会社' AND c.name = 'メタウォーターサービス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '富士電機株式会社' AND c.name = '持分法を適用していない非連結子会社及び関連会社（株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社技術承継機構' AND c.name = '東洋マークFAシンカテクノロジー株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社技術承継機構' AND c.name = 'ティオック株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社技術承継機構' AND c.name = 'エムエスシー製造株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社技術承継機構' AND c.name = '株式会社篠原製作所京和精工株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社技術承継機構' AND c.name = '株式会社キンポーメルテック株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社技術承継機構' AND c.name = 'エアロクラフトジャパン株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社技術承継機構' AND c.name = '天鳥株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社技術承継機構' AND c.name = 'LTD.株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社マネジメントソリューションズ' AND c.name = 'Digital株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社マネジメントソリューションズ' AND c.name = 'コミュニケーションズ麦嵩隆管理咨洵(上海)有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社マネジメントソリューションズ' AND c.name = 'すべての子会社を連結しております。連結子会社の数4社連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ヨロズ' AND c.name = 'オグラオートモーティブタイランド社ヨロズエンジニアリングシステムズタイランド社广州萬宝井汽車部件有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ヨロズ' AND c.name = 'ヨロズ栃木株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ヨロズ' AND c.name = '武漢萬宝井汽車部件有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ヨロズ' AND c.name = 'ヨロズエンジニアリング株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ヨロズ' AND c.name = 'ヨロズ大分株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ヨロズ' AND c.name = '株式会社庄内ヨロズ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '中央倉庫株式会社' AND c.name = '社：中倉陸運株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '中央倉庫株式会社' AND c.name = '中央倉庫ワークス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '中央倉庫株式会社' AND c.name = '社：ユーシーエス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '中央倉庫株式会社' AND c.name = '持分法適用関連会社であった株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '中央倉庫株式会社' AND c.name = '安田中倉国際貨運代理（上海）有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エストラスト' AND c.name = '建和住宅株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エストラスト' AND c.name = 'オリエルホーム株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エーアイ' AND c.name = '株式会社スーパーワンは株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エーアイ' AND c.name = '株式会社スーパーワン株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ジャックス' AND c.name = 'ジャックスリース株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ジャックス' AND c.name = 'ジャックス債権回収サービス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ジャックス' AND c.name = 'サービス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '旭化成株式会社' AND c.name = '非連結子会社の名称等主要な非連結子会社……旭化成ネットワークス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '旭化成株式会社' AND c.name = '社主要な会社名……旭化成ネットワークス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '旭化成株式会社' AND c.name = '社主要な会社名……旭有機材株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '旭化成株式会社' AND c.name = 'Inc.等)及び関連会社(南陽化成株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ビジョン' AND c.name = '非連結子会社の名称等主要な非連結子会社ビジョンベンチャーズ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ビジョン' AND c.name = '社持分法を適用する関連会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社リプライオリティ' AND c.name = '連結の範囲に関する事項すべての子会社を連結しております。連結子会社の数1社連結子会社の名称日本ウェルネス研究所株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社クレオ' AND c.name = 'ブライエ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社クレオ' AND c.name = 'ココト株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ルネサンス' AND c.name = '連結の範囲に含めておりました株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社やまびこ' AND c.name = 'やまびこエンジニアリング株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社やまびこ' AND c.name = '双伸工業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社やまびこ' AND c.name = '新大華機械股份有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社やまびこ' AND c.name = '追浜工業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社やまびこ' AND c.name = '社連結子会社の名称やまびこジャパン株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社やまびこ' AND c.name = '愛可機械（深圳）有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社やまびこ' AND c.name = 'エコー産業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社やまびこ' AND c.name = '社会社等の名称寧波奥浜動力科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '湖北工業株式会社' AND c.name = 'SDN.BHD.東莞瑚北電子有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '湖北工業株式会社' AND c.name = '蘇州瑚北光電子有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '湖北工業株式会社' AND c.name = 'エピフォトニクス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '湖北工業株式会社' AND c.name = 'LTD.エピフォトニクス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ブラス' AND c.name = 'lyrics株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ブラス' AND c.name = 'アロウブライト株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ブラス' AND c.name = 'INC．株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '日本興業株式会社' AND c.name = '社(2）連結子会社の名称ニッコーエクステリア株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '日本興業株式会社' AND c.name = '株式会社サンキャリー葉月工業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'シマダヤ株式会社' AND c.name = 'シマダヤ西日本株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'シマダヤ株式会社' AND c.name = 'シマダヤ東北株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'シマダヤ株式会社' AND c.name = 'シマダヤ関東株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'シマダヤ株式会社' AND c.name = 'シマダヤ商事株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ナガホリ' AND c.name = '長堀（香港）有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ナガホリ' AND c.name = 'ナガホリリテール株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ナガホリ' AND c.name = 'エスジェイジュエリー株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ナガホリ' AND c.name = '社ソマ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ファースト住建株式会社' AND c.name = '住宅の横綱大和建設株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ファースト住建株式会社' AND c.name = '株式取得により株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ファースト住建株式会社' AND c.name = 'パル建設株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ファースト住建株式会社' AND c.name = '株式会社Ｌａｂｏ及びいい不動産プラザ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ファースト住建株式会社' AND c.name = 'アオイ建設株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ファースト住建株式会社' AND c.name = 'いい不動産プラザ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ファースト住建株式会社' AND c.name = 'ファースト工務店株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ファースト住建株式会社' AND c.name = 'リタ総合不動産株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ファースト住建株式会社' AND c.name = '持分法を適用していない非連結子会社（ファースト工務店株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ファースト住建株式会社' AND c.name = '有限会社アオイ設計事務所及びリタ総合不動産株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シーイーシー' AND c.name = 'シーイーシー(上海)信息系統有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シーイーシー' AND c.name = '社連結子会社の名称フォーサイトシステム株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シーイーシー' AND c.name = '株式会社宮崎太陽農園株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シーイーシー' AND c.name = 'シーイーシークロスメディア株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シーイーシー' AND c.name = '株式会社シーイーシーカスタマサービス大分シーイーシー株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '北陸電気工業株式会社' AND c.name = '北陸興産株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '北陸電気工業株式会社' AND c.name = '北陸興産株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '北陸電気工業株式会社' AND c.name = '北陸興産株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '朝日印刷株式会社' AND c.name = '朝日印刷ビジネスサポート株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '朝日印刷株式会社' AND c.name = 'Pte.Ltd.'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '朝日印刷株式会社' AND c.name = '朝日印刷ビジネスサポート株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '朝日印刷株式会社' AND c.name = 'Pte.Ltd.'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ジーエルテクノホールディングス株式会社' AND c.name = '杭州泰谷諾石英有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ジーエルテクノホールディングス株式会社' AND c.name = '技尓上海商貿有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カナデン' AND c.name = '髙島電機株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カナデン' AND c.name = '科拿電国際貿易（上海）有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カナデン' AND c.name = '科拿電（香港）有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カナデン' AND c.name = 'テクノクリエイト株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カナデン' AND c.name = '当社の連結子会社であった株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社カナデン' AND c.name = '関連会社（菱神電子エンジニアリング株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ナガオカ' AND c.name = 'LTD.矢澤フェロマイト株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社ナガオカ' AND c.name = '社連結子会社の名称那賀設備（大連）有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '蝶理株式会社' AND c.name = '株式会社小桜商会蝶理GLEX株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '蝶理株式会社' AND c.name = '以下のとおりであります。(会社名)株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '蝶理株式会社' AND c.name = 'アサダユウミヤコ化学株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '蝶理株式会社' AND c.name = '蝶理マシナリー株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '蝶理株式会社' AND c.name = 'INC.蝶理(中国)商業有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '蝶理株式会社' AND c.name = 'ＳＴＸ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '旭有機材株式会社' AND c.name = 'アビトップ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '旭有機材株式会社' AND c.name = 'ドリコウェルテクノ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '太陽ホールディングス株式会社' AND c.name = '重要性が増加した株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '太陽ホールディングス株式会社' AND c.name = 'リック（現：株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '太陽ホールディングス株式会社' AND c.name = '新たに設立したTGE水上ソーラー1号合同会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社トクヤマ' AND c.name = '省略しております。前連結会計年度まで連結子会社であった新第一塩ビ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社トクヤマ' AND c.name = '愛研徳医療器械（蘇州）有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社トクヤマ' AND c.name = 'トックス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社トクヤマ' AND c.name = '三井化学東セロ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社トクヤマ' AND c.name = '年4月1日付でアールエム東セロ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社トクヤマ' AND c.name = '持分法を適用していない非連結子会社（愛研徳医療器械（蘇州）有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社トクヤマ' AND c.name = 'および関連会社（大分鉱業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '小松マテーレ株式会社' AND c.name = '小松美特料蘇州貿易有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '丸尾カルシウム株式会社' AND c.name = '株式会社丸尾上海貿易有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '丸尾カルシウム株式会社' AND c.name = '東莞立丸奈米科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社アテクト' AND c.name = '安泰科科技股份有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '神東塗料株式会社' AND c.name = 'システムズ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '東洋ドライルーブ株式会社' AND c.name = '広州徳来路博科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '東洋ドライルーブ株式会社' AND c.name = '中山市三民金属処理有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '東洋ドライルーブ株式会社' AND c.name = '昆山三民塗頼表面処理技術有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '大日本塗料株式会社' AND c.name = 'Indonesia迪恩特塗料浙江有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '大日本塗料株式会社' AND c.name = '株式会社神東艾仕得塗料系統股份有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シーボン' AND c.name = '社連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シーボン' AND c.name = 'ジャフマック倩朋（上海）化粧品有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シーボン' AND c.name = '株式会社クリニメディック株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'プレミアアンチエイジング株式会社' AND c.name = 'ベイ安美上海化粧品有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'プレミアアンチエイジング株式会社' AND c.name = '威耐可适商ボウ北京有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '太陽化学株式会社' AND c.name = '上海太陽食研国際貿易有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '太陽化学株式会社' AND c.name = 'マーケティング株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '太陽化学株式会社' AND c.name = 'タイヨーインタコリアリミテッドタイヨーカガクインディアプライベイトリミテッド株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '太陽化学株式会社' AND c.name = '社連結子会社の名称タイヨーインタナショナルインク開封太陽金明食品有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '太陽化学株式会社' AND c.name = '香奈維斯（天津）食品有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '太陽化学株式会社' AND c.name = '無錫太陽緑宝科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社日本ピグメントホールディングス' AND c.name = '上海新素材特種聚合物有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社日本ピグメントホールディングス' AND c.name = '大恭化學工業股份有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ＵＢＥ株式会社' AND c.name = 'UBE三菱セメント株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '松井建設株式会社' AND c.name = '松井リフォーム株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '松井建設株式会社' AND c.name = 'すべての子会社（2社）を連結している。連結子会社名松友商事株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '松井建設株式会社' AND c.name = 'いなぎ文化センターサービス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シード' AND c.name = '上海実瞳光学科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シード' AND c.name = 'SA（スイス）上海実瞳健康科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シード' AND c.name = '中国）香港実瞳健康科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シード' AND c.name = '上海実瞳商務咨詢有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シード' AND c.name = '上海実瞳視光医療科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シード' AND c.name = '香港実瞳光学科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シード' AND c.name = '横浜近視予防研究所株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シード' AND c.name = '上海実瞳商務咨詢有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シード' AND c.name = '上海実瞳視光医療科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シード' AND c.name = '香港実瞳光学科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社シード' AND c.name = '横浜近視予防研究所株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = 'カピタルバリアブレ偉福科技工業(中山)有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '株式会社九州エフテック株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '社フクダエンジニアリング株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '偉福科技工業(武漢)有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '偉福(広州)汽車技術開発有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '煙台福研模具有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = 'インドネシア城南武漢科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '株式会社城南九州製作所城南佛山科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = 'カピタルバリアブレ偉福科技工業(中山)有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '株式会社九州エフテック株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '社フクダエンジニアリング株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '偉福科技工業(武漢)有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '偉福(広州)汽車技術開発有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '煙台福研模具有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = 'カピタルバリアブレ偉福科技工業(中山)有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '株式会社九州エフテック株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '社フクダエンジニアリング株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '偉福科技工業(武漢)有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '偉福(広州)汽車技術開発有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '煙台福研模具有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = 'インドネシア城南武漢科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社エフテック' AND c.name = '株式会社城南九州製作所城南佛山科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社守谷商会' AND c.name = '機材サービス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社守谷商会' AND c.name = '未来ネットワーク株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社守谷商会' AND c.name = 'アスペック丸善土木株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社守谷商会' AND c.name = '守谷不動産株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社守谷商会' AND c.name = '社連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '太平洋工業株式会社' AND c.name = '天津太平洋汽車部件有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '太平洋工業株式会社' AND c.name = '太平洋産業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '太平洋工業株式会社' AND c.name = 'LTD.太平洋バルブ工業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '太平洋工業株式会社' AND c.name = '太平洋エアコントロール工業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '太平洋工業株式会社' AND c.name = '国内子会社)ピーアイシステム株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '太平洋工業株式会社' AND c.name = 'NV/SA太平洋汽門工業股份有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '太平洋工業株式会社' AND c.name = '太平洋汽車部件科技（常熟）有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '太平洋工業株式会社' AND c.name = '長沙太平洋半谷汽車部件有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '太平洋工業株式会社' AND c.name = '会社等の名称PECホールディングス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社セレス' AND c.name = 'ゆめみ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社セレス' AND c.name = 'ディアナstudio15株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社セレス' AND c.name = 'マーキュリー株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社セレス' AND c.name = '連結子会社の数9社連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社セレス' AND c.name = '株式会社ラボル株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社セレス' AND c.name = 'バッカス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社セレス' AND c.name = 'サルース株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社セレス' AND c.name = '持分法を適用した関連会社の数1社持分法適用会社の名称ビットバンク株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社スズケン' AND c.name = '持分法を適用しない理由ＥＰＳ益新株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社スズケン' AND c.name = '持分法非適用の関連会社の名称ＥＰＳ益新株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社九電工として「定款一部変更の件」を付議しており、当該議案が承認可決されると、 株式会社クラフティア新英訳名 KRAFTIA CORPORATION' AND c.name = '円賀工業株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社 吉野家ホールディングス' AND c.name = '吉野家中国投資有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社 吉野家ホールディングス' AND c.name = '深圳吉野家快餐有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '神栄株式会社' AND c.name = '神栄テクノロジー株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '神栄株式会社' AND c.name = '神栄商事（青島）貿易有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '神栄株式会社' AND c.name = '神栄ホームクリエイト株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '神栄株式会社' AND c.name = '神栄リビングインダストリー株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '神栄株式会社' AND c.name = '神栄キャパシタ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '神栄株式会社' AND c.name = '関西通商株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '神栄株式会社' AND c.name = '関西通商株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ヤマト モビリティ ＆ Mfg.株式会社' AND c.name = '株式会社香港大和工貿有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ヤマト モビリティ ＆ Mfg.株式会社' AND c.name = '大和高精密工業深圳有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ヤマト モビリティ ＆ Mfg.株式会社' AND c.name = '亜禡特貿易上海有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社安永' AND c.name = '米国上海安永精密切割機有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '有限会社久世' AND c.name = '久世香港有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '有限会社久世' AND c.name = '上海日生食品物流有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '有限会社久世' AND c.name = '久華世成都商貿有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '戸田建設株式会社' AND c.name = 'アグリゲーション株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '戸田建設株式会社' AND c.name = '新たに設立した戸田建設不動産投資顧問株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '戸田建設株式会社' AND c.name = 'Ltd.及び株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '戸田建設株式会社' AND c.name = 'ヒューマンコミュニティサービス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '戸田建設株式会社' AND c.name = 'Construction株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '戸田建設株式会社' AND c.name = 'ヒューマンコミュニティサービス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '戸田建設株式会社' AND c.name = '持分法非適用の関連会社名株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ＤＭ三井製糖ホールディングス株式会社' AND c.name = 'DM三井製糖株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '第一実業株式会社' AND c.name = 'ウエイブエンジニアリング及び当社の非連結子会社であった株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '第一実業株式会社' AND c.name = '同日付で第一エンジニアリング株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '第一実業株式会社' AND c.name = '当社の連結子会社であった株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '第一実業株式会社' AND c.name = '当社の非連結子会社であった第一エンジニアリング株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '第一実業株式会社' AND c.name = 'DJ-WAVEエンジニアリングへ商号変更しております。この組織再編により株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '第一実業株式会社' AND c.name = '一實股份有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '第一実業株式会社' AND c.name = '非連結子会社の名称プラントデジタルエックス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '第一実業株式会社' AND c.name = '持分法を適用しない非連結子会社及び関連会社のうち主要な会社等の名称非連結子会社プラントデジタルエックス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '第一実業株式会社' AND c.name = '一實股份有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '第一実業株式会社' AND c.name = '関連会社第一スルザー株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '株式会社秋川牧園' AND c.name = '株式会社ゆめファーム秋川牧園常州農業有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ＳＧホールディングス株式会社' AND c.name = '年４月１日付で名糖運輸株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ＳＧホールディングス株式会社' AND c.name = 'ＳＧフィルダー株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ＳＧホールディングス株式会社' AND c.name = 'ＳＧムービング株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ＳＧホールディングス株式会社' AND c.name = 'INC.上海虹迪物流科技有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ＳＧホールディングス株式会社' AND c.name = '株式会社ヒューテックノオリンＳＧリアルティ株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ＳＧホールディングス株式会社' AND c.name = 'ＳＧシステム株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ＳＧホールディングス株式会社' AND c.name = '社主要な連結子会社の名称佐川急便株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ＳＧホールディングス株式会社' AND c.name = '名糖運輸株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ＳＧホールディングス株式会社' AND c.name = '佐川アドバンス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ＳＧホールディングス株式会社' AND c.name = '株式会社ワールドサプライ佐川グローバルロジスティクス株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ＳＧホールディングス株式会社' AND c.name = '佐川ヒューモニー株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ＳＧホールディングス株式会社' AND c.name = 'ＳＧモータース株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ＳＧホールディングス株式会社' AND c.name = '株式会社Ｃ＆Ｆロジホールディングス名糖運輸株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ＳＧホールディングス株式会社' AND c.name = 'ジャパン株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ＳＧホールディングス株式会社' AND c.name = '社主要な会社の名称国家能源集団格尓木光伏発電有限公司'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ＳＧホールディングス株式会社' AND c.name = '直販配送株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'ＳＧホールディングス株式会社' AND c.name = '持分法を適用しない関連会社の名称等株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '佐田建設株式会社' AND c.name = '社連結子会社の名称佐田道路株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '佐田建設株式会社' AND c.name = '株式会社島田組株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '佐田建設株式会社' AND c.name = 'リフォーム群馬彩光建設株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'アプライド株式会社' AND c.name = '社連結子会社の名称株式会社'
ON CONFLICT DO NOTHING;

INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = 'アプライド株式会社' AND c.name = 'シティ情報ふくおか株式会社'
ON CONFLICT DO NOTHING;
