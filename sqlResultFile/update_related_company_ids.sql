-- 関連企業ID集約用SQL


UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('大英リビングサポート株式会社')
)
WHERE name = '大英産業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('煙台三拓進和撹拌設備維修有限公司', '煙台進和接合技術有限公司', '自動化控制設備有限公司', '貿易有限公司')
)
WHERE name = '株式会社進和';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('メディア株式会社', 'およびヤマトエナジーマネジメント株式会社')
)
WHERE name = 'ヤマトホールディングス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('現アートセッティングデリバリー株式会社', 'ヤマトホームコンビニエンス株式会社')
)
WHERE name = 'ヤマトホールディングス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社連結子会社の名称武蔵エンジニアリング株式会社ムサシ・フィールド・サポート株式会社エム・ビー・エス株式会社エフ・ビー・エム株式会社武蔵興産株式会社株式会社武蔵エンタープライズ株式会社ムサシ・エービーシームサシ・イメージ情報株式会社ムサシ・アイ・テクノ株式会社')
)
WHERE name = '株式会社ムサシ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社非連結子会社の名称株式会社')
)
WHERE name = '株式会社ムサシ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社持分法を適用した関係会社の名称株式会社')
)
WHERE name = '株式会社ムサシ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社持分法を適用していない非連結子会社の名称株式会社')
)
WHERE name = '株式会社ムサシ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社主要な連結子会社の名称マルゼン工業株式会社株式会社')
)
WHERE name = '株式会社マルゼン';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('非連結子会社の名称等非連結子会社台湾丸善股份有限公司')
)
WHERE name = '株式会社マルゼン';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('持分法を適用しない非連結子会社の台湾丸善股份有限公司')
)
WHERE name = '株式会社マルゼン';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('非連結子会社名株式会社大同テクノダイド建設株式会社翔研工業株式会社')
)
WHERE name = '大同工業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('ダイド建設株式会社', '翔研工業株式会社', 'テクノ株式会社')
)
WHERE name = '大同工業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('同じく連結子会社である株式会社', '日付で株式会社', '当社の連結子会社であった株式会社宮崎ジャムコ及び株式会社', '社当該連結子会社の名称株式会社ジャムコエアクラフトインテリアズ株式会社徳島ジャムコ株式会社ジャムコエアロテック株式会社オレンジジャムコ株式会社')
)
WHERE name = '株式会社ジャムコ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('香港高秀集團有限公司株式会社', '当社の連結子会社であった株式会社', '株式会社グリーン情報株式会社', '佛山市南方高秀電子科技有限公司', '江西高秀進出口貿易有限公司', '九江高秀園芸製品有限公司', '浙江正特高秀園芸建材有限公司', 'トーコー資材株式会社', '社連結子会社の名称ガーデンクリエイト株式会社')
)
WHERE name = '株式会社タカショー';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('持分法を適用しない関連会社株式会社', '上海高秀園芸建材有限公司及び満洲里高秀木業有限公司')
)
WHERE name = '株式会社タカショー';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('以下を自己の計算において所有しているにもかかわらず関連会社としなかった主要な会社等の名称総曲輪シテイ株式会社金沢都市開発株式会社オタヤ開発株式会社')
)
WHERE name = '株式会社大和';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('アールシーアイ株式会社')
)
WHERE name = '株式会社ケー・エフ・シー';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('唐山日翔建材科技有限公司')
)
WHERE name = '株式会社ケー・エフ・シー';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('唐山日翔建材科技有限公司')
)
WHERE name = '株式会社ケー・エフ・シー';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('マナック上海貿易有限公司')
)
WHERE name = '株式会社マナック・ケミカル・パートナーズ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('上海丸嘉貿易有限公司')
)
WHERE name = 'フルサト・マルカホールディングス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('広州丸嘉貿易有限公司')
)
WHERE name = 'フルサト・マルカホールディングス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('上海丸嘉貿易有限公司')
)
WHERE name = 'フルサト・マルカホールディングス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('テクノロジーソリューションズ株式会社東京エレクトロン九州株式会社東京エレクトロン宮城株式会社')
)
WHERE name = '東京エレクトロン株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('デバイス株式会社')
)
WHERE name = '東京エレクトロン株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('蘇州宏樹視覚芸術有限公司')
)
WHERE name = '株式会社メタリアル';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社株式会社')
)
WHERE name = '株式会社サトー商会';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社株式会社サトー食肉サービス株式会社サトーサービス株式会社')
)
WHERE name = '株式会社サトー商会';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社株式会社', '社株式会社サトー食肉サービス株式会社サトーサービス株式会社')
)
WHERE name = '株式会社サトー商会';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('株式会社株式会社大成中村自働機械株式会社株式会社高橋汽罐工業向井化工機株式会社')
)
WHERE name = '株式会社ＪＲＣ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('貿易有限公司')
)
WHERE name = '株式会社ＪＲＣ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('貿易有限公司')
)
WHERE name = '株式会社ＪＲＣ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('南山自重堂防護科技有限公司')
)
WHERE name = '株式会社自重堂';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('当連結会計年度において株式会社', '株式会社清水薬局及び株式会社')
)
WHERE name = '東邦ホールディングス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('主要な非連結子会社の名称等主要な非連結子会社の名称沖縄東邦株式会社')
)
WHERE name = '東邦ホールディングス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社主要な会社等の名称酒井薬品株式会社あゆみ製薬ホールディングス株式会社あゆみ製薬株式会社持分法適用により生じたあゆみ製薬ホールディングス株式会社')
)
WHERE name = '東邦ホールディングス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('持分法を適用していない非連結子会社及び関連会社等の状況主要な非連結子会社の名称沖縄東邦株式会社主要な関連会社等の名称株式会社')
)
WHERE name = '東邦ホールディングス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社連結子会社の名称エポンゴルフ株式会社')
)
WHERE name = '株式会社遠藤製作所';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社持分法適用関連会社の名称セブンシックス株式会社')
)
WHERE name = '株式会社遠藤製作所';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('汽車技術開発有限公司煙台福研模具有限公司', '社フクダエンジニアリング株式会社株式会社九州エフテック株式会社')
)
WHERE name = '株式会社エフテック';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社株式会社')
)
WHERE name = '株式会社エフテック';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社プログレッシブ・ツールズアンド・コンポーネンツ・リミテッドピー・ティー・ジェイ・エフ・ディー・インドネシア城南武漢科技有限公司株式会社城南九州製作所城南佛山科技有限公司')
)
WHERE name = '株式会社エフテック';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('汽車技術開発有限公司煙台福研模具有限公司', '社フクダエンジニアリング株式会社株式会社九州エフテック株式会社')
)
WHERE name = '株式会社エフテック';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('汽車技術開発有限公司煙台福研模具有限公司', '社フクダエンジニアリング株式会社株式会社九州エフテック株式会社')
)
WHERE name = '株式会社エフテック';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社プログレッシブ・ツールズアンド・コンポーネンツ・リミテッドジョーナンユーケー・リミテッドピー・ティー・ジェイ・エフ・ディー・インドネシア城南武漢科技有限公司株式会社城南九州製作所城南佛山科技有限公司')
)
WHERE name = '株式会社エフテック';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社連結子会社の名称株式会社たまのやカンノ・トレーディング株式会社株式会社', '株式会社フルール株式会社ハートライン株式会社北関東互助センター株式会社喜月堂セレオカンノ・トレーディング・ベトナム有限会社')
)
WHERE name = 'こころネット株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('日付けで天津中建万里石石材有限公司', '関連会社の名称天津万里石石材有限公司', '天津万里石石材有限公司', '日本エンディングパートナーズ株式会社')
)
WHERE name = 'こころネット株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('京大桂物理系研究棟株式会社', 'ツクシ工業株式会社')
)
WHERE name = '株式会社大林組';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('信幸建設株式会社', '東亜機械工業株式会社')
)
WHERE name = '東亜建設工業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('かずさまごころサービス株式会社')
)
WHERE name = '東亜建設工業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('かずさまごころサービス株式会社', '浅間山開発株式会社')
)
WHERE name = '東亜建設工業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('広島エルピージー配送センター東部エルピージーセンター株式会社')
)
WHERE name = '広島ガス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('鎮江中船日立造船機械有限公司')
)
WHERE name = 'カナデビア株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('上海康恒昱造環境技術有限公司')
)
WHERE name = 'カナデビア株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('高田プラント建設株式会社', '高田サービス株式会社', '渡部工業株式会社')
)
WHERE name = '高田工業所株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('ホクサン株式会社')
)
WHERE name = '石原産業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('マテリアル株式会社')
)
WHERE name = '石原産業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('ホクサン株式会社')
)
WHERE name = '石原産業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('高木精工香港有限公司')
)
WHERE name = '株式会社タカギセイコー';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('高和精工上海有限公司')
)
WHERE name = '株式会社タカギセイコー';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('佛山市南海華達高木模具有限公司')
)
WHERE name = '株式会社タカギセイコー';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('高木汽車部件佛山有限公司')
)
WHERE name = '株式会社タカギセイコー';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('武漢高木汽車部件有限公司')
)
WHERE name = '株式会社タカギセイコー';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('大連大顕高木模具有限公司')
)
WHERE name = '株式会社タカギセイコー';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('サイアムリケン社シュリラムピストンアンドリング社南京理研動力系統零部件有限公司')
)
WHERE name = 'リケンＮＰＲ株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('聖龍理研新能源寧波有限公司')
)
WHERE name = 'リケンＮＰＲ株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('有限会社末長一番保険サービス株式会社')
)
WHERE name = '木徳神糧株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('株式会社サンエツ商事三越金属上海有限公司')
)
WHERE name = '株式会社ＣＫサンエツ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('台湾三越股份有限公司')
)
WHERE name = '株式会社ＣＫサンエツ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('昭和物産株式会社', '関東食品株式会社')
)
WHERE name = '株式会社トーホー';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('アセットマネジメント合同会社')
)
WHERE name = '株式会社ｆａｎｔａｓｉｓｔａ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社連結子会社名台湾上村股份有限公司', '韓国上村株式会社')
)
WHERE name = '上村工業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('創世訊聯科技深圳有限公司')
)
WHERE name = 'テクミラホールディングス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('当社は連結子会社であった株式会社', '株式取得により株式会社')
)
WHERE name = '株式会社アイ・エス・ビー';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('日付で株式会社')
)
WHERE name = '株式会社プラザホールディングス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('連結子会社の名称近畿都市整備株式会社日本都市整備株式会社東北都市整備株式会社オオバ調査測量株式会社')
)
WHERE name = '株式会社オオバ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('同じく連結子会社である九州産交ランドマーク株式会社', '当社の連結子会社であった九州産交カード株式会社')
)
WHERE name = '株式会社エイチ・アイ・エス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('ホールディングス株式会社', '当社の連結子会社であった株式会社', 'エネルギーホールディングス株式会社')
)
WHERE name = '株式会社エイチ・アイ・エス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('当社の持分法適用関連会社であった株式会社', 'トラベル株式会社')
)
WHERE name = '株式会社エイチ・アイ・エス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('電力株式会社', '当社の連結子会社であったハウステンボス株式会社', 'エナジー株式会社', 'ヴィソンホテルマネジメント株式会社')
)
WHERE name = '株式会社エイチ・アイ・エス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('当社の連結子会社であった株式会社', '同じく連結子会社である九州産交リテール株式会社', '当社の連結子会社であった肥後リカー株式会社')
)
WHERE name = '株式会社エイチ・アイ・エス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('クルーズ株式会社', '当社の連結子会社であった洛碁中華大飯店股份有限公司')
)
WHERE name = '株式会社エイチ・アイ・エス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社連結子会社の名称株式会社')
)
WHERE name = '株式会社ワッツ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社主な会社等の名称株式会社')
)
WHERE name = '株式会社ワッツ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('南海電設株式会社', '社主要な連結子会社名株式会社', 'ソリューションズ株式会社', 'テンパール工業株式会社', '北川工業株式会社', 'サンテレホン株式会社')
)
WHERE name = '日東工業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('持分法を適用しない非連結子会社及び関連会社のうち主要な会社等の名称株式会社府中テンパール寺下工業株式会社')
)
WHERE name = '日東工業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('諏訪ソーラーエナジー合同会社', '日新レジン株式会社', '社竹鶴石油株式会社')
)
WHERE name = '日新商事株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('リーフ株式会社', '社日新興産株式会社')
)
WHERE name = '日新商事株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社連結子会社の名称デジタルプロモーション株式会社')
)
WHERE name = 'アビックス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('会社の名称株式会社', '日付で株式会社')
)
WHERE name = 'アビックス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('当連結会計年度中に当社が新たに株式会社', '当連結会計年度から株式会社')
)
WHERE name = 'アビックス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('株式会社マサルファシリティーズ空気設備工業株式会社', '空気設備工業株式会社')
)
WHERE name = '株式会社マサル';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('耐科斯托普軟件大連有限公司')
)
WHERE name = 'トレイダーズホールディングス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('光馳科技上海有限公司')
)
WHERE name = '株式会社オプトラン';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('光馳科技股份有限公司')
)
WHERE name = '株式会社オプトラン';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('台湾光馳上海商貿有限公司')
)
WHERE name = '株式会社オプトラン';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('光馳半導体技術上海有限公司')
)
WHERE name = '株式会社オプトラン';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('浙江晶馳光電科技有限公司')
)
WHERE name = '株式会社オプトラン';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('安徽繁楓新能源科技有限公司')
)
WHERE name = '株式会社オプトラン';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('月に上海繁楓真空科技有限公司')
)
WHERE name = '株式会社オプトラン';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('当連結会計年度から株式会社')
)
WHERE name = '株式会社インフォマート';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社連結子会社の名称株式会社', '株式会社木構造デザイン株式会社')
)
WHERE name = '株式会社エヌ・シー・エヌ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社会社名株式会社', '重要性が増した株式会社', '開発株式会社', '住宅ローンサービス株式会社株式会社')
)
WHERE name = '株式会社エヌ・シー・エヌ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社連結子会社の名称株式会社エスケーアイ株式会社セントラルパートナーズエスケーアイマネージメント株式会社エスケーアイ開発株式会社')
)
WHERE name = '株式会社サカイホールディングス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('フジオカエアータイト株式会社及びスワン商事株式会社')
)
WHERE name = '三洋工業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('平和オイルシール工業株式会社', 'イーグル工業株式会社')
)
WHERE name = 'ＮＯＫ株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('杭州杭化哈利瑪化工有限公司')
)
WHERE name = 'ハリマ化成グループ株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('三好化成工業株式会社')
)
WHERE name = 'ハリマ化成グループ株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('新日本油化株式会社')
)
WHERE name = 'ハリマ化成グループ株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('秋田十條化成株式会社')
)
WHERE name = 'ハリマ化成グループ株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('ヤマダ上海ポンプ貿易有限公司', '株式会社ヤマダプロダクツサービス株式会社')
)
WHERE name = '株式会社ヤマダコーポレーション';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('主要な持分法適用会社の名称株式会社')
)
WHERE name = '株式会社ニチレイ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('株式会社ベジパル有限会社')
)
WHERE name = '株式会社ピックルスホールディングス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('片倉上海農業科技有限公司')
)
WHERE name = '片倉コープアグリ株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('パイクリスタル株式会社', 'ノバフォーム株式会社')
)
WHERE name = 'ダイセル株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('ポリプラ・エボニック株式会社', 'ノバセル株式会社')
)
WHERE name = 'ダイセル株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('豊科フイルム株式会社')
)
WHERE name = 'ダイセル株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('またダイセルパイロテクニクス株式会社')
)
WHERE name = 'ダイセル株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('ポリプラ・エボニック株式会社')
)
WHERE name = 'ダイセル株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('豊科フイルム株式会社')
)
WHERE name = 'ダイセル株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社連結子会社の名称株式会社', '当社の連結子会社であった株式会社')
)
WHERE name = '株式会社フェリシモ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社持分法適用の関連会社の名称株式会社')
)
WHERE name = '株式会社フェリシモ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('フタバインダストリアルグジャラート株式会社', 'アメリカ株式会社', 'フタバインダストリアルテキサス株式会社', 'オートモーティブコンポーネンツ株式会社', 'フタバインディアナアメリカ株式会社', '社国内連結子会社名株式会社', 'オートモーティブカナダ株式会社')
)
WHERE name = 'フタバ産業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社関連会社の名称株式会社', '協祥機械工業株式会社')
)
WHERE name = 'フタバ産業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('持分法を適用しない関連会社の名称株式会社')
)
WHERE name = 'フタバ産業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('可利瑪股份有限公司')
)
WHERE name = '株式会社クリーマ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社主要な連結子会社の名称株式会社オリンピアパシフィックゴルフマネージメント株式会社', 'プロパティーズ株式会社株式会社アコーディア・ゴルフホールディングス株式会社', '現株式会社')
)
WHERE name = '株式会社平和';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('連結子会社の名称テクノクリエイト株式会社', '当社の連結子会社であった株式会社', '髙島電機株式会社')
)
WHERE name = '株式会社カナデン';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('菱神電子エンジニアリング株式会社')
)
WHERE name = '株式会社カナデン';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('株式会社岡山臨港及び岡山臨港倉庫運輸株式会社')
)
WHERE name = '株式会社クラレ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('株式会社岡山臨港及び岡山臨港倉庫運輸株式会社')
)
WHERE name = '株式会社クラレ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('ランク王株式会社', 'ワールドリンク株式会社')
)
WHERE name = 'クルーズ株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社会社等の名称株式会社カタリストキャピタル株式会社')
)
WHERE name = 'クルーズ株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('大倉フーズ株式会社', '貿易有限公司', '社主要な連結子会社の名称株式会社', '大鳳商事株式会社')
)
WHERE name = '株式会社ニッピ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('主要な非連結子会社名ニッピ都市開発株式会社', '日本皮革株式会社')
)
WHERE name = '株式会社ニッピ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社会社等の名称株式会社')
)
WHERE name = '株式会社ニッピ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('日本皮革株式会社', '持分法を適用しない非連結子会社及び関連会社のうち主要な会社等の名称ニッピ都市開発株式会社')
)
WHERE name = '株式会社ニッピ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('コイト電工株式会社')
)
WHERE name = '株式会社小糸製作所';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('竹田サンテック株式会社')
)
WHERE name = '株式会社小糸製作所';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('石川医療器株式会社')
)
WHERE name = 'メディアスホールディングス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('当社の連結子会社であった株式会社', '社連結子会社の名称株式会社プラコレ株式会社カヤックアキバスタジオ株式会社', '株式会社アスラフィルム及びラゾ株式会社', '英治出版株式会社配信技術研究所株式会社株式会社メガ・コミュニケーションズ株式会社アスラフィルムラゾ株式会社', '配信技術研究所株式会社', 'サンネット株式会社', '株式会社株式会社琉球カヤックスタジオ株式会社', '株式会社カヤックゼロ株式会社ゲムトレ株式会社カヤックボンド株式会社カヤックポラリス株式会社', '不動産株式会社', '英治出版株式会社')
)
WHERE name = '株式会社カヤック';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('琉球フットボールクラブ株式会社')
)
WHERE name = '株式会社カヤック';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社主な連結子会社の名称株式会社トランス株式会社トレードワークス株式会社クラフトワーク株式会社', '上海多来多貿易有限公司')
)
WHERE name = '株式会社トランザクション';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('台湾長大顧問有限公司')
)
WHERE name = '人・夢・技術グループ株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社連結子会社の名称株式会社ギガコアネットインタナショナル株式会社株式会社ラムダシステムズ株式会社レゾナ株式会社アコード・システム株式会社プロネットコア興産株式会社')
)
WHERE name = '株式会社コア';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社非連結子会社の名称株式会社')
)
WHERE name = '株式会社コア';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社持分法適用会社の名称株式会社')
)
WHERE name = '株式会社コア';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('持分法を適用しない非連結子会社及び関連会社の名称株式会社医療福祉工学研究所株式会社東北情報センター株式会社')
)
WHERE name = '株式会社コア';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('株式会社ジェイクレスト合同会社')
)
WHERE name = '株式会社ジェイホールディングス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('新日本海重工業株式会社')
)
WHERE name = '株式会社三井E&amp;S';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社主要な連結子会社の名称株式会社')
)
WHERE name = '株式会社ジェイテック';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社・連結子会社の名称株式会社ライフスタイルウォーター株式会社')
)
WHERE name = '株式会社ベネフィットジャパン';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('非連結子会社の名称等非連結子会社株式会社')
)
WHERE name = '株式会社ベネフィットジャパン';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('持分法を適用していない非連結子会社及び関連会社の名称株式会社', '当連結会計年度において株式会社', '持分法を適用しない関連会社であった株式会社')
)
WHERE name = '株式会社ベネフィットジャパン';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('持分法適用関連会社であった上毛建設株式会社')
)
WHERE name = '株式会社ヤマト';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社関連会社の名称上毛建設株式会社', '持分法適用関連会社であった上毛建設株式会社')
)
WHERE name = '株式会社ヤマト';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('以下を自己の計算において所有しているにもかかわらず関連会社としなかった主要な会社等の名称総曲輪シテイ株式会社金沢都市開発株式会社オタヤ開発株式会社')
)
WHERE name = '株式会社大和';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('関東航空計器株式会社')
)
WHERE name = '株式会社石川製作所';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('互金蘇州投資管理有限公司')
)
WHERE name = '株式会社マーキュリアホールディングス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社連結子会社の名称行方建設株式会社')
)
WHERE name = 'サンユー建設株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('非連結子会社の名称サンユーエステート株式会社')
)
WHERE name = 'サンユー建設株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('サンユーエステート株式会社', 'サンユーテクノ株式会社')
)
WHERE name = 'サンユー建設株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('非連結子会社の名称等株式会社西日本リアライズ株式会社タフアース株式会社らいとケア株式会社')
)
WHERE name = 'ライト工業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('井口建設株式会社', '港シビル株式会社')
)
WHERE name = '株式会社大盛工業';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('連結子会社であった株式会社', '社主要な連結子会社の名称伊藤喜オールスチール株式会社富士リビング工業株式会社株式会社イトーキマーケットスペース株式会社イトーキ東光製作所イトーキマルイ工業株式会社三幸ファシリティーズ株式会社株式会社エフエム・スタッフ株式会社イトーキシェアードバリュー新日本システック株式会社株式会社ダルトン株式会社')
)
WHERE name = '株式会社イトーキ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('株式会社ニコモ株式会社')
)
WHERE name = '株式会社光陽社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('テクノロジーズ株式会社')
)
WHERE name = '株式会社ヤマザキ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('重要性が増したため新潟イーグル株式会社')
)
WHERE name = 'イーグル工業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('チームス株式会社株式会社', '社・連結子会社の名称株式会社')
)
WHERE name = 'グロースエクスパートナーズ株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('関東航空計器株式会社')
)
WHERE name = '株式会社石川製作所';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('株式会社阿茲阿斯大連紡織服飾有限公司')
)
WHERE name = 'アゼアス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('大連保税区日里貿易有限公司')
)
WHERE name = 'アゼアス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('ワイ・エフ物流株式会社')
)
WHERE name = 'ユアサ・フナショク株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('ワイ・エフ・エージェンシー株式会社')
)
WHERE name = 'ユアサ・フナショク株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('日本畜産振興株式会社')
)
WHERE name = 'ユアサ・フナショク株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('・アイコム情報機器株式会社', '・株式会社マクロテクノス・株式会社', '・和歌山アイコム株式会社')
)
WHERE name = 'アイコム株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社・ポジション株式会社')
)
WHERE name = 'アイコム株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('上海実瞳健康科技有限公司', '上海実瞳光学科技有限公司', '香港実瞳健康科技有限公司')
)
WHERE name = '株式会社シード';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('香港実瞳光学科技有限公司', '上海実瞳商務咨詢有限公司', '上海実瞳視光医療科技有限公司', '横浜近視予防研究所株式会社')
)
WHERE name = '株式会社シード';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('香港実瞳光学科技有限公司', '上海実瞳商務咨詢有限公司', '上海実瞳視光医療科技有限公司', '横浜近視予防研究所株式会社')
)
WHERE name = '株式会社シード';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('新規設立によりアイスタイルデータコンサルティング株式会社', '株式会社アイスタイルプロダクツアイスタイルデータコンサルティング株式会社', '主要な連結子会社の名称株式会社アイスタイルリテール株式会社アイスタイルトレーディング株式会社アイスタイルキャリア株式会社', 'パートナーズ株式会社メディア・グローブ株式会社')
)
WHERE name = '株式会社アイスタイル';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('トレンダーズ株式会社', '新たに株式を取得したことにより株式会社')
)
WHERE name = '株式会社アイスタイル';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('ホールディングス株式会社', 'ホールディングス株式会社及び株式会社グルメデリカとの合併に伴い日本クッカリー株式会社は株式会社')
)
WHERE name = '株式会社ニッスイ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('ヤクルトチャイルドサポート株式会社', 'スペインヤクルト株式会社', '大船渡ヤクルト販売株式会社', '従来連結子会社であった北京ヤクルト販売株式会社')
)
WHERE name = '株式会社ヤクルト本社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('韓国ヤクルト株式会社')
)
WHERE name = '株式会社ヤクルト本社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('持分法を適用していない関連会社の香川ヤクルト販売株式会社')
)
WHERE name = '株式会社ヤクルト本社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('当社の持分法適用関連会社であった株式会社')
)
WHERE name = '江崎グリコ株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('江栄商事株式会社')
)
WHERE name = '江崎グリコ株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('関東フローズン株式会社', '江栄商事株式会社')
)
WHERE name = '江崎グリコ株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('バクテクス株式会社')
)
WHERE name = '森永製菓株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('延吉秀愛食品有限公司', '青島秀愛食品有限公司', '前連結会計年度において連結子会社でありました株式会社', '上海秀愛国際貿易有限公司', '香港正栄国際貿易有限公司', '連結子会社の名称筑波乳業株式会社')
)
WHERE name = '正栄食品工業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('近藤製粉株式会社')
)
WHERE name = '正栄食品工業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社主要な会社等の名称近藤製粉株式会社')
)
WHERE name = '正栄食品工業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社株式会社')
)
WHERE name = '株式会社紀文食品';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('非連結子会社の名称等非連結子会社株式会社')
)
WHERE name = '株式会社紀文食品';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('シティパートナーズ株式会社', '社株式会社ほっかほっか亭総本部株式会社味工房スイセン株式会社ファースト・メイト店舗流通ネット株式会社株式会社', 'インベストメント・マネジメント株式会社', '稲葉ピーナツ株式会社株式会社谷貝食品株式会社アイファクトリー株式会社ほっかほっか亭京滋地区本部株式会社', '株式会社株式会社鹿児島食品サービス株式会社')
)
WHERE name = '株式会社ハークスレイ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社株式会社アサヒ・トーヨー株式会社')
)
WHERE name = '株式会社ハークスレイ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社主要な連結子会社の名称株式会社クワザワ株式会社フリー・ステアーズ株式会社クワザワ工業株式会社住まいのクワザワ丸三商事株式会社')
)
WHERE name = 'クワザワホールディングス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('主要な非連結子会社名日桑建材株式会社和光クリーン株式会社恵庭アサノコンクリート株式会社')
)
WHERE name = 'クワザワホールディングス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社会社等の名称北海道管材株式会社')
)
WHERE name = 'クワザワホールディングス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('持分法を適用しない非連結子会社及び関連会社のうち主要な会社等の名称日桑建材株式会社和光クリーン株式会社恵庭アサノコンクリート株式会社大野アサノコンクリート株式会社')
)
WHERE name = 'クワザワホールディングス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社連結子会社の名称株式会社')
)
WHERE name = '株式会社フェイスネットワーク';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('アセットマネジメント株式会社フェイスプロパティーズ合同会社', 'サロン株式会社')
)
WHERE name = '株式会社フェイスネットワーク';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('アセットマネジメント株式会社フェイスプロパティーズ合同会社', 'サロン株式会社')
)
WHERE name = '株式会社フェイスネットワーク';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('雄健建設株式会社', 'フジ・アメニティサービス株式会社')
)
WHERE name = 'フジ住宅株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('雄健建設株式会社', '関西電設工業株式会社', 'フジ・アメニティサービス株式会社')
)
WHERE name = 'フジ住宅株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社連結子会社の名称株式会社')
)
WHERE name = '株式会社キャンディル';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('中之島高速鉄道株式会社')
)
WHERE name = '京阪ホールディングス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('株式会社京阪ビジネスマネジメント等非連結子会社及び株式会社')
)
WHERE name = '京阪ホールディングス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('四積化工株式会社', '東積加工株式会社', '甲府積水産業株式会社', '積水ソーラーフィルム株式会社')
)
WHERE name = '積水化学工業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('セキスイハイムクリエイト株式会社')
)
WHERE name = '積水化学工業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社主要な会社名積水化成品工業株式会社')
)
WHERE name = '積水化学工業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('セキスイハイムクリエイト株式会社')
)
WHERE name = '積水化学工業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('主要な連結子会社の名称株式会社フージャースコーポレーション株式会社フージャースケアデザイン株式会社フージャースアセットマネジメント株式会社フージャースリビングサービス株式会社')
)
WHERE name = '株式会社フージャースホールディングス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('東京日本橋兜町ホテル株式会社', '社連結子会社の名称平和不動産プロパティマネジメント株式会社ハウジングサービス株式会社平和不動産アセットマネジメント株式会社株式会社東京証券会館東京日比谷ホテル株式会社東京日本橋兜町ホテル株式会社')
)
WHERE name = '平和不動産株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('ディレクション株式会社', 'アセットインベストメント株式会社', '社連結子会社名株式会社テーオーリネンサプライ株式会社テーオーシーサプライ星製薬株式会社株式会社')
)
WHERE name = '株式会社テーオーシー';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('非連結子会社の名称等株式会社')
)
WHERE name = '株式会社テーオーシー';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社会社の名称大崎再開発ビル株式会社')
)
WHERE name = '株式会社テーオーシー';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('持分法を適用しない非連結子会社及び関連会社のうち主要な会社等の名称株式会社')
)
WHERE name = '株式会社テーオーシー';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('当該連結子会社は株式会社エリアクエスト不動産コンサルティング及び株式会社')
)
WHERE name = '株式会社エリアクエスト';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('前連結会計年度において連結子会社でありました株式会社', '商貿有限公司', '前連結会計年度において連結子会社でありました台湾比智商貿股フン有限公司')
)
WHERE name = '株式会社ピアラ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('日付で株式会社')
)
WHERE name = '株式会社北の達人コーポレーション';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('連結子会社であった株式会社', '同社が株式を保有していた株式会社')
)
WHERE name = '株式会社北の達人コーポレーション';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('学校空調東広島株式会社')
)
WHERE name = '株式会社中電工';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('インベストメント株式会社')
)
WHERE name = '株式会社中電工';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('幸栄電設株式会社', 'ソーラー株式会社', '学校空調三原株式会社', '学校空調やまぐち株式会社', '学校空調周南株式会社', '学校空調東広島株式会社', '三和電気工事株式会社')
)
WHERE name = '株式会社中電工';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社連結子会社の名称北陸ダイセキ株式会社株式会社ダイセキ環境ソリューション株式会社', 'システム機工株式会社株式会社グリーンアローズ中部株式会社グリーンアローズ九州株式会社杉本商事有限会社')
)
WHERE name = '株式会社ダイセキ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社連結子会社の名称株式会社ミダック株式会社ミダックライナー株式会社三晃株式会社ミダックこなん遠州砕石株式会社株式会社')
)
WHERE name = '株式会社ミダックホールディングス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('株式会社株式会社')
)
WHERE name = '株式会社ミダックホールディングス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('持分法適用の関連会社の名称株式会社')
)
WHERE name = '株式会社ミダックホールディングス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('株式会社株式会社')
)
WHERE name = '株式会社ミダックホールディングス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('株式会社ジャスプロ広州捷多商貿有限公司')
)
WHERE name = 'ゼット株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('有限公司株式会社', '連結子会社名株式会社南陽レンテック株式会社南陽重車輌共栄通信工業株式会社株式会社戸髙製作所株式会社エイ・エス・エイ・ピイ株式会社')
)
WHERE name = '株式会社南陽';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('持分法を適用した非連結子会社名及び関連会社名建南和股份有限公司')
)
WHERE name = '株式会社南陽';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('日に株式会社')
)
WHERE name = '株式会社ジェイエスエス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('江蘇京海服装貿易有限公司')
)
WHERE name = '株式会社ダブルエー';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('広州富田貿易有限公司')
)
WHERE name = '株式会社トミタ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('非連結子会社の名称株式会社トミタファミリー有限会社')
)
WHERE name = '株式会社トミタ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('持分法を適用しない非連結子会社の名称株式会社トミタファミリー有限会社')
)
WHERE name = '株式会社トミタ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社関連会社の名称エムスリーキャリア株式会社')
)
WHERE name = '株式会社エス・エム・エス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社連結子会社の名称株式会社シルク・ラボラトリ株式会社')
)
WHERE name = '株式会社IC';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('神商精密株式会社', '日本グラニュレーター株式会社')
)
WHERE name = '神鋼商事株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社主要な会社名アジア化工株式会社大阪精工株式会社日本スタッドウェルディング株式会社株式会社')
)
WHERE name = '神鋼商事株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('当連結会計年度では連結子会社であった成都三国機械電子有限公司')
)
WHERE name = '株式会社ミクニ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('主要な非連結子会社の名称等主要な非連結子会社名ミクニ・アール・ケイ精密株式会社')
)
WHERE name = '株式会社ミクニ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('ミクニ・アール・ケイ精密株式会社')
)
WHERE name = '株式会社ミクニ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('三國リビングサービス株式会社')
)
WHERE name = '株式会社ミクニ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('前連結会計年度まで連結子会社であった新第一塩ビ株式会社')
)
WHERE name = '株式会社トクヤマ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('日付でアールエム東セロ株式会社', '三井化学東セロ株式会社', '前連結会計年度まで持分法適用関連会社であったサン・トックス株式会社')
)
WHERE name = '株式会社トクヤマ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('大分鉱業株式会社')
)
WHERE name = '株式会社トクヤマ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社会社名ソニー生命保険株式会社ソニーライフ・コミュニケーションズ株式会社ソニー損害保険株式会社ソニー銀行株式会社ソニー・ライフケア株式会社ライフケアデザイン株式会社プラウドライフ株式会社ソニーフィナンシャルベンチャーズ株式会社')
)
WHERE name = 'ソニーフィナンシャルグループ株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社会社名ビー・エックス・ジェイ・エー・ワン・ホールディング株式会社ソニーペイメントサービス株式会社', 'ソリューションズ株式会社')
)
WHERE name = 'ソニーフィナンシャルグループ株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('主要な非連結子会社の名称等長興株式会社')
)
WHERE name = '長瀬産業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('・無錫澄泓微電子材料有限公司')
)
WHERE name = '長瀬産業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('長興株式会社')
)
WHERE name = '長瀬産業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社連結子会社の名称株式会社')
)
WHERE name = '株式会社伸和ホールディングス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('丸彦商事株式会社')
)
WHERE name = '清水建設 株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社連結子会社の名称株式会社山形メイコー株式会社宮城メイコー株式会社メイコーテック株式会社メイコーテクノメイコーエレクディベロップ株式会社メイコーエレクマニュファクチャー株式会社名幸電子香港有限公司', '有限公司広州市斯皮徳貿易有限公司')
)
WHERE name = '株式会社メイコー';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('連結子会社の名称株式会社インベスト株式会社', 'ガンマ株式会社', 'デルタ山田プライド株式会社株式会社')
)
WHERE name = '株式会社インサイト';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('持分法適用会社数はクミ化成株式会社', '社主要な会社等の名称クミ化成株式会社株式会社鈴裕化学クミ化成株式会社', '社の損益をクミ化成株式会社')
)
WHERE name = '明和産業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('ニトックス株式会社', '天津天富軟管工業有限公司', '社主要な連結子会社の名称株式会社')
)
WHERE name = '株式会社テクノフレックス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('ファインバブルテック株式会社')
)
WHERE name = '株式会社丸山製作所';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('ファインバブルテック株式会社')
)
WHERE name = '株式会社丸山製作所';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('ファインバブルテック株式会社')
)
WHERE name = '株式会社丸山製作所';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('ファインバブルテック株式会社')
)
WHERE name = '株式会社丸山製作所';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社主要な連結子会社の名称株式会社オプティムアグリ・みちのくオプティム・バンクテクノロジーズ株式会社株式会社オプティム・デジタルコンストラクション株式会社ユラスコア株式会社')
)
WHERE name = '株式会社オプティム';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('号株式会社', '株式会社株式会社', '社ディピューラメディカルソリューションズ株式会社')
)
WHERE name = '株式会社オプティム';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('人材育成株式会社')
)
WHERE name = '株式会社 協和コンサルタンツ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('康鴻森田香港有限公司')
)
WHERE name = '株式会社モリタホールディングス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('南京晨光森田環保科技有限公司')
)
WHERE name = '株式会社モリタホールディングス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('ショーエイ株式会社', '藤和コンクリート圧送株式会社', 'タロトデンキ株式会社', 'コマツ栃木株式会社')
)
WHERE name = '藤井産業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社栃木小松フォークリフト株式会社')
)
WHERE name = '藤井産業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('明和物産株式会社')
)
WHERE name = '株式会社カクヤス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社連結子会社の名称株式会社', '新視野光學股份有限公司株式会社ジェネリックコーポレーション株式会社')
)
WHERE name = '株式会社シンシア';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('常州英富紡織有限公司', '連結子会社の名称株式会社', '上海福拓線貿易有限公司', '上海富士克制線有限公司', '上海新富士克制線有限公司')
)
WHERE name = '株式会社フジックス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社会社の名称株式会社')
)
WHERE name = 'インフォメティス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('非連結子会社名株式会社大同テクノダイド建設株式会社翔研工業株式会社')
)
WHERE name = '大同工業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('ダイド建設株式会社', '翔研工業株式会社', 'テクノ株式会社')
)
WHERE name = '大同工業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('取得による企業結合によりニューロアドバンスド株式会社', 'アドテック株式会社', '社日本光電富岡株式会社')
)
WHERE name = '日本光電工業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('インテリジェントシステムズ株式会社株式会社', '連結子会社であったリーサコンサルティング株式会社', '社連結子会社の名称大連天達科技有限公司三友テクノロジー株式会社株式会社テンダゲームス株式会社', 'インテリジェントシステムズ株式会社及び株式会社')
)
WHERE name = '株式会社テンダ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('主要な非連結子会社名沈陽邦友科技有限公司', '沈陽邦友科技有限公司')
)
WHERE name = '株式会社テンダ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('持分法の適用に関する事項持分法を適用しない非連結子会社の名称沈陽邦友科技有限公司', '持分法を適用しない理由沈陽邦友科技有限公司')
)
WHERE name = '株式会社テンダ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社主要な連結子会社名株式会社')
)
WHERE name = '株式会社ナガセ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('私立学校奨学支援保険サービス株式会社')
)
WHERE name = '株式会社ナガセ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('総合地所株式会社', '株式会社長谷工コミュニティ九州株式会社長谷工コミュニティ西日本株式会社', '不二建設株式会社')
)
WHERE name = '株式会社　長谷工コーポレーション';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('株式会社長谷工ナヴィエ株式会社')
)
WHERE name = '株式会社　長谷工コーポレーション';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('持分法を適用しない非連結子会社及び関連会社のうち主要な会社名持分法非適用の主要な非連結子会社株式会社長谷工ナヴィエ株式会社')
)
WHERE name = '株式会社　長谷工コーポレーション';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社連結子会社の名称砂防エンジニアリング株式会社株式会社')
)
WHERE name = '株式会社日水コン';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社非連結子会社の名称株式会社')
)
WHERE name = '株式会社日水コン';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('有限公司株式会社')
)
WHERE name = '株式会社日水コン';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('センター株式会社')
)
WHERE name = '富士電機株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('富士グリーンパワー株式会社')
)
WHERE name = '富士電機株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('メタウォーターサービス株式会社', 'メタウォーター株式会社', '富士ファーマナイト株式会社')
)
WHERE name = '富士電機株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('シンカテクノロジー株式会社エムエスシー製造株式会社株式会社篠原製作所京和精工株式会社株式会社キンポーメルテック株式会社エアロクラフトジャパン株式会社天鳥株式会社ティオック株式会社', '主要な連結子会社の名称株式会社')
)
WHERE name = '株式会社技術承継機構';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社連結子会社の名称株式会社')
)
WHERE name = '株式会社マネジメントソリューションズ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('ブラジル社ヨロズタイランド社ワイ・オグラオートモーティブタイランド社ヨロズエンジニアリングシステムズタイランド社广州萬宝井汽車部件有限公司武漢萬宝井汽車部件有限公司', '株式会社庄内ヨロズ株式会社ヨロズエンジニアリング株式会社', '連結子会社の名称株式会社ヨロズ栃木株式会社ヨロズ大分株式会社')
)
WHERE name = '株式会社ヨロズ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('中倉陸運株式会社', '中央倉庫ワークス株式会社')
)
WHERE name = '中央倉庫株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('持分法適用関連会社であった株式会社', 'ユーシーエス株式会社')
)
WHERE name = '中央倉庫株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('建和住宅株式会社')
)
WHERE name = '株式会社エストラスト';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('オリエルホーム株式会社')
)
WHERE name = '株式会社エストラスト';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('株式会社スーパーワン株式会社', '株式会社スーパーワンは株式会社')
)
WHERE name = '株式会社エーアイ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社・ジャックス債権回収サービス株式会社・ジャックス・トータル・サービス株式会社・ジャックスリース株式会社')
)
WHERE name = '株式会社ジャックス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('旭化成ネットワークス株式会社')
)
WHERE name = '旭化成株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('旭化成ネットワークス株式会社', '旭有機材株式会社')
)
WHERE name = '旭化成株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('南陽化成株式会社')
)
WHERE name = '旭化成株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('主要な非連結子会社の名称等主要な非連結子会社ビジョンベンチャーズ株式会社')
)
WHERE name = '株式会社ビジョン';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社持分法を適用する関連会社の名称株式会社')
)
WHERE name = '株式会社ビジョン';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社連結子会社の名称日本ウェルネス研究所株式会社')
)
WHERE name = '株式会社リプライオリティ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社株式会社ココト株式会社ブライエ株式会社')
)
WHERE name = '株式会社クレオ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('連結の範囲に含めておりました株式会社')
)
WHERE name = '株式会社ルネサンス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('新大華機械股份有限公司', '社連結子会社の名称やまびこジャパン株式会社', 'エコー産業株式会社', 'やまびこエンジニアリング株式会社', '追浜工業株式会社', '双伸工業株式会社')
)
WHERE name = '株式会社やまびこ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社会社等の名称寧波奥浜動力科技有限公司')
)
WHERE name = '株式会社やまびこ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('エピフォトニクス株式会社', '東莞瑚北電子有限公司蘇州瑚北光電子有限公司')
)
WHERE name = '湖北工業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('株式会社アロウブライト株式会社')
)
WHERE name = '株式会社ブラス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('連結子会社の名称ニッコーエクステリア株式会社株式会社サンキャリー葉月工業株式会社')
)
WHERE name = '日本興業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('シマダヤ関東株式会社', 'シマダヤ商事株式会社', 'シマダヤ西日本株式会社', 'シマダヤ東北株式会社')
)
WHERE name = 'シマダヤ株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('有限公司エスジェイジュエリー株式会社株式会社', '社ソマ株式会社ナガホリリテール株式会社')
)
WHERE name = '株式会社ナガホリ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('株式取得により株式会社', 'いい不動産プラザ株式会社', 'パル建設株式会社', 'アオイ建設株式会社', '及びいい不動産プラザ株式会社', '住宅の横綱大和建設株式会社')
)
WHERE name = 'ファースト住建株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('リタ総合不動産株式会社', 'ファースト工務店株式会社')
)
WHERE name = 'ファースト住建株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('有限会社アオイ設計事務所及びリタ総合不動産株式会社', 'ファースト工務店株式会社')
)
WHERE name = 'ファースト住建株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('信息系統有限公司株式会社宮崎太陽農園株式会社', '社連結子会社の名称フォーサイトシステム株式会社シーイーシークロスメディア株式会社株式会社シーイーシーカスタマサービス大分シーイーシー株式会社')
)
WHERE name = '株式会社シーイーシー';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('北陸興産株式会社')
)
WHERE name = '北陸電気工業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('北陸興産株式会社')
)
WHERE name = '北陸電気工業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('北陸興産株式会社')
)
WHERE name = '北陸電気工業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('朝日印刷ビジネスサポート株式会社')
)
WHERE name = '朝日印刷株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('朝日印刷ビジネスサポート株式会社')
)
WHERE name = '朝日印刷株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('杭州泰谷諾石英有限公司')
)
WHERE name = 'ジーエルテクノホールディングス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('技尓上海商貿有限公司')
)
WHERE name = 'ジーエルテクノホールディングス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('連結子会社の名称テクノクリエイト株式会社', '当社の連結子会社であった株式会社', '髙島電機株式会社')
)
WHERE name = '株式会社カナデン';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('菱神電子エンジニアリング株式会社')
)
WHERE name = '株式会社カナデン';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('矢澤フェロマイト株式会社')
)
WHERE name = '株式会社ナガオカ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('商業有限公司', '株式会社アサダユウミヤコ化学株式会社株式会社', '株式会社蝶理マシナリー株式会社株式会社')
)
WHERE name = '蝶理株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('アビトップ株式会社')
)
WHERE name = '旭有機材株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('ドリコウェルテクノ株式会社')
)
WHERE name = '旭有機材株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('号合同会社', '重要性が増加した株式会社')
)
WHERE name = '太陽ホールディングス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('前連結会計年度まで連結子会社であった新第一塩ビ株式会社')
)
WHERE name = '株式会社トクヤマ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('日付でアールエム東セロ株式会社', '三井化学東セロ株式会社', '前連結会計年度まで持分法適用関連会社であったサン・トックス株式会社')
)
WHERE name = '株式会社トクヤマ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('大分鉱業株式会社')
)
WHERE name = '株式会社トクヤマ';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('小松美特料蘇州貿易有限公司')
)
WHERE name = '小松マテーレ株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('株式会社丸尾上海貿易有限公司')
)
WHERE name = '丸尾カルシウム株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('東莞立丸奈米科技有限公司')
)
WHERE name = '丸尾カルシウム株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('安泰科科技股份有限公司')
)
WHERE name = '株式会社アテクト';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('システムズ株式会社')
)
WHERE name = '神東塗料株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('広州徳来路博科技有限公司')
)
WHERE name = '東洋ドライルーブ株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('中山市三民金属処理有限公司')
)
WHERE name = '東洋ドライルーブ株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('昆山三民塗頼表面処理技術有限公司')
)
WHERE name = '東洋ドライルーブ株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('迪恩特塗料浙江有限公司')
)
WHERE name = '大日本塗料株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('株式会社神東艾仕得塗料系統股份有限公司')
)
WHERE name = '大日本塗料株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社連結子会社の名称株式会社', '化粧品有限公司株式会社クリニメディック株式会社')
)
WHERE name = '株式会社シーボン';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('ベイ安美上海化粧品有限公司')
)
WHERE name = 'プレミアアンチエイジング株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('威耐可适商ボウ北京有限公司')
)
WHERE name = 'プレミアアンチエイジング株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社連結子会社の名称タイヨーインタナショナルインク開封太陽金明食品有限公司タイヨーインタコリアリミテッドタイヨーカガクインディアプライベイトリミテッド株式会社タイヨーラボテクノ・マーケティング株式会社無錫太陽緑宝科技有限公司上海太陽食研国際貿易有限公司', '食品有限公司')
)
WHERE name = '太陽化学株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('上海新素材特種聚合物有限公司')
)
WHERE name = '株式会社日本ピグメントホールディングス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('大恭化學工業股份有限公司')
)
WHERE name = '株式会社日本ピグメントホールディングス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('三菱セメント株式会社')
)
WHERE name = 'ＵＢＥ株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('連結子会社名松友商事株式会社松井リフォーム株式会社')
)
WHERE name = '松井建設株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('いなぎ文化センターサービス株式会社')
)
WHERE name = '松井建設株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('上海実瞳健康科技有限公司', '上海実瞳光学科技有限公司', '香港実瞳健康科技有限公司')
)
WHERE name = '株式会社シード';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('香港実瞳光学科技有限公司', '上海実瞳商務咨詢有限公司', '上海実瞳視光医療科技有限公司', '横浜近視予防研究所株式会社')
)
WHERE name = '株式会社シード';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('香港実瞳光学科技有限公司', '上海実瞳商務咨詢有限公司', '上海実瞳視光医療科技有限公司', '横浜近視予防研究所株式会社')
)
WHERE name = '株式会社シード';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('汽車技術開発有限公司煙台福研模具有限公司', '社フクダエンジニアリング株式会社株式会社九州エフテック株式会社')
)
WHERE name = '株式会社エフテック';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社株式会社')
)
WHERE name = '株式会社エフテック';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社プログレッシブ・ツールズアンド・コンポーネンツ・リミテッドピー・ティー・ジェイ・エフ・ディー・インドネシア城南武漢科技有限公司株式会社城南九州製作所城南佛山科技有限公司')
)
WHERE name = '株式会社エフテック';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('汽車技術開発有限公司煙台福研模具有限公司', '社フクダエンジニアリング株式会社株式会社九州エフテック株式会社')
)
WHERE name = '株式会社エフテック';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('汽車技術開発有限公司煙台福研模具有限公司', '社フクダエンジニアリング株式会社株式会社九州エフテック株式会社')
)
WHERE name = '株式会社エフテック';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社プログレッシブ・ツールズアンド・コンポーネンツ・リミテッドジョーナンユーケー・リミテッドピー・ティー・ジェイ・エフ・ディー・インドネシア城南武漢科技有限公司株式会社城南九州製作所城南佛山科技有限公司')
)
WHERE name = '株式会社エフテック';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('未来ネットワーク株式会社', '社連結子会社の名称株式会社アスペック丸善土木株式会社守谷不動産株式会社機材サービス株式会社未来ネットワーク株式会社')
)
WHERE name = '株式会社守谷商会';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('太平洋汽門工業股份有限公司', '太平洋バルブ工業株式会社太平洋エアコントロール工業株式会社天津太平洋汽車部件有限公司長沙太平洋半谷汽車部件有限公司', 'ピーアイシステム株式会社太平洋産業株式会社')
)
WHERE name = '太平洋工業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('ホールディングス株式会社')
)
WHERE name = '太平洋工業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社連結子会社の名称株式会社ゆめみ株式会社マーキュリー株式会社バッカス株式会社', '株式会社株式会社ラボル株式会社サルース株式会社')
)
WHERE name = '株式会社セレス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社持分法適用会社の名称ビットバンク株式会社')
)
WHERE name = '株式会社セレス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('益新株式会社')
)
WHERE name = '株式会社スズケン';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('円賀工業株式会社')
)
WHERE name = '株式会社九電工として「定款一部変更の件」を付議しており、当該議案が承認可決されると、 株式会社クラフティア新英訳名 KRAFTIA CORPORATION';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('吉野家中国投資有限公司')
)
WHERE name = '株式会社 吉野家ホールディングス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('深圳吉野家快餐有限公司')
)
WHERE name = '株式会社 吉野家ホールディングス';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('神栄テクノロジー株式会社', '神栄キャパシタ株式会社', '神栄ホームクリエイト株式会社', '神栄リビングインダストリー株式会社', '貿易有限公司')
)
WHERE name = '神栄株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('関西通商株式会社')
)
WHERE name = '神栄株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('関西通商株式会社')
)
WHERE name = '神栄株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('株式会社香港大和工貿有限公司')
)
WHERE name = 'ヤマト モビリティ ＆ Mfg.株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('大和高精密工業深圳有限公司')
)
WHERE name = 'ヤマト モビリティ ＆ Mfg.株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('亜禡特貿易上海有限公司')
)
WHERE name = 'ヤマト モビリティ ＆ Mfg.株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('米国上海安永精密切割機有限公司')
)
WHERE name = '株式会社安永';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('久世香港有限公司')
)
WHERE name = '有限会社久世';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('上海日生食品物流有限公司')
)
WHERE name = '有限会社久世';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('久華世成都商貿有限公司')
)
WHERE name = '有限会社久世';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('新たに設立した戸田建設不動産投資顧問株式会社', '及び株式会社', 'フローティング・ウィンド・アグリゲーション株式会社')
)
WHERE name = '戸田建設株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('主要な非連結子会社名エスシーシー・ヒューマンコミュニティサービス株式会社')
)
WHERE name = '戸田建設株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('主要な持分法非適用の関連会社名株式会社', '主要な持分法非適用の非連結子会社名エスシーシー・ヒューマンコミュニティサービス株式会社')
)
WHERE name = '戸田建設株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('三井製糖株式会社')
)
WHERE name = 'ＤＭ三井製糖ホールディングス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('当社の非連結子会社であった第一エンジニアリング株式会社', '当社の連結子会社であった株式会社ウエイブエンジニアリング及び当社の非連結子会社であった株式会社', 'この組織再編により株式会社', '同日付で第一エンジニアリング株式会社は株式会社')
)
WHERE name = '第一実業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('主要な非連結子会社の名称プラントデジタルエックス株式会社', '一實股份有限公司')
)
WHERE name = '第一実業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('持分法を適用しない非連結子会社及び関連会社のうち主要な会社等の名称非連結子会社プラントデジタルエックス株式会社', '一實股份有限公司関連会社第一スルザー株式会社')
)
WHERE name = '第一実業株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('株式会社ゆめファーム秋川牧園常州農業有限公司')
)
WHERE name = '株式会社秋川牧園';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('グローバル・ジャパン株式会社株式会社', 'ロジホールディングス名糖運輸株式会社株式会社', 'システム株式会社', '上海虹迪物流科技有限公司', 'ムービング株式会社株式会社ワールドサプライ佐川グローバルロジスティクス株式会社', 'モータース株式会社', '名糖運輸株式会社', '社主要な連結子会社の名称佐川急便株式会社佐川ヒューモニー株式会社', 'フィルダー株式会社', '日付で名糖運輸株式会社', 'リアルティ株式会社佐川アドバンス株式会社')
)
WHERE name = 'ＳＧホールディングス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社主要な会社の名称国家能源集団格尓木光伏発電有限公司株式会社', '直販配送株式会社')
)
WHERE name = 'ＳＧホールディングス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('持分法を適用しない関連会社の名称等株式会社')
)
WHERE name = 'ＳＧホールディングス株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社連結子会社の名称佐田道路株式会社株式会社島田組株式会社リフォーム群馬彩光建設株式会社')
)
WHERE name = '佐田建設株式会社';

UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('社連結子会社の名称株式会社シティ情報ふくおか株式会社')
)
WHERE name = 'アプライド株式会社';
