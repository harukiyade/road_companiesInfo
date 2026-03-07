import pandas as pd
import re
import json
i[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004hPOSTGRES_HOST='34.84.189.233' POSTGRES_PORT='5432' POSTGRES_PASSWORD='Legatus2000/' python scripts/merge_by_url_execute.py[K[?2004l
=== URLベースのマージ＆クレンジング (本番実行) ===
1. マスターレコードのURL一覧を抽出中... (数十秒かかります)
 -> 3899763 件のマスターURL情報をメモリにロードしました。
 -> URLの正規化とインデックス作成中... (数秒〜十数秒かかります)
2. 対象のUUIDレコードを抽出中...
 -> URLを持つUUIDレコードは 0 件です。
3. マッチング処理を開始します...

完了: 0 件のデータをURLをキーにしてマージ＆クレンジングしました。
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[7mPOSTGRES_HOST='34[7m.[7m84.189.233' POSTGRES_PORT='5432' POSTGRES_PASSWORD='Legatu[7ms[7m2000/' python scripts/sync_relation_ids_safe.py[27m[K[A[A[27mP[27mO[27mS[27mT[27mG[27mR[27mE[27mS[27m_[27mH[27mO[27mS[27mT[27m=[27m'[27m3[27m4.[27m8[27m4[27m.[27m1[27m8[27m9[27m.[27m2[27m3[27m3[27m'[27m [27mP[27mO[27mS[27mT[27mG[27mR[27mE[27mS[27m_[27mP[27mO[27mR[27mT[27m=[27m'[27m5[27m4[27m3[27m2[27m'[27m [27mP[27mO[27mS[27mT[27mG[27mR[27mE[27mS[27m_[27mP[27mA[27mS[27mS[27mW[27mO[27mR[27mD[27m=[27m'[27mL[27me[27mg[27ma[27mt[27mus[27m2[27m0[27m0[27m0[27m/[27m'[27m [27mp[27my[27mt[27mh[27mo[27mn[27m [27ms[27mc[27mr[27mi[27mp[27mt[27ms[27m/[27ms[27my[27mn[27mc[27m_[27mr[27me[27ml[27ma[27mt[27mi[27mo[27mn[27m_[27mi[27md[27ms[27m_[27ms[27ma[27mf[27me[27m.[27mp[27my[?2004l
1. 中間テーブルから更新対象のデータを集計中...
 -> 更新対象の親企業数: 1805 件
2. 1,000件ずつ companies テーブルを更新中...
 -> 1000 / 1805 件完了...
 -> 1805 / 1805 件完了...

すべての同期が完了しました。
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[7mPOSTGRES_HOST='34[7m.[7m84.189.233' POSTGRES_PORT='5432' POSTGRES_PASSWORD='Legatu[7ms[7m2000/' python scripts/cleanup_duplicate_relations.py[27m[K[A[A[11D[27mP[27mO[27mS[27mT[27mG[27mR[27mE[27mS[27m_[27mH[27mO[27mS[27mT[27m=[27m'[27m3[27m4.[27m8[27m4[27m.[27m1[27m8[27m9[27m.[27m2[27m3[27m3[27m'[27m [27mP[27mO[27mS[27mT[27mG[27mR[27mE[27mS[27m_[27mP[27mO[27mR[27mT[27m=[27m'[27m5[27m4[27m3[27m2[27m'[27m [27mP[27mO[27mS[27mT[27mG[27mR[27mE[27mS[27m_[27mP[27mA[27mS[27mS[27mW[27mO[27mR[27mD[27m=[27m'[27mL[27me[27mg[27ma[27mt[27mus[27m2[27m0[27m0[27m0[27m/[27m'[27m [27mp[27my[27mt[27mh[27mo[27mn[27m [27ms[27mc[27mr[27mi[27mp[27mt[27ms[27m/[27mc[27ml[27me[27ma[27mn[27mu[27mp[27m_[27md[27mu[27mp[27ml[27mi[27mc[27ma[27mt[27me[27m_[27mr[27me[27ml[27ma[27mt[27mi[27mo[27mn[27ms[27m.[27mp[27my[?2004l
1. 重複紐付けが発生している企業名を抽出中...
 -> 対象となる重複企業名: 76 種

2. 企業名ごとにクレンジングを実行中...
 -> 0/76 件完了 (現在の名前: 三洋工業株式会社)

すべてのクレンジングが完了しました。
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[7mPOSTGRES_HOST='34[7m.[7m84.189.233' POSTGRES_PORT='5432' POSTGRES_PASSWORD='Legatu[7ms[7m2000/' python scripts/repair_optim_relations.py[27m[K[A[A[27mP[27mO[27mS[27mT[27mG[27mR[27mE[27mS[27m_[27mH[27mO[27mS[27mT[27m=[27m'[27m3[27m4.[27m8[27m4[27m.[27m1[27m8[27m9[27m.[27m2[27m3[27m3[27m'[27m [27mP[27mO[27mS[27mT[27mG[27mR[27mE[27mS[27m_[27mP[27mO[27mR[27mT[27m=[27m'[27m5[27m4[27m3[27m2[27m'[27m [27mP[27mO[27mS[27mT[27mG[27mR[27mE[27mS[27m_[27mP[27mA[27mS[27mS[27mW[27mO[27mR[27mD[27m=[27m'[27mL[27me[27mg[27ma[27mt[27mus[27m2[27m0[27m0[27m0[27m/[27m'[27m [27mp[27my[27mt[27mh[27mo[27mn[27m [27ms[27mc[27mr[27mi[27mp[27mt[27ms[27m/[27mr[27me[27mp[27ma[27mi[27mr[27m_[27mo[27mp[27mt[27mi[27mm[27m_[27mr[27me[27ml[27ma[27mt[27mi[27mo[27mn[27ms[27m.[27mp[27my[?2004l
1. 株式会社オプティムの『正しいレコード』を探索中...
 -> [警告] 本家のオプティムが見つかりませんでした。
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[7mPOSTGRES_HOST='34[7m.[7m84.189.233' POSTGRES_PORT='5432' POSTGRES_PASSWORD='Legatu[7ms[7m2000/' python scripts/repair_optim_v3.py[27m[K[A[A[1C[27mP[27mO[27mS[27mT[27mG[27mR[27mE[27mS[27m_[27mH[27mO[27mS[27mT[27m=[27m'[27m3[27m4.[27m8[27m4[27m.[27m1[27m8[27m9[27m.[27m2[27m3[27m3[27m'[27m [27mP[27mO[27mS[27mT[27mG[27mR[27mE[27mS[27m_[27mP[27mO[27mR[27mT[27m=[27m'[27m5[27m4[27m3[27m2[27m'[27m [27mP[27mO[27mS[27mT[27mG[27mR[27mE[27mS[27m_[27mP[27mA[27mS[27mS[27mW[27mO[27mR[27mD[27m=[27m'[27mL[27me[27mg[27ma[27mt[27mus[27m2[27m0[27m0[27m0[27m/[27m'[27m [27mp[27my[27mt[27mh[27mo[27mn[27m [27ms[27mc[27mr[27mi[27mp[27mt[27ms[27m/[27mr[27me[27mp[27ma[27mi[27mr[27m_[27mo[27mp[27mt[27mi[27mm[27m_[27mv[27m3[27m.[27mp[27my[?2004l
1. 中間テーブルから『失われた親ID』を特定中...
 -> 関連会社側からの親特定に失敗しました。名前一致で進めます。

2. 現在の『本命』レコードを選択中...
 -> 本命を ID: 2966731 (住所: 佐賀市本庄町１) に決定しました。

3. 中間テーブルの親IDを 2966731 に統合中...
 -> 0 件の紐付けを修復しました。

4. companiesテーブルへのIDリスト同期を実行中...

成功！ ID: 2966731 のレコードに関連会社が復旧しました。
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[7mPOSTGRES_HOST='34[7m.[7m84.189.233' POSTGRES_PORT='5432' POSTGRES_PASSWORD='Legatu[7ms[7m2000/' python scripts/rebuild_all_relations.py[27m[K[A[A[27mP[27mO[27mS[27mT[27mG[27mR[27mE[27mS[27m_[27mH[27mO[27mS[27mT[27m=[27m'[27m3[27m4.[27m8[27m4[27m.[27m1[27m8[27m9[27m.[27m2[27m3[27m3[27m'[27m [27mP[27mO[27mS[27mT[27mG[27mR[27mE[27mS[27m_[27mP[27mO[27mR[27mT[27m=[27m'[27m5[27m4[27m3[27m2[27m'[27m [27mP[27mO[27mS[27mT[27mG[27mR[27mE[27mS[27m_[27mP[27mA[27mS[27mS[27mW[27mO[27mR[27mD[27m=[27m'[27mL[27me[27mg[27ma[27mt[27mus[27m2[27m0[27m0[27m0[27m/[27m'[27m [27mp[27my[27mt[27mh[27mo[27mn[27m [27ms[27mc[27mr[27mi[27mp[27mt[27ms[27m/[27mr[27me[27mb[27mu[27mi[27ml[27md[27m_[27ma[27ml[27ml[27m_[27mr[27me[27ml[27ma[27mt[27mi[27mo[27mn[27ms[27m.[27mp[27my[?2004l
1. 中間テーブルの child_company_id を最新の企業IDに更新中...
 -> 445 件の子会社IDを最新化しました。

2. 親会社側の company_relation_ids カラムを一括更新中...
 -> 335 件の親企業のリンクを復元しました。

すべての関連会社データの反映が完了しました！
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[7mPOSTGRES_HOST='34[7m.[7m84.189.233' POSTGRES_PORT='5432' POSTGRES_PASSWORD='Legatu[7ms[7m2000/' python scripts/rebuild_all_relations.py[27m[K[A[A[27mP[27mO[27mS[27mT[27mG[27mR[27mE[27mS[27m_[27mH[27mO[27mS[27mT[27m=[27m'[27m3[27m4.[27m8[27m4[27m.[27m1[27m8[27m9[27m.[27m2[27m3[27m3[27m'[27m [27mP[27mO[27mS[27mT[27mG[27mR[27mE[27mS[27m_[27mP[27mO[27mR[27mT[27m=[27m'[27m5[27m4[27m3[27m2[27m'[27m [27mP[27mO[27mS[27mT[27mG[27mR[27mE[27mS[27m_[27mP[27mA[27mS[27mS[27mW[27mO[27mR[27mD[27m=[27m'[27mL[27me[27mg[27ma[27mt[27mus[27m2[27m0[27m0[27m0[27m/[27m'[27m [27mp[27my[27mt[27mh[27mo[27mn[27m [27ms[27mc[27mr[27mi[27mp[27mt[27ms[27m/[27mr[27me[27mb[27mu[27mi[27ml[27md[27m_[27ma[27ml[27ml[27m_[27mr[27me[27ml[27ma[27mt[27mi[27mo[27mn[27ms[27m.[27mp[K                                          s [K[A[58C[K[1B[K[A[58C   gatus2000/' python scripts/rebuild_all_relations.py[K[K                                          s [K[A[58C[K[1B[K[A[58C                                                      . [K[A[58C[K[1B[K[A[58C              P  [?2004l[1B[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[7mpip install panda[7ms[7m sqlalchemy psycopg2-binary[27m[K[A[14C[27mp[27mi[27mp[27m [27mi[27mn[27ms[27mt[27ma[27ml[27ml[27m [27mp[27ma[27mn[27md[27mas[27m [27ms[27mq[27ml[27ma[27ml[27mc[27mh[27me[27mm[27my[27m [27mp[27ms[27my[27mc[27mo[27mp[27mg[27m2[27m-[27mb[27mi[27mn[27ma[27mr[27my[?2004l
Requirement already satisfied: pandas in /Users/harumacmini/.pyenv/versions/3.12.2/lib/python3.12/site-packages (2.3.3)
Requirement already satisfied: sqlalchemy in /Users/harumacmini/.pyenv/versions/3.12.2/lib/python3.12/site-packages (2.0.45)
Requirement already satisfied: psycopg2-binary in /Users/harumacmini/.pyenv/versions/3.12.2/lib/python3.12/site-packages (2.9.11)
Requirement already satisfied: numpy>=1.26.0 in /Users/harumacmini/.pyenv/versions/3.12.2/lib/python3.12/site-packages (from pandas) (2.4.1)
Requirement already satisfied: python-dateutil>=2.8.2 in /Users/harumacmini/.pyenv/versions/3.12.2/lib/python3.12/site-packages (from pandas) (2.9.0.post0)
Requirement already satisfied: pytz>=2020.1 in /Users/harumacmini/.pyenv/versions/3.12.2/lib/python3.12/site-packages (from pandas) (2025.2)
Requirement already satisfied: tzdata>=2022.7 in /Users/harumacmini/.pyenv/versions/3.12.2/lib/python3.12/site-packages (from pandas) (2025.3)
Requirement already satisfied: typing-extensions>=4.6.0 in /Users/harumacmini/.pyenv/versions/3.12.2/lib/python3.12/site-packages (from sqlalchemy) (4.15.0)
Requirement already satisfied: six>=1.5 in /Users/harumacmini/.pyenv/versions/3.12.2/lib/python3.12/site-packages (from python-dateutil>=2.8.2->pandas) (1.17.0)

[1m[[0m[34;49mnotice[0m[1;39;49m][0m[39;49m A new release of pip is available: [0m[31;49m24.0[0m[39;49m -> [0m[32;49m26.0.1[0m
[1m[[0m[34;49mnotice[0m[1;39;49m][0m[39;49m To update, run: [0m[32;49mpip install --upgrade pip[0m
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[7mexport POSTGRES_P[7mA[7mSSWORD=[27m[K[A[34C[27me[27mx[27mp[27mo[27mr[27mt[27m [27mP[27mO[27mS[27mT[27mG[27mR[27mE[27mS[27m_[27mPA[27mS[27mS[27mW[27mO[27mR[27mD[27m=Legatus2000/[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[7mpython scripts/au[7md[7mit_csv_vs_db_integrity.py[27m[K[A[16C[27mp[27my[27mt[27mh[27mo[27mn[27m [27ms[27mc[27mr[27mi[27mp[27mt[27ms[27m/[27ma[27mud[27mi[27mt[27m_[27mc[27ms[27mv[27m_[27mv[27ms[27m_[27md[27mb[27m_[27mi[27mn[27mt[27me[27mg[27mr[27mi[27mt[27my[27m.[27mp[27my[?2004l
対象CSV: 182 件（fixed_csv_3/ 配下、/later/ 除外）
Traceback (most recent call last):
  File "/Users/harumacmini/programming/info_companyDetail/scripts/audit_csv_vs_db_integrity.py", line 454, in <module>
    main()
  File "/Users/harumacmini/programming/info_companyDetail/scripts/audit_csv_vs_db_integrity.py", line 415, in main
    conn = psycopg2.connect(
           ^^^^^^^^^^^^^^^^^
  File "/Users/harumacmini/.pyenv/versions/3.12.2/lib/python3.12/site-packages/psycopg2/__init__.py", line 122, in connect
    conn = _connect(dsn, connection_factory=connection_factory, **kwasync)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
psycopg2.OperationalError: connection to server at "127.0.0.1", port 5432 failed: Connection refused
	Is the server running on that host and accepting TCP/IP connections?

[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[7mbrew services lis[7mt[27m[K[A[41C[27mb[27mr[27me[27mw[27m [27ms[27me[27mr[27mv[27mi[27mc[27me[27ms[27m [27ml[27mi[27mst[?2004l
[?25l[34m⠋[0m JSON API formula.jws.json     Downloading  32.0MB/-------
[K[34m⠋[0m JSON API formula_tap_migratio Downloading   1.9KB/-------
[K[34m⠋[0m JSON API cask.jws.json        Downloading  15.3MB/-------
[K[34m⠋[0m JSON API cask_tap_migrations. Downloading   2.4KB/-------[K[3F[34m⠋[0m JSON API formula.jws.json     Downloading  32.0MB/-------
[K[34m⠋[0m JSON API formula_tap_migratio Downloading   1.9KB/-------
[K[34m⠋[0m JSON API cask.jws.json        Downloading  15.3MB/-------
[K[34m⠋[0m JSON API cask_tap_migrations. Downloading   2.4KB/-------[K[3F[32m✔︎[0m JSON API formula_tap_migratio Downloaded    1.9KB/  1.9KB
[K[32m✔︎[0m JSON API cask_tap_migrations. Downloaded    2.4KB/  2.4KB
[K[34m⠙[0m JSON API formula.jws.json
[K[34m⠙[0m JSON API cask.jws.json        Downloading 258.0KB/-------[K[1F[34m⠙[0m JSON API formula.jws.json     Downloading   1.3MB/-------
[K[34m⠙[0m JSON API cask.jws.json        Downloading   1.9MB/-------[K[1F[34m⠚[0m JSON API formula.jws.json     Downloading   2.6MB/-------
[K[34m⠚[0m JSON API cask.jws.json        Downloading   4.1MB/-------[K[1F[34m⠚[0m JSON API formula.jws.json     Downloading   3.8MB/-------
[K[34m⠚[0m JSON API cask.jws.json        Downloading   6.7MB/-------[K[1F[34m⠞[0m JSON API formula.jws.json     Downloading   5.0MB/-------
[K[34m⠞[0m JSON API cask.jws.json        Downloading   8.3MB/-------[K[1F[34m⠞[0m JSON API formula.jws.json     Downloading   6.1MB/-------
[K[34m⠞[0m JSON API cask.jws.json        Downloading   9.7MB/-------[K[1F[34m⠖[0m JSON API formula.jws.json     Downloading   7.2MB/-------
[K[34m⠖[0m JSON API cask.jws.json        Downloading  11.1MB/-------[K[1F[34m⠖[0m JSON API formula.jws.json     Downloading   8.7MB/-------
[K[34m⠖[0m JSON API cask.jws.json        Downloading  12.5MB/-------[K[1F[34m⠦[0m JSON API formula.jws.json     Downloading   9.8MB/-------
[K[34m⠦[0m JSON API cask.jws.json        Downloading  14.0MB/-------[K[1F[34m⠦[0m JSON API formula.jws.json     Downloading  11.5MB/-------
[K[34m⠦[0m JSON API cask.jws.json        Downloading  15.3MB/-------[K[1F[34m⠴[0m JSON API formula.jws.json     Downloading  18.8MB/-------
[K[32m✔︎[0m JSON API cask.jws.json        Downloaded   15.3MB/ 15.3MB[K[1F[32m✔︎[0m JSON API cask.jws.json        Downloaded   15.3MB/ 15.3MB
[K[34m⠴[0m JSON API formula.jws.json     Downloading  21.8MB/-------[K[0G[34m⠲[0m JSON API formula.jws.json     Downloading  25.9MB/-------[K[0G[34m⠲[0m JSON API formula.jws.json     Downloading  28.6MB/-------[K[0G[34m⠳[0m JSON API formula.jws.json     Downloading  31.9MB/-------[K[0G[34m⠳[0m JSON API formula.jws.json     Downloading  31.9MB/-------[K[0G[32m✔︎[0m JSON API formula.jws.json     Downloaded   31.9MB/ 31.9MB
[K[?25h[33mWarning:[0m No services available to control with `brew services`
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[7mbrew services sta[7mr[7mt postgresql[27m[K[A[29C[27mb[27mr[27me[27mw[27m [27ms[27me[27mr[27mv[27mi[27mc[27me[27ms[27m [27ms[27mt[27mar[27mt[27m [27mp[27mo[27ms[27mt[27mg[27mr[27me[27ms[27mq[27ml[?2004l
[31mError:[0m Formula `postgresql@14` is not installed.
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[7mbrew list | grep [7mp[7mostgres[27m[K[A[34C[27mb[27mr[27me[27mw[27m [27ml[27mi[27ms[27mt[27m [27m|[27m [27mg[27mr[27me[27mp[27m p[27mo[27ms[27mt[27mg[27mr[27me[27ms[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[7mbrew list | grep [7mp[7mostgres[27m[K[A[34C[27mb[27mr[27me[27mw[27m [27ml[27mi[27ms[27mt[27m [27m|[27m [27mg[27mr[27me[27mp[27m p[27mo[27ms[27mt[27mg[27mr[27me[27ms[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[7mbrew install post[7mg[7mresql@16[27m[K[A[33C[27mb[27mr[27me[27mw[27m [27mi[27mn[27ms[27mt[27ma[27ml[27ml[27m [27mp[27mo[27ms[27mtg[27mr[27me[27ms[27mq[27ml[27m@[27m1[27m6[?2004l
[34m==>[0m [1mAuto-updating Homebrew...[0m
Adjust how often this is run with `$HOMEBREW_AUTO_UPDATE_SECS` or disable with
`$HOMEBREW_NO_AUTO_UPDATE=1`. Hide these hints with `$HOMEBREW_NO_ENV_HINTS=1` (see `man brew`).
[32m==>[0m [1mFetching downloads for: [32mpostgresql@16[39m[0m
[?25l[34m⠋[0m Bottle Manifest postgresql@16 (16.12)[K[0G[34m⠋[0m Bottle Manifest postgresql@16 (16.12)[K[0G[34m⠙[0m Bottle Manifest postgresql@16 (16.12)[K[0G[34m⠙[0m Bottle Manifest postgresql@16 (16.12)[K[0G[34m⠚[0m Bottle Manifest postgresql@16 (16.12)[K[0G[34m⠚[0m Bottle Manifest postgresql@16 (16.12)[K[0G[34m⠞[0m Bottle Manifest postgresql@16 (16.12)[K[0G[34m⠞[0m Bottle Manifest postgresql@16 (16.12)[K[0G[34m⠖[0m Bottle Manifest postgresql@16 (16.12)[K[0G[34m⠖[0m Bottle Manifest postgresql@16 Downloading   8.2KB/-------[K[0G[34m⠦[0m Bottle Manifest postgresql@16 Downloading   8.2KB/-------[K[0G[34m⠦[0m Bottle Manifest postgresql@16 Downloading   8.2KB/-------[K[0G[32m✔︎[0m Bottle Manifest postgresql@16 Downloaded   25.8KB/ 25.8KB
[K[?25h[?25l[34m⠴[0m Bottle Manifest icu4c@78 (78.2)
[K[34m⠴[0m Bottle icu4c@78 (78.2)
[K[34m⠴[0m Bottle Manifest openssl@3 (3.6.1)
[K[34m⠴[0m Bottle openssl@3 (3.6.1)
[K[34m⠴[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠴[0m Bottle krb5 (1.22.2)
[K[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle xz (5.8.2)
[K[34m⠴[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠴[0m Bottle zstd (1.5.7_1)
[K[34m⠴[0m Bottle Manifest libunistring (1.4.2)
[K[34m⠴[0m Bottle libunistring (1.4.2)
[K[34m⠴[0m Bottle Manifest gettext (1.0)
[K[34m⠴[0m Bottle gettext (1.0)
[K[34m⠴[0m Bottle postgresql@16 (16.12)[K[14F[34m⠴[0m Bottle Manifest icu4c@78 (78.2)
[K[34m⠴[0m Bottle icu4c@78 (78.2)
[K[34m⠴[0m Bottle Manifest openssl@3 (3.6.1)
[K[34m⠴[0m Bottle openssl@3 (3.6.1)
[K[34m⠴[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠴[0m Bottle krb5 (1.22.2)
[K[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle xz (5.8.2)
[K[34m⠴[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠴[0m Bottle zstd (1.5.7_1)
[K[34m⠴[0m Bottle Manifest libunistring (1.4.2)
[K[34m⠴[0m Bottle libunistring (1.4.2)
[K[34m⠴[0m Bottle Manifest gettext (1.0)
[K[34m⠴[0m Bottle gettext (1.0)
[K[34m⠴[0m Bottle postgresql@16 (16.12)[K[14F[32m✔︎[0m Bottle Manifest icu4c@78 (78. Downloaded    9.7KB/  9.7KB
[K[34m⠲[0m Bottle icu4c@78 (78.2)
[K[34m⠲[0m Bottle Manifest openssl@3 (3.6.1)
[K[34m⠲[0m Bottle openssl@3 (3.6.1)
[K[34m⠲[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠲[0m Bottle krb5 (1.22.2)
[K[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle xz (5.8.2)
[K[34m⠲[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠲[0m Bottle zstd (1.5.7_1)
[K[34m⠲[0m Bottle Manifest libunistring (1.4.2)
[K[34m⠲[0m Bottle libunistring (1.4.2)
[K[34m⠲[0m Bottle Manifest gettext (1.0)
[K[34m⠲[0m Bottle gettext (1.0)
[K[34m⠲[0m Bottle postgresql@16 (16.12)[K[13F[34m⠲[0m Bottle icu4c@78 (78.2)
[K[34m⠲[0m Bottle Manifest openssl@3 (3.6.1)
[K[34m⠲[0m Bottle openssl@3 (3.6.1)
[K[34m⠲[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠲[0m Bottle krb5 (1.22.2)
[K[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle xz (5.8.2)
[K[34m⠲[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠲[0m Bottle zstd (1.5.7_1)
[K[34m⠲[0m Bottle Manifest libunistring (1.4.2)
[K[34m⠲[0m Bottle libunistring (1.4.2)
[K[34m⠲[0m Bottle Manifest gettext (1.0)
[K[34m⠲[0m Bottle gettext (1.0)
[K[34m⠲[0m Bottle postgresql@16 (16.12)[K[13F[34m⠳[0m Bottle icu4c@78 (78.2)
[K[34m⠳[0m Bottle Manifest openssl@3 (3.6.1)
[K[34m⠳[0m Bottle openssl@3 (3.6.1)
[K[34m⠳[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠳[0m Bottle krb5 (1.22.2)
[K[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle xz (5.8.2)
[K[34m⠳[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠳[0m Bottle zstd (1.5.7_1)
[K[34m⠳[0m Bottle Manifest libunistring (1.4.2)
[K[34m⠳[0m Bottle libunistring (1.4.2)
[K[34m⠳[0m Bottle Manifest gettext (1.0)
[K[34m⠳[0m Bottle gettext (1.0)
[K[34m⠳[0m Bottle postgresql@16 (16.12)[K[13F[32m✔︎[0m Bottle Manifest openssl@3 (3. Downloaded   11.8KB/ 11.8KB
[K[32m✔︎[0m Bottle Manifest libunistring  Downloaded    7.3KB/  7.3KB
[K[34m⠳[0m Bottle icu4c@78 (78.2)
[K[34m⠳[0m Bottle openssl@3 (3.6.1)
[K[34m⠳[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠳[0m Bottle krb5 (1.22.2)
[K[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle xz (5.8.2)
[K[34m⠳[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠳[0m Bottle zstd (1.5.7_1)
[K[34m⠳[0m Bottle libunistring (1.4.2)
[K[34m⠳[0m Bottle Manifest gettext (1.0)
[K[34m⠳[0m Bottle gettext (1.0)
[K[34m⠳[0m Bottle postgresql@16 (16.12)[K[11F[34m⠓[0m Bottle icu4c@78 (78.2)
[K[34m⠓[0m Bottle openssl@3 (3.6.1)
[K[34m⠓[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠓[0m Bottle krb5 (1.22.2)
[K[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle xz (5.8.2)
[K[34m⠓[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠓[0m Bottle zstd (1.5.7_1)
[K[34m⠓[0m Bottle libunistring (1.4      Downloading  28.7KB/  1.9MB
[K[34m⠓[0m Bottle Manifest gettext (1.0)
[K[34m⠓[0m Bottle gettext (1.0)
[K[34m⠓[0m Bottle postgresql@16 (16.12)[K[11F[34m⠓[0m Bottle icu4c@78 (78.2)
[K[34m⠓[0m Bottle openssl@3 (3.6.1)
[K[34m⠓[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠓[0m Bottle krb5 (1.22.2)
[K[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle xz (5.8.2)
[K[34m⠓[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠓[0m Bottle zstd (1.5.7_1)
[K[34m⠓[0m Bottle libunistring (1.4 #    Downloading 356.4KB/  1.9MB
[K[34m⠓[0m Bottle Manifest gettext (1.0)
[K[34m⠓[0m Bottle gettext (1.0)
[K[34m⠓[0m Bottle postgresql@16 (16.12)[K[11F[34m⠋[0m Bottle icu4c@78 (78.2)
[K[34m⠋[0m Bottle openssl@3 (3.6.1)
[K[34m⠋[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠋[0m Bottle krb5 (1.22.2)
[K[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle xz (5.8.2)
[K[34m⠋[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠋[0m Bottle zstd (1.5.7_1)
[K[34m⠋[0m Bottle libunistring (1.4 ##   Downloading 843.8KB/  1.9MB
[K[34m⠋[0m Bottle Manifest gettext (1.0)
[K[34m⠋[0m Bottle gettext (1.0)
[K[34m⠋[0m Bottle postgresql@16 (16.12)[K[11F[34m⠋[0m Bottle icu4c@78 (78.2)
[K[34m⠋[0m Bottle openssl@3 (3.6.1)
[K[34m⠋[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠋[0m Bottle krb5 (1.22.2)
[K[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle xz (5.8.2)
[K[34m⠋[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠋[0m Bottle zstd (1.5.7_1)
[K[34m⠋[0m Bottle libunistring (1.4 ###  Downloading   1.4MB/  1.9MB
[K[34m⠋[0m Bottle Manifest gettext (1.0)
[K[34m⠋[0m Bottle gettext (1.0)
[K[34m⠋[0m Bottle postgresql@16 (16.12)[K[11F[34m⠙[0m Bottle icu4c@78 (78.2)
[K[34m⠙[0m Bottle openssl@3 (3.6.1)
[K[34m⠙[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠙[0m Bottle krb5 (1.22.2)          Downloading  36.9KB/-------
[K[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle xz (5.8.2)
[K[34m⠙[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠙[0m Bottle zstd (1.5.7_1)
[K[34m⠙[0m Bottle libunistring (1.4 ###  Downloading   1.6MB/  1.9MB
[K[34m⠙[0m Bottle Manifest gettext (1.0)
[K[34m⠙[0m Bottle gettext (1.0)
[K[34m⠙[0m Bottle postgresql@16 (16.12)[K[11F[34m⠙[0m Bottle icu4c@78 (78.2)
[K[34m⠙[0m Bottle openssl@3 (3.6.1)
[K[34m⠙[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠙[0m Bottle krb5 (1.22.2)          Downloading 127.0KB/-------
[K[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle xz (5.8.2)
[K[34m⠙[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠙[0m Bottle zstd (1.5.7_1)
[K[34m⠙[0m Bottle libunistring (1.4.2)   Extracting    1.9MB/  1.9MB
[K[34m⠙[0m Bottle Manifest gettext (1.0)
[K[34m⠙[0m Bottle gettext (1.0)
[K[34m⠙[0m Bottle postgresql@16 (16.12)[K[11F[32m✔︎[0m Bottle libunistring (1.4.2)   Downloaded    1.9MB/  1.9MB
[K[34m⠚[0m Bottle icu4c@78 (78.2)
[K[34m⠚[0m Bottle openssl@3 (3.6.1)
[K[34m⠚[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠚[0m Bottle krb5 (1.22.2)          Downloading 241.7KB/-------
[K[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle xz (5.8.2)
[K[34m⠚[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠚[0m Bottle zstd (1.5.7_1)
[K[34m⠚[0m Bottle Manifest gettext (1.0)
[K[34m⠚[0m Bottle gettext (1.0)
[K[34m⠚[0m Bottle postgresql@16 (16.12)[K[10F[34m⠚[0m Bottle icu4c@78 (78.2)
[K[34m⠚[0m Bottle openssl@3 (3.6.1)
[K[34m⠚[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠚[0m Bottle krb5 (1.22.2)          Downloading 438.3KB/-------
[K[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle xz (5.8.2)
[K[34m⠚[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠚[0m Bottle zstd (1.5.7_1)
[K[34m⠚[0m Bottle Manifest gettext (1.0)
[K[34m⠚[0m Bottle gettext (1.0)
[K[34m⠚[0m Bottle postgresql@16 (16.12)[K[10F[34m⠞[0m Bottle icu4c@78 (78.2)
[K[34m⠞[0m Bottle openssl@3 (3.6.1)
[K[34m⠞[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠞[0m Bottle krb5 (1.22.2)          Downloading 733.2KB/-------
[K[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle xz (5.8.2)
[K[34m⠞[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠞[0m Bottle zstd (1.5.7_1)
[K[34m⠞[0m Bottle Manifest gettext (1.0)
[K[34m⠞[0m Bottle gettext (1.0)
[K[34m⠞[0m Bottle postgresql@16 (16.12)[K[10F[34m⠞[0m Bottle icu4c@78 (78.2)
[K[34m⠞[0m Bottle openssl@3 (3.6.1)
[K[34m⠞[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠞[0m Bottle krb5 (1.22.2)          Downloading 978.9KB/-------
[K[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle xz (5.8.2)
[K[34m⠞[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠞[0m Bottle zstd (1.5.7_1)
[K[34m⠞[0m Bottle Manifest gettext (1.0)
[K[34m⠞[0m Bottle gettext (1.0)
[K[34m⠞[0m Bottle postgresql@16 (16.12)[K[10F[34m⠖[0m Bottle icu4c@78 (78.2)
[K[34m⠖[0m Bottle openssl@3 (3.6.1)
[K[34m⠖[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠖[0m Bottle krb5 (1.22.2)          Downloading 978.9KB/-------
[K[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle xz (5.8.2)
[K[34m⠖[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠖[0m Bottle zstd (1.5.7_1)
[K[34m⠖[0m Bottle Manifest gettext (1.0)
[K[34m⠖[0m Bottle gettext (1.0)
[K[34m⠖[0m Bottle postgresql@16 (16.12)[K[10F[34m⠖[0m Bottle icu4c@78 (78.2)
[K[34m⠖[0m Bottle openssl@3 (3.6.1)      Downloading  45.1KB/ 10.9MB
[K[34m⠖[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠖[0m Bottle krb5 (1.22.2)          Downloading   1.2MB/-------
[K[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle xz (5.8.2)
[K[34m⠖[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠖[0m Bottle zstd (1.5.7_1)
[K[34m⠖[0m Bottle Manifest gettext (1.0)
[K[34m⠖[0m Bottle gettext (1.0)          Downloading  45.1KB/-------
[K[34m⠖[0m Bottle postgresql@16 (16.12)[K[10F[34m⠦[0m Bottle icu4c@78 (78.2)
[K[34m⠦[0m Bottle openssl@3 (3.6.1)      Downloading 241.7KB/ 10.9MB
[K[34m⠦[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠦[0m Bottle krb5 (1.22.2)          Extracting    1.3MB/-------
[K[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle xz (5.8.2)
[K[34m⠦[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠦[0m Bottle zstd (1.5.7_1)
[K[34m⠦[0m Bottle Manifest gettext (1.0)
[K[34m⠦[0m Bottle gettext (1.0)          Downloading 127.0KB/-------
[K[34m⠦[0m Bottle postgresql@16 (16.12)[K[10F[32m✔︎[0m Bottle krb5 (1.22.2)          Downloaded    1.3MB/  1.3MB
[K[34m⠦[0m Bottle icu4c@78 (78.2)
[K[34m⠦[0m Bottle openssl@3 (3.6.1)      Downloading 536.6KB/ 10.9MB
[K[34m⠦[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle xz (5.8.2)
[K[34m⠦[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠦[0m Bottle zstd (1.5.7_1)
[K[34m⠦[0m Bottle Manifest gettext (1.0)
[K[34m⠦[0m Bottle gettext (1.0)          Downloading 258.0KB/-------
[K[34m⠦[0m Bottle postgresql@16 (16.12)[K[9F[34m⠴[0m Bottle icu4c@78 (78.2)
[K[34m⠴[0m Bottle openssl@3 (3.6.1)      Downloading 798.7KB/ 10.9MB
[K[34m⠴[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle xz (5.8.2)
[K[34m⠴[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠴[0m Bottle zstd (1.5.7_1)
[K[34m⠴[0m Bottle Manifest gettext (1.0)
[K[34m⠴[0m Bottle gettext (1.0)          Downloading 372.7KB/-------
[K[34m⠴[0m Bottle postgresql@16 (16.12)[K[9F[34m⠴[0m Bottle icu4c@78 (78.2)
[K[34m⠴[0m Bottle openssl@3 (3.6.1)      Downloading   1.0MB/ 10.9MB
[K[34m⠴[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle xz (5.8.2)
[K[34m⠴[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠴[0m Bottle zstd (1.5.7_1)
[K[34m⠴[0m Bottle Manifest gettext (1.0)
[K[34m⠴[0m Bottle gettext (1.0)          Downloading 471.0KB/-------
[K[34m⠴[0m Bottle postgresql@16 (16.12)[K[9F[34m⠲[0m Bottle icu4c@78 (78.2)
[K[34m⠲[0m Bottle openssl@3 (3.6.1) #    Downloading   1.4MB/ 10.9MB
[K[34m⠲[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle xz (5.8.2)
[K[34m⠲[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠲[0m Bottle zstd (1.5.7_1)
[K[34m⠲[0m Bottle Manifest gettext (1.0)
[K[34m⠲[0m Bottle gettext (1.0)          Downloading 634.9KB/-------
[K[34m⠲[0m Bottle postgresql@16 (16.12)[K[9F[34m⠲[0m Bottle icu4c@78 (78.2)
[K[34m⠲[0m Bottle openssl@3 (3.6.1) #    Downloading   1.8MB/ 10.9MB
[K[34m⠲[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle xz (5.8.2)
[K[34m⠲[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠲[0m Bottle zstd (1.5.7_1)
[K[34m⠲[0m Bottle Manifest gettext (1.0)
[K[34m⠲[0m Bottle gettext (1.0)          Downloading 798.7KB/-------
[K[34m⠲[0m Bottle postgresql@16 (16.12)[K[9F[34m⠳[0m Bottle icu4c@78 (78.2)
[K[34m⠳[0m Bottle openssl@3 (3.6.1) #    Downloading   2.1MB/ 10.9MB
[K[34m⠳[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle xz (5.8.2)
[K[34m⠳[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠳[0m Bottle zstd (1.5.7_1)
[K[34m⠳[0m Bottle Manifest gettext (1.0)
[K[34m⠳[0m Bottle gettext (1.0)          Downloading 929.8KB/-------
[K[34m⠳[0m Bottle postgresql@16 (16.12)[K[9F[34m⠳[0m Bottle icu4c@78 (78.2)
[K[34m⠳[0m Bottle openssl@3 (3.6.1) #    Downloading   2.4MB/ 10.9MB
[K[34m⠳[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle xz (5.8.2)
[K[34m⠳[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠳[0m Bottle zstd (1.5.7_1)
[K[34m⠳[0m Bottle Manifest gettext (1.0)
[K[34m⠳[0m Bottle gettext (1.0)          Downloading   1.1MB/-------
[K[34m⠳[0m Bottle postgresql@16 (16.12)[K[9F[34m⠓[0m Bottle icu4c@78 (78.2)
[K[34m⠓[0m Bottle openssl@3 (3.6.1) #    Downloading   2.7MB/ 10.9MB
[K[34m⠓[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle xz (5.8.2)
[K[34m⠓[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠓[0m Bottle zstd (1.5.7_1)
[K[34m⠓[0m Bottle Manifest gettext (1.0)
[K[34m⠓[0m Bottle gettext (1.0)          Downloading   1.2MB/-------
[K[34m⠓[0m Bottle postgresql@16 (16.12)[K[9F[34m⠓[0m Bottle icu4c@78 (78.2)
[K[34m⠓[0m Bottle openssl@3 (3.6.1) #    Downloading   3.1MB/ 10.9MB
[K[34m⠓[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle xz (5.8.2)
[K[34m⠓[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠓[0m Bottle zstd (1.5.7_1)
[K[34m⠓[0m Bottle Manifest gettext (1.0)
[K[34m⠓[0m Bottle gettext (1.0)          Downloading   1.4MB/-------
[K[34m⠓[0m Bottle postgresql@16 (16.12)[K[9F[34m⠋[0m Bottle icu4c@78 (78.2)
[K[34m⠋[0m Bottle openssl@3 (3.6.1) #    Downloading   3.4MB/ 10.9MB
[K[34m⠋[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle xz (5.8.2)
[K[34m⠋[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠋[0m Bottle zstd (1.5.7_1)
[K[34m⠋[0m Bottle Manifest gettext (1.0)
[K[34m⠋[0m Bottle gettext (1.0)          Downloading   1.5MB/-------
[K[34m⠋[0m Bottle postgresql@16 (16.12)[K[9F[34m⠋[0m Bottle icu4c@78 (78.2)
[K[34m⠋[0m Bottle openssl@3 (3.6.1) #    Downloading   3.8MB/ 10.9MB
[K[34m⠋[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle xz (5.8.2)
[K[34m⠋[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠋[0m Bottle zstd (1.5.7_1)
[K[34m⠋[0m Bottle Manifest gettext (1.0)
[K[34m⠋[0m Bottle gettext (1.0)          Downloading   1.7MB/-------
[K[34m⠋[0m Bottle postgresql@16 (16.12)[K[9F[34m⠙[0m Bottle icu4c@78 (78.2)
[K[34m⠙[0m Bottle openssl@3 (3.6.1) ##   Downloading   4.1MB/ 10.9MB
[K[34m⠙[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle xz (5.8.2)
[K[34m⠙[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠙[0m Bottle zstd (1.5.7_1)
[K[34m⠙[0m Bottle Manifest gettext (1.0)
[K[34m⠙[0m Bottle gettext (1.0)          Downloading   1.8MB/-------
[K[34m⠙[0m Bottle postgresql@16 (16.12)[K[9F[34m⠙[0m Bottle icu4c@78 (78.2)
[K[34m⠙[0m Bottle openssl@3 (3.6.1) ##   Downloading   4.6MB/ 10.9MB
[K[34m⠙[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle xz (5.8.2)
[K[34m⠙[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠙[0m Bottle zstd (1.5.7_1)
[K[34m⠙[0m Bottle Manifest gettext (1.0)
[K[34m⠙[0m Bottle gettext (1.0)          Downloading   2.0MB/-------
[K[34m⠙[0m Bottle postgresql@16 (16.12)[K[9F[34m⠚[0m Bottle icu4c@78 (78.2)
[K[34m⠚[0m Bottle openssl@3 (3.6.1) ##   Downloading   4.9MB/ 10.9MB
[K[34m⠚[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle xz (5.8.2)
[K[34m⠚[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠚[0m Bottle zstd (1.5.7_1)
[K[34m⠚[0m Bottle Manifest gettext (1.0)
[K[34m⠚[0m Bottle gettext (1.0)          Downloading   2.1MB/-------
[K[34m⠚[0m Bottle postgresql@16 (16.12)[K[9F[34m⠚[0m Bottle icu4c@78 (78.2)
[K[34m⠚[0m Bottle openssl@3 (3.6.1) ##   Downloading   5.3MB/ 10.9MB
[K[34m⠚[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle xz (5.8.2)
[K[34m⠚[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠚[0m Bottle zstd (1.5.7_1)
[K[34m⠚[0m Bottle Manifest gettext (1.0)
[K[34m⠚[0m Bottle gettext (1.0)          Downloading   2.3MB/-------
[K[34m⠚[0m Bottle postgresql@16 (16.12)[K[9F[34m⠞[0m Bottle icu4c@78 (78.2)
[K[34m⠞[0m Bottle openssl@3 (3.6.1) ##   Downloading   5.6MB/ 10.9MB
[K[34m⠞[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle xz (5.8.2)
[K[34m⠞[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠞[0m Bottle zstd (1.5.7_1)
[K[34m⠞[0m Bottle Manifest gettext (1.0)
[K[34m⠞[0m Bottle gettext (1.0)          Downloading   2.5MB/-------
[K[34m⠞[0m Bottle postgresql@16 (16.12)[K[9F[34m⠞[0m Bottle icu4c@78 (78.2)
[K[34m⠞[0m Bottle openssl@3 (3.6.1) ##   Downloading   6.1MB/ 10.9MB
[K[34m⠞[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle xz (5.8.2)
[K[34m⠞[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠞[0m Bottle zstd (1.5.7_1)
[K[34m⠞[0m Bottle Manifest gettext (1.0)
[K[34m⠞[0m Bottle gettext (1.0)          Downloading   2.6MB/-------
[K[34m⠞[0m Bottle postgresql@16 (16.12)[K[9F[34m⠖[0m Bottle icu4c@78 (78.2)
[K[34m⠖[0m Bottle openssl@3 (3.6.1) ##   Downloading   6.4MB/ 10.9MB
[K[34m⠖[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle xz (5.8.2)
[K[34m⠖[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠖[0m Bottle zstd (1.5.7_1)
[K[34m⠖[0m Bottle Manifest gettext (1.0)
[K[34m⠖[0m Bottle gettext (1.0)          Downloading   2.8MB/-------
[K[34m⠖[0m Bottle postgresql@16 (16.12)[K[9F[34m⠖[0m Bottle icu4c@78 (78.2)
[K[34m⠖[0m Bottle openssl@3 (3.6.1) ##   Downloading   6.7MB/ 10.9MB
[K[34m⠖[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle xz (5.8.2)
[K[34m⠖[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠖[0m Bottle zstd (1.5.7_1)
[K[34m⠖[0m Bottle Manifest gettext (1.0)
[K[34m⠖[0m Bottle gettext (1.0)          Downloading   2.9MB/-------
[K[34m⠖[0m Bottle postgresql@16 (16.12)[K[9F[34m⠦[0m Bottle icu4c@78 (78.2)
[K[34m⠦[0m Bottle openssl@3 (3.6.1) ###  Downloading   6.9MB/ 10.9MB
[K[34m⠦[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle xz (5.8.2)
[K[34m⠦[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠦[0m Bottle zstd (1.5.7_1)
[K[34m⠦[0m Bottle Manifest gettext (1.0)
[K[34m⠦[0m Bottle gettext (1.0)          Downloading   3.0MB/-------
[K[34m⠦[0m Bottle postgresql@16 (16.12)[K[9F[34m⠦[0m Bottle icu4c@78 (78.2)
[K[34m⠦[0m Bottle openssl@3 (3.6.1) ###  Downloading   7.2MB/ 10.9MB
[K[34m⠦[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle xz (5.8.2)
[K[34m⠦[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠦[0m Bottle zstd (1.5.7_1)
[K[34m⠦[0m Bottle Manifest gettext (1.0)
[K[34m⠦[0m Bottle gettext (1.0)          Downloading   3.1MB/-------
[K[34m⠦[0m Bottle postgresql@16 (16.12)[K[9F[34m⠴[0m Bottle icu4c@78 (78.2)
[K[34m⠴[0m Bottle openssl@3 (3.6.1) ###  Downloading   7.3MB/ 10.9MB
[K[34m⠴[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle xz (5.8.2)
[K[34m⠴[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠴[0m Bottle zstd (1.5.7_1)
[K[34m⠴[0m Bottle Manifest gettext (1.0)
[K[34m⠴[0m Bottle gettext (1.0)          Downloading   3.2MB/-------
[K[34m⠴[0m Bottle postgresql@16 (16.12)[K[9F[34m⠴[0m Bottle icu4c@78 (78.2)
[K[34m⠴[0m Bottle openssl@3 (3.6.1) ###  Downloading   7.9MB/ 10.9MB
[K[34m⠴[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle xz (5.8.2)
[K[34m⠴[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠴[0m Bottle zstd (1.5.7_1)
[K[34m⠴[0m Bottle Manifest gettext (1.0)
[K[34m⠴[0m Bottle gettext (1.0)          Downloading   3.2MB/-------
[K[34m⠴[0m Bottle postgresql@16 (16.12)[K[9F[34m⠲[0m Bottle icu4c@78 (78.2)
[K[34m⠲[0m Bottle openssl@3 (3.6.1) ###  Downloading   8.1MB/ 10.9MB
[K[34m⠲[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle xz (5.8.2)
[K[34m⠲[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠲[0m Bottle zstd (1.5.7_1)
[K[34m⠲[0m Bottle Manifest gettext (1.0)
[K[34m⠲[0m Bottle gettext (1.0)          Downloading   3.5MB/-------
[K[34m⠲[0m Bottle postgresql@16 (16.12)[K[9F[34m⠲[0m Bottle icu4c@78 (78.2)
[K[34m⠲[0m Bottle openssl@3 (3.6.1) ###  Downloading   8.4MB/ 10.9MB
[K[34m⠲[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle xz (5.8.2)
[K[34m⠲[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠲[0m Bottle zstd (1.5.7_1)
[K[34m⠲[0m Bottle Manifest gettext (1.0)
[K[34m⠲[0m Bottle gettext (1.0)          Downloading   3.6MB/-------
[K[34m⠲[0m Bottle postgresql@16 (16.12)[K[9F[34m⠳[0m Bottle icu4c@78 (78.2)
[K[34m⠳[0m Bottle openssl@3 (3.6.1) ###  Downloading   8.6MB/ 10.9MB
[K[34m⠳[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle xz (5.8.2)             Downloading  20.5KB/-------
[K[34m⠳[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠳[0m Bottle zstd (1.5.7_1)
[K[34m⠳[0m Bottle Manifest gettext (1.0)
[K[34m⠳[0m Bottle gettext (1.0)          Downloading   3.7MB/-------
[K[34m⠳[0m Bottle postgresql@16 (16.12)[K[9F[34m⠳[0m Bottle icu4c@78 (78.2)
[K[34m⠳[0m Bottle openssl@3 (3.6.1) ###  Downloading   8.6MB/ 10.9MB
[K[34m⠳[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle xz (5.8.2)             Downloading  32.8KB/-------
[K[34m⠳[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠳[0m Bottle zstd (1.5.7_1)
[K[34m⠳[0m Bottle Manifest gettext (1.0)
[K[34m⠳[0m Bottle gettext (1.0)          Downloading   3.7MB/-------
[K[34m⠳[0m Bottle postgresql@16 (16.12)[K[9F[34m⠓[0m Bottle icu4c@78 (78.2)
[K[34m⠓[0m Bottle openssl@3 (3.6.1) ###  Downloading   8.9MB/ 10.9MB
[K[34m⠓[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle xz (5.8.2)             Downloading  36.9KB/-------
[K[34m⠓[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠓[0m Bottle zstd (1.5.7_1)
[K[34m⠓[0m Bottle Manifest gettext (1.0)
[K[34m⠓[0m Bottle gettext (1.0)          Downloading   3.8MB/-------
[K[34m⠓[0m Bottle postgresql@16 (16.12)[K[9F[34m⠓[0m Bottle icu4c@78 (78.2)
[K[34m⠓[0m Bottle openssl@3 (3.6.1) ###  Downloading   9.1MB/ 10.9MB
[K[34m⠓[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle xz (5.8.2)             Downloading 110.6KB/-------
[K[34m⠓[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠓[0m Bottle zstd (1.5.7_1)
[K[34m⠓[0m Bottle Manifest gettext (1.0)
[K[34m⠓[0m Bottle gettext (1.0)          Downloading   3.9MB/-------
[K[34m⠓[0m Bottle postgresql@16 (16.12)[K[9F[34m⠋[0m Bottle icu4c@78 (78.2)
[K[34m⠋[0m Bottle openssl@3 (3.6.1) ###  Downloading   9.4MB/ 10.9MB
[K[34m⠋[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle xz (5.8.2)             Downloading 192.5KB/-------
[K[34m⠋[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠋[0m Bottle zstd (1.5.7_1)
[K[34m⠋[0m Bottle Manifest gettext (1.0)
[K[34m⠋[0m Bottle gettext (1.0)          Downloading   4.0MB/-------
[K[34m⠋[0m Bottle postgresql@16 (16.12)[K[9F[34m⠋[0m Bottle icu4c@78 (78.2)
[K[34m⠋[0m Bottle openssl@3 (3.6.1) #### Downloading   9.7MB/ 10.9MB
[K[34m⠋[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle xz (5.8.2)             Downloading 290.8KB/-------
[K[34m⠋[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠋[0m Bottle zstd (1.5.7_1)
[K[34m⠋[0m Bottle Manifest gettext (1.0)
[K[34m⠋[0m Bottle gettext (1.0)          Downloading   4.2MB/-------
[K[34m⠋[0m Bottle postgresql@16 (16.12)[K[9F[34m⠙[0m Bottle icu4c@78 (78.2)
[K[34m⠙[0m Bottle openssl@3 (3.6.1) #### Downloading   9.9MB/ 10.9MB
[K[34m⠙[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle xz (5.8.2)             Downloading 340.0KB/-------
[K[34m⠙[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠙[0m Bottle zstd (1.5.7_1)
[K[34m⠙[0m Bottle Manifest gettext (1.0)
[K[34m⠙[0m Bottle gettext (1.0)          Downloading   4.3MB/-------
[K[34m⠙[0m Bottle postgresql@16 (16.12)[K[9F[34m⠙[0m Bottle icu4c@78 (78.2)
[K[34m⠙[0m Bottle openssl@3 (3.6.1) #### Downloading  10.1MB/ 10.9MB
[K[34m⠙[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle xz (5.8.2)             Downloading 421.9KB/-------
[K[34m⠙[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠙[0m Bottle zstd (1.5.7_1)
[K[34m⠙[0m Bottle Manifest gettext (1.0)
[K[34m⠙[0m Bottle gettext (1.0)          Downloading   4.3MB/-------
[K[34m⠙[0m Bottle postgresql@16 (16.12)[K[9F[34m⠚[0m Bottle icu4c@78 (78.2)
[K[34m⠚[0m Bottle openssl@3 (3.6.1) #### Downloading  10.3MB/ 10.9MB
[K[34m⠚[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle xz (5.8.2)             Downloading 487.4KB/-------
[K[34m⠚[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠚[0m Bottle zstd (1.5.7_1)
[K[34m⠚[0m Bottle Manifest gettext (1.0)
[K[34m⠚[0m Bottle gettext (1.0)          Downloading   4.4MB/-------
[K[34m⠚[0m Bottle postgresql@16 (16.12)[K[9F[34m⠚[0m Bottle icu4c@78 (78.2)
[K[34m⠚[0m Bottle openssl@3 (3.6.1) #### Downloading  10.3MB/ 10.9MB
[K[34m⠚[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle xz (5.8.2)             Downloading 487.4KB/-------
[K[34m⠚[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠚[0m Bottle zstd (1.5.7_1)
[K[34m⠚[0m Bottle Manifest gettext (1.0)
[K[34m⠚[0m Bottle gettext (1.0)          Downloading   4.4MB/-------
[K[34m⠚[0m Bottle postgresql@16 (16.12)[K[9F[34m⠞[0m Bottle icu4c@78 (78.2)
[K[34m⠞[0m Bottle openssl@3 (3.6.1) #### Downloading  10.9MB/ 10.9MB
[K[34m⠞[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle xz (5.8.2)             Downloading 716.8KB/-------
[K[34m⠞[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠞[0m Bottle zstd (1.5.7_1)
[K[34m⠞[0m Bottle Manifest gettext (1.0)
[K[34m⠞[0m Bottle gettext (1.0)          Downloading   4.7MB/-------
[K[34m⠞[0m Bottle postgresql@16 (16.12)[K[9F[32m✔︎[0m Bottle xz (5.8.2)             Downloaded  764.3KB/764.3KB
[K[34m⠞[0m Bottle icu4c@78 (78.2)
[K[34m⠞[0m Bottle openssl@3 (3.6.1)      Extracting   10.9MB/ 10.9MB
[K[34m⠞[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠞[0m Bottle zstd (1.5.7_1)
[K[34m⠞[0m Bottle Manifest gettext (1.0)
[K[34m⠞[0m Bottle gettext (1.0)          Downloading   5.0MB/-------
[K[34m⠞[0m Bottle postgresql@16 (16.12)[K[8F[34m⠖[0m Bottle icu4c@78 (78.2)
[K[34m⠖[0m Bottle openssl@3 (3.6.1)      Extracting   10.9MB/ 10.9MB
[K[34m⠖[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠖[0m Bottle zstd (1.5.7_1)
[K[34m⠖[0m Bottle Manifest gettext (1.0)
[K[34m⠖[0m Bottle gettext (1.0)          Downloading   5.1MB/-------
[K[34m⠖[0m Bottle postgresql@16 (16.12)[K[8F[34m⠖[0m Bottle icu4c@78 (78.2)
[K[34m⠖[0m Bottle openssl@3 (3.6.1)      Extracting   10.9MB/ 10.9MB
[K[34m⠖[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠖[0m Bottle zstd (1.5.7_1)
[K[34m⠖[0m Bottle Manifest gettext (1.0)
[K[34m⠖[0m Bottle gettext (1.0)          Downloading   5.5MB/-------
[K[34m⠖[0m Bottle postgresql@16 (16.12)[K[8F[34m⠦[0m Bottle icu4c@78 (78.2)
[K[34m⠦[0m Bottle openssl@3 (3.6.1)      Extracting   10.9MB/ 10.9MB
[K[34m⠦[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠦[0m Bottle zstd (1.5.7_1)
[K[34m⠦[0m Bottle Manifest gettext (1.0)
[K[34m⠦[0m Bottle gettext (1.0)          Downloading   5.7MB/-------
[K[34m⠦[0m Bottle postgresql@16 (16.12)[K[8F[34m⠦[0m Bottle icu4c@78 (78.2)
[K[34m⠦[0m Bottle openssl@3 (3.6.1)      Extracting   10.9MB/ 10.9MB
[K[34m⠦[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠦[0m Bottle zstd (1.5.7_1)
[K[34m⠦[0m Bottle Manifest gettext (1.0)
[K[34m⠦[0m Bottle gettext (1.0)          Downloading   5.9MB/-------
[K[34m⠦[0m Bottle postgresql@16 (16.12)[K[8F[34m⠴[0m Bottle icu4c@78 (78.2)
[K[34m⠴[0m Bottle openssl@3 (3.6.1)      Extracting   10.9MB/ 10.9MB
[K[34m⠴[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠴[0m Bottle zstd (1.5.7_1)
[K[34m⠴[0m Bottle Manifest gettext (1.0)
[K[34m⠴[0m Bottle gettext (1.0)          Downloading   6.3MB/-------
[K[34m⠴[0m Bottle postgresql@16 (16.12)[K[8F[34m⠴[0m Bottle icu4c@78 (78.2)
[K[34m⠴[0m Bottle openssl@3 (3.6.1)      Extracting   10.9MB/ 10.9MB
[K[34m⠴[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠴[0m Bottle zstd (1.5.7_1)
[K[34m⠴[0m Bottle Manifest gettext (1.0)
[K[34m⠴[0m Bottle gettext (1.0)          Downloading   6.7MB/-------
[K[34m⠴[0m Bottle postgresql@16 (16.12)[K[8F[34m⠲[0m Bottle icu4c@78 (78.2)
[K[34m⠲[0m Bottle openssl@3 (3.6.1)      Extracting   10.9MB/ 10.9MB
[K[34m⠲[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠲[0m Bottle zstd (1.5.7_1)
[K[34m⠲[0m Bottle Manifest gettext (1.0)
[K[34m⠲[0m Bottle gettext (1.0)          Downloading   7.0MB/-------
[K[34m⠲[0m Bottle postgresql@16 (16.12)[K[8F[34m⠲[0m Bottle icu4c@78 (78.2)
[K[34m⠲[0m Bottle openssl@3 (3.6.1)      Extracting   10.9MB/ 10.9MB
[K[34m⠲[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠲[0m Bottle zstd (1.5.7_1)
[K[34m⠲[0m Bottle Manifest gettext (1.0)
[K[34m⠲[0m Bottle gettext (1.0)          Downloading   7.4MB/-------
[K[34m⠲[0m Bottle postgresql@16 (16.12)[K[8F[34m⠳[0m Bottle icu4c@78 (78.2)
[K[34m⠳[0m Bottle openssl@3 (3.6.1)      Extracting   10.9MB/ 10.9MB
[K[34m⠳[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠳[0m Bottle zstd (1.5.7_1)
[K[34m⠳[0m Bottle Manifest gettext (1.0)
[K[34m⠳[0m Bottle gettext (1.0)          Downloading   7.7MB/-------
[K[34m⠳[0m Bottle postgresql@16 (16      Downloading 127.0KB/ 19.1MB[K[8F[34m⠳[0m Bottle icu4c@78 (78.2)
[K[34m⠳[0m Bottle openssl@3 (3.6.1)      Extracting   10.9MB/ 10.9MB
[K[34m⠳[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠳[0m Bottle zstd (1.5.7_1)
[K[34m⠳[0m Bottle Manifest gettext (1.0)
[K[34m⠳[0m Bottle gettext (1.0)          Downloading   8.0MB/-------
[K[34m⠳[0m Bottle postgresql@16 (16      Downloading 356.4KB/ 19.1MB[K[8F[34m⠓[0m Bottle icu4c@78 (78.2)
[K[34m⠓[0m Bottle openssl@3 (3.6.1)      Extracting   10.9MB/ 10.9MB
[K[34m⠓[0m Bottle Manifest krb5 (1.22.2)
[K[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠓[0m Bottle zstd (1.5.7_1)
[K[34m⠓[0m Bottle Manifest gettext (1.0)
[K[34m⠓[0m Bottle gettext (1.0)          Downloading   8.2MB/-------
[K[34m⠓[0m Bottle postgresql@16 (16      Downloading 585.7KB/ 19.1MB[K[8F[32m✔︎[0m Bottle Manifest krb5 (1.22.2) Downloaded   16.3KB/ 16.3KB
[K[34m⠓[0m Bottle icu4c@78 (78.2)
[K[34m⠓[0m Bottle openssl@3 (3.6.1)      Extracting   10.9MB/ 10.9MB
[K[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠓[0m Bottle zstd (1.5.7_1)
[K[34m⠓[0m Bottle Manifest gettext (1.0)
[K[34m⠓[0m Bottle gettext (1.0)          Downloading   8.5MB/-------
[K[34m⠓[0m Bottle postgresql@16 (16      Downloading 815.1KB/ 19.1MB[K[7F[32m✔︎[0m Bottle openssl@3 (3.6.1)      Downloaded   10.9MB/ 10.9MB
[K[34m⠋[0m Bottle icu4c@78 (78.2)
[K[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠋[0m Bottle zstd (1.5.7_1)
[K[34m⠋[0m Bottle Manifest gettext (1.0)
[K[34m⠋[0m Bottle gettext (1.0)          Downloading   8.7MB/-------
[K[34m⠋[0m Bottle postgresql@16 (16      Downloading   1.1MB/ 19.1MB[K[6F[34m⠋[0m Bottle icu4c@78 (78.2)
[K[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠋[0m Bottle zstd (1.5.7_1)
[K[34m⠋[0m Bottle Manifest gettext (1.0)
[K[34m⠋[0m Bottle gettext (1.0)          Downloading   8.9MB/-------
[K[34m⠋[0m Bottle postgresql@16 (16      Downloading   1.3MB/ 19.1MB[K[6F[34m⠙[0m Bottle icu4c@78 (78.2)
[K[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠙[0m Bottle zstd (1.5.7_1)
[K[34m⠙[0m Bottle Manifest gettext (1.0)
[K[34m⠙[0m Bottle gettext (1.0)          Downloading   9.1MB/-------
[K[34m⠙[0m Bottle postgresql@16 (16      Downloading   1.5MB/ 19.1MB[K[6F[34m⠙[0m Bottle icu4c@78 (78.2)
[K[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle Manifest zstd (1.5.7_1)
[K[34m⠙[0m Bottle zstd (1.5.7_1)
[K[34m⠙[0m Bottle Manifest gettext (1.0)
[K[34m⠙[0m Bottle gettext (1.0)          Downloading   9.2MB/-------
[K[34m⠙[0m Bottle postgresql@16 (16      Downloading   1.6MB/ 19.1MB[K[6F[34m⠚[0m Bottle icu4c@78 (78.2)
[K[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle Manifest zstd (1.5.7_1 Downloading  13.2KB/-------
[K[34m⠚[0m Bottle zstd (1.5.7_1)
[K[34m⠚[0m Bottle Manifest gettext (1.0)
[K[34m⠚[0m Bottle gettext (1.0)          Downloading   9.4MB/-------
[K[34m⠚[0m Bottle postgresql@16 (16      Downloading   1.8MB/ 19.1MB[K[6F[32m✔︎[0m Bottle Manifest zstd (1.5.7_1 Downloaded   13.2KB/ 13.2KB
[K[34m⠚[0m Bottle icu4c@78 (78.2)
[K[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle zstd (1.5.7_1)
[K[34m⠚[0m Bottle Manifest gettext (1.0)
[K[34m⠚[0m Bottle gettext (1.0)          Downloading   9.5MB/-------
[K[34m⠚[0m Bottle postgresql@16 (16      Downloading   2.0MB/ 19.1MB[K[5F[34m⠞[0m Bottle icu4c@78 (78.2)
[K[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle zstd (1.5.7_1)
[K[34m⠞[0m Bottle Manifest gettext (1.0)
[K[34m⠞[0m Bottle gettext (1.0)          Downloading   9.6MB/-------
[K[34m⠞[0m Bottle postgresql@16 (16      Downloading   2.1MB/ 19.1MB[K[5F[34m⠞[0m Bottle icu4c@78 (78.2)
[K[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle zstd (1.5.7_1)
[K[34m⠞[0m Bottle Manifest gettext (1.0)
[K[34m⠞[0m Bottle gettext (1.0)          Downloading   9.8MB/-------
[K[34m⠞[0m Bottle postgresql@16 (16 #    Downloading   2.4MB/ 19.1MB[K[5F[34m⠖[0m Bottle icu4c@78 (78.2)
[K[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle zstd (1.5.7_1)
[K[34m⠖[0m Bottle Manifest gettext (1.0)
[K[34m⠖[0m Bottle gettext (1.0)          Downloading  10.0MB/-------
[K[34m⠖[0m Bottle postgresql@16 (16 #    Downloading   2.6MB/ 19.1MB[K[5F[34m⠖[0m Bottle icu4c@78 (78.2)
[K[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle zstd (1.5.7_1)
[K[34m⠖[0m Bottle Manifest gettext (1.0)
[K[34m⠖[0m Bottle gettext (1.0)          Downloading  10.2MB/-------
[K[34m⠖[0m Bottle postgresql@16 (16 #    Downloading   3.0MB/ 19.1MB[K[5F[34m⠦[0m Bottle icu4c@78 (78.2)
[K[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle zstd (1.5.7_1)
[K[34m⠦[0m Bottle Manifest gettext (1.0)
[K[34m⠦[0m Bottle gettext (1.0)          Extracting   10.2MB/-------
[K[34m⠦[0m Bottle postgresql@16 (16 #    Downloading   3.3MB/ 19.1MB[K[5F[34m⠦[0m Bottle icu4c@78 (78.2)
[K[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle zstd (1.5.7_1)
[K[34m⠦[0m Bottle Manifest gettext (1.0)
[K[34m⠦[0m Bottle gettext (1.0)          Extracting   10.2MB/-------
[K[34m⠦[0m Bottle postgresql@16 (16 #    Downloading   3.8MB/ 19.1MB[K[5F[34m⠴[0m Bottle icu4c@78 (78.2)
[K[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle zstd (1.5.7_1)
[K[34m⠴[0m Bottle Manifest gettext (1.0)
[K[34m⠴[0m Bottle gettext (1.0)          Extracting   10.2MB/-------
[K[34m⠴[0m Bottle postgresql@16 (16 #    Downloading   4.4MB/ 19.1MB[K[5F[34m⠴[0m Bottle icu4c@78 (78.2)
[K[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle zstd (1.5.7_1)
[K[34m⠴[0m Bottle Manifest gettext (1.0)
[K[34m⠴[0m Bottle gettext (1.0)          Extracting   10.2MB/-------
[K[34m⠴[0m Bottle postgresql@16 (16 #    Downloading   4.9MB/ 19.1MB[K[5F[34m⠲[0m Bottle icu4c@78 (78.2)
[K[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle zstd (1.5.7_1)
[K[34m⠲[0m Bottle Manifest gettext (1.0)
[K[34m⠲[0m Bottle gettext (1.0)          Extracting   10.2MB/-------
[K[34m⠲[0m Bottle postgresql@16 (16 #    Downloading   5.4MB/ 19.1MB[K[5F[34m⠲[0m Bottle icu4c@78 (78.2)
[K[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle zstd (1.5.7_1)
[K[34m⠲[0m Bottle Manifest gettext (1.0)
[K[34m⠲[0m Bottle gettext (1.0)          Extracting   10.2MB/-------
[K[34m⠲[0m Bottle postgresql@16 (16 #    Downloading   5.9MB/ 19.1MB[K[5F[32m✔︎[0m Bottle gettext (1.0)          Downloaded   10.2MB/ 10.2MB
[K[34m⠳[0m Bottle icu4c@78 (78.2)
[K[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle zstd (1.5.7_1)
[K[34m⠳[0m Bottle Manifest gettext (1.0)
[K[34m⠳[0m Bottle postgresql@16 (16 #    Downloading   6.5MB/ 19.1MB[K[4F[34m⠳[0m Bottle icu4c@78 (78.2)
[K[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle zstd (1.5.7_1)
[K[34m⠳[0m Bottle Manifest gettext (1.0)
[K[34m⠳[0m Bottle postgresql@16 (16 #    Downloading   7.1MB/ 19.1MB[K[4F[34m⠓[0m Bottle icu4c@78 (78.2)
[K[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle zstd (1.5.7_1)
[K[34m⠓[0m Bottle Manifest gettext (1.0)
[K[34m⠓[0m Bottle postgresql@16 (16 ##   Downloading   7.5MB/ 19.1MB[K[4F[34m⠓[0m Bottle icu4c@78 (78.2)
[K[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle zstd (1.5.7_1)
[K[34m⠓[0m Bottle Manifest gettext (1.0)
[K[34m⠓[0m Bottle postgresql@16 (16 ##   Downloading   8.0MB/ 19.1MB[K[4F[34m⠋[0m Bottle icu4c@78 (78.2)
[K[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle zstd (1.5.7_1)
[K[34m⠋[0m Bottle Manifest gettext (1.0)
[K[34m⠋[0m Bottle postgresql@16 (16 ##   Downloading   8.6MB/ 19.1MB[K[4F[34m⠋[0m Bottle icu4c@78 (78.2)
[K[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle zstd (1.5.7_1)
[K[34m⠋[0m Bottle Manifest gettext (1.0)
[K[34m⠋[0m Bottle postgresql@16 (16 ##   Downloading   9.1MB/ 19.1MB[K[4F[34m⠙[0m Bottle icu4c@78 (78.2)
[K[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle zstd (1.5.7_1)
[K[34m⠙[0m Bottle Manifest gettext (1.0)
[K[34m⠙[0m Bottle postgresql@16 (16 ##   Downloading   9.6MB/ 19.1MB[K[4F[34m⠙[0m Bottle icu4c@78 (78.2)
[K[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle zstd (1.5.7_1)
[K[34m⠙[0m Bottle Manifest gettext (1.0)
[K[34m⠙[0m Bottle postgresql@16 (16 ##   Downloading  10.1MB/ 19.1MB[K[4F[34m⠚[0m Bottle icu4c@78 (78.2)
[K[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle zstd (1.5.7_1)
[K[34m⠚[0m Bottle Manifest gettext (1.0)
[K[34m⠚[0m Bottle postgresql@16 (16 ##   Downloading  10.5MB/ 19.1MB[K[4F[34m⠚[0m Bottle icu4c@78 (78.2)
[K[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle zstd (1.5.7_1)
[K[34m⠚[0m Bottle Manifest gettext (1.0)
[K[34m⠚[0m Bottle postgresql@16 (16 ##   Downloading  10.8MB/ 19.1MB[K[4F[34m⠞[0m Bottle icu4c@78 (78.2)
[K[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle zstd (1.5.7_1)
[K[34m⠞[0m Bottle Manifest gettext (1.0)
[K[34m⠞[0m Bottle postgresql@16 (16 ##   Downloading  11.3MB/ 19.1MB[K[4F[34m⠞[0m Bottle icu4c@78 (78.2)
[K[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle zstd (1.5.7_1)
[K[34m⠞[0m Bottle Manifest gettext (1.0)
[K[34m⠞[0m Bottle postgresql@16 (16 ##   Downloading  11.7MB/ 19.1MB[K[4F[34m⠖[0m Bottle icu4c@78 (78.2)
[K[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle zstd (1.5.7_1)
[K[34m⠖[0m Bottle Manifest gettext (1.0)
[K[34m⠖[0m Bottle postgresql@16 (16 ###  Downloading  12.1MB/ 19.1MB[K[4F[34m⠖[0m Bottle icu4c@78 (78.2)
[K[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle zstd (1.5.7_1)
[K[34m⠖[0m Bottle Manifest gettext (1.0)
[K[34m⠖[0m Bottle postgresql@16 (16 ###  Downloading  12.4MB/ 19.1MB[K[4F[34m⠦[0m Bottle icu4c@78 (78.2)
[K[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle zstd (1.5.7_1)
[K[34m⠦[0m Bottle Manifest gettext (1.0)
[K[34m⠦[0m Bottle postgresql@16 (16 ###  Downloading  12.9MB/ 19.1MB[K[4F[34m⠦[0m Bottle icu4c@78 (78.2)
[K[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle zstd (1.5.7_1)
[K[34m⠦[0m Bottle Manifest gettext (1.0)
[K[34m⠦[0m Bottle postgresql@16 (16 ###  Downloading  12.9MB/ 19.1MB[K[4F[34m⠴[0m Bottle icu4c@78 (78.2)
[K[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle zstd (1.5.7_1)
[K[34m⠴[0m Bottle Manifest gettext (1.0)
[K[34m⠴[0m Bottle postgresql@16 (16 ###  Downloading  13.8MB/ 19.1MB[K[4F[34m⠴[0m Bottle icu4c@78 (78.2)
[K[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle zstd (1.5.7_1)
[K[34m⠴[0m Bottle Manifest gettext (1.0)
[K[34m⠴[0m Bottle postgresql@16 (16 ###  Downloading  14.2MB/ 19.1MB[K[4F[34m⠲[0m Bottle icu4c@78 (78.2)
[K[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle zstd (1.5.7_1)         Downloading  36.9KB/793.6KB
[K[34m⠲[0m Bottle Manifest gettext (1.0)
[K[34m⠲[0m Bottle postgresql@16 (16 ###  Downloading  14.6MB/ 19.1MB[K[4F[34m⠲[0m Bottle icu4c@78 (78.2)
[K[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle zstd (1.5.7_1)         Downloading  77.8KB/793.6KB
[K[34m⠲[0m Bottle Manifest gettext (1.0)
[K[34m⠲[0m Bottle postgresql@16 (16 ###  Downloading  15.1MB/ 19.1MB[K[4F[34m⠳[0m Bottle icu4c@78 (78.2)
[K[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle zstd (1.5.7_1)    #    Downloading 110.6KB/793.6KB
[K[34m⠳[0m Bottle Manifest gettext (1.0)
[K[34m⠳[0m Bottle postgresql@16 (16 ###  Downloading  15.5MB/ 19.1MB[K[4F[34m⠳[0m Bottle icu4c@78 (78.2)
[K[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle zstd (1.5.7_1)    #    Downloading 127.0KB/793.6KB
[K[34m⠳[0m Bottle Manifest gettext (1.0)
[K[34m⠳[0m Bottle postgresql@16 (16 ###  Downloading  15.6MB/ 19.1MB[K[4F[34m⠓[0m Bottle icu4c@78 (78.2)
[K[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle zstd (1.5.7_1)    #    Downloading 176.1KB/793.6KB
[K[34m⠓[0m Bottle Manifest gettext (1.0)
[K[34m⠓[0m Bottle postgresql@16 (16 ###  Downloading  16.0MB/ 19.1MB[K[4F[34m⠓[0m Bottle icu4c@78 (78.2)
[K[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle zstd (1.5.7_1)    #    Downloading 225.3KB/793.6KB
[K[34m⠓[0m Bottle Manifest gettext (1.0)
[K[34m⠓[0m Bottle postgresql@16 (16 ###  Downloading  16.4MB/ 19.1MB[K[4F[34m⠋[0m Bottle icu4c@78 (78.2)
[K[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle zstd (1.5.7_1)    #    Downloading 258.0KB/793.6KB
[K[34m⠋[0m Bottle Manifest gettext (1.0)
[K[34m⠋[0m Bottle postgresql@16 (16 ###  Downloading  16.7MB/ 19.1MB[K[4F[34m⠋[0m Bottle icu4c@78 (78.2)
[K[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle zstd (1.5.7_1)    ##   Downloading 307.2KB/793.6KB
[K[34m⠋[0m Bottle Manifest gettext (1.0)
[K[34m⠋[0m Bottle postgresql@16 (16 #### Downloading  17.1MB/ 19.1MB[K[4F[34m⠙[0m Bottle icu4c@78 (78.2)
[K[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle zstd (1.5.7_1)    ##   Downloading 340.0KB/793.6KB
[K[34m⠙[0m Bottle Manifest gettext (1.0)
[K[34m⠙[0m Bottle postgresql@16 (16 #### Downloading  17.4MB/ 19.1MB[K[4F[34m⠙[0m Bottle icu4c@78 (78.2)
[K[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle zstd (1.5.7_1)    ##   Downloading 340.0KB/793.6KB
[K[34m⠙[0m Bottle Manifest gettext (1.0)
[K[34m⠙[0m Bottle postgresql@16 (16 #### Downloading  17.4MB/ 19.1MB[K[4F[34m⠚[0m Bottle icu4c@78 (78.2)
[K[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle zstd (1.5.7_1)    ##   Downloading 372.7KB/793.6KB
[K[34m⠚[0m Bottle Manifest gettext (1.0)
[K[34m⠚[0m Bottle postgresql@16 (16 #### Downloading  17.6MB/ 19.1MB[K[4F[34m⠚[0m Bottle icu4c@78 (78.2)
[K[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle zstd (1.5.7_1)    ##   Downloading 421.9KB/793.6KB
[K[34m⠚[0m Bottle Manifest gettext (1.0)
[K[34m⠚[0m Bottle postgresql@16 (16 #### Downloading  17.9MB/ 19.1MB[K[4F[34m⠞[0m Bottle icu4c@78 (78.2)
[K[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle zstd (1.5.7_1)    ##   Downloading 454.7KB/793.6KB
[K[34m⠞[0m Bottle Manifest gettext (1.0)
[K[34m⠞[0m Bottle postgresql@16 (16 #### Downloading  18.1MB/ 19.1MB[K[4F[34m⠞[0m Bottle icu4c@78 (78.2)
[K[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle zstd (1.5.7_1)    ##   Downloading 495.6KB/793.6KB
[K[34m⠞[0m Bottle Manifest gettext (1.0)
[K[34m⠞[0m Bottle postgresql@16 (16 #### Downloading  18.5MB/ 19.1MB[K[4F[34m⠖[0m Bottle icu4c@78 (78.2)
[K[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle zstd (1.5.7_1)    ###  Downloading 544.8KB/793.6KB
[K[34m⠖[0m Bottle Manifest gettext (1.0)
[K[34m⠖[0m Bottle postgresql@16 (16 #### Downloading  18.8MB/ 19.1MB[K[4F[34m⠖[0m Bottle icu4c@78 (78.2)
[K[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle zstd (1.5.7_1)    ###  Downloading 610.3KB/793.6KB
[K[34m⠖[0m Bottle Manifest gettext (1.0)
[K[34m⠖[0m Bottle postgresql@16 (16.12)  Downloaded   19.1MB/ 19.1MB[K[4F[34m⠦[0m Bottle icu4c@78 (78.2)
[K[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle zstd (1.5.7_1)         Extracting  793.6KB/793.6KB
[K[34m⠦[0m Bottle Manifest gettext (1.0)
[K[34m⠦[0m Bottle postgresql@16 (16.12)  Extracting   19.1MB/ 19.1MB[K[4F[32m✔︎[0m Bottle zstd (1.5.7_1)         Downloaded  793.6KB/793.6KB
[K[34m⠦[0m Bottle icu4c@78 (78.2)
[K[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle Manifest gettext (1.0)
[K[34m⠦[0m Bottle postgresql@16 (16.12)  Extracting   19.1MB/ 19.1MB[K[3F[34m⠴[0m Bottle icu4c@78 (78.2)
[K[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle Manifest gettext (1.0)
[K[34m⠴[0m Bottle postgresql@16 (16.12)  Extracting   19.1MB/ 19.1MB[K[3F[34m⠴[0m Bottle icu4c@78 (78.2)
[K[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle Manifest gettext (1.0)
[K[34m⠴[0m Bottle postgresql@16 (16.12)  Extracting   19.1MB/ 19.1MB[K[3F[34m⠲[0m Bottle icu4c@78 (78.2)
[K[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle Manifest gettext (1.0)
[K[34m⠲[0m Bottle postgresql@16 (16.12)  Extracting   19.1MB/ 19.1MB[K[3F[34m⠲[0m Bottle icu4c@78 (78.2)
[K[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle Manifest gettext (1.0)
[K[34m⠲[0m Bottle postgresql@16 (16.12)  Extracting   19.1MB/ 19.1MB[K[3F[34m⠳[0m Bottle icu4c@78 (78.2)
[K[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle Manifest gettext (1.0)
[K[34m⠳[0m Bottle postgresql@16 (16.12)  Extracting   19.1MB/ 19.1MB[K[3F[34m⠳[0m Bottle icu4c@78 (78.2)
[K[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle Manifest gettext (1.0)
[K[34m⠳[0m Bottle postgresql@16 (16.12)  Extracting   19.1MB/ 19.1MB[K[3F[34m⠓[0m Bottle icu4c@78 (78.2)
[K[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle Manifest gettext (1.0)
[K[34m⠓[0m Bottle postgresql@16 (16.12)  Extracting   19.1MB/ 19.1MB[K[3F[34m⠓[0m Bottle icu4c@78 (78.2)
[K[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle Manifest gettext (1.0)
[K[34m⠓[0m Bottle postgresql@16 (16.12)  Extracting   19.1MB/ 19.1MB[K[3F[34m⠋[0m Bottle icu4c@78 (78.2)
[K[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle Manifest gettext (1.0)
[K[34m⠋[0m Bottle postgresql@16 (16.12)  Extracting   19.1MB/ 19.1MB[K[3F[34m⠋[0m Bottle icu4c@78 (78.2)
[K[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle Manifest gettext (1.0)
[K[34m⠋[0m Bottle postgresql@16 (16.12)  Extracting   19.1MB/ 19.1MB[K[3F[32m✔︎[0m Bottle postgresql@16 (16.12)  Downloaded   19.1MB/ 19.1MB
[K[34m⠙[0m Bottle icu4c@78 (78.2)
[K[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle Manifest gettext (1.0)[K[2F[34m⠙[0m Bottle icu4c@78 (78.2)
[K[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle Manifest gettext (1.0)[K[2F[34m⠚[0m Bottle icu4c@78 (78.2)
[K[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle Manifest gettext (1.0)[K[2F[34m⠚[0m Bottle icu4c@78 (78.2)
[K[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle Manifest gettext (1.0)[K[2F[34m⠞[0m Bottle icu4c@78 (78.2)
[K[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle Manifest gettext (1.0)[K[2F[34m⠞[0m Bottle icu4c@78 (78.2)
[K[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle Manifest gettext (1.0)[K[2F[34m⠖[0m Bottle icu4c@78 (78.2)
[K[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle Manifest gettext (1.0)[K[2F[34m⠖[0m Bottle icu4c@78 (78.2)
[K[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle Manifest gettext (1.0)[K[2F[34m⠦[0m Bottle icu4c@78 (78.2)
[K[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle Manifest gettext (1.0)[K[2F[34m⠦[0m Bottle icu4c@78 (78.2)
[K[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle Manifest gettext (1.0)[K[2F[34m⠴[0m Bottle icu4c@78 (78.2)
[K[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle Manifest gettext (1.0)[K[2F[34m⠴[0m Bottle icu4c@78 (78.2)
[K[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle Manifest gettext (1.0)[K[2F[34m⠲[0m Bottle icu4c@78 (78.2)
[K[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle Manifest gettext (1.0)[K[2F[34m⠲[0m Bottle icu4c@78 (78.2)
[K[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle Manifest gettext (1.0)[K[2F[34m⠳[0m Bottle icu4c@78 (78.2)
[K[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle Manifest gettext (1.0)[K[2F[34m⠳[0m Bottle icu4c@78 (78.2)
[K[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle Manifest gettext (1.0)[K[2F[34m⠓[0m Bottle icu4c@78 (78.2)
[K[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle Manifest gettext (1.0)[K[2F[34m⠓[0m Bottle icu4c@78 (78.2)
[K[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle Manifest gettext (1.0)[K[2F[34m⠋[0m Bottle icu4c@78 (78.2)
[K[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle Manifest gettext (1.0)[K[2F[34m⠋[0m Bottle icu4c@78 (78.2)
[K[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle Manifest gettext (1.0)[K[2F[34m⠙[0m Bottle icu4c@78 (78.2)
[K[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle Manifest gettext (1.0)[K[2F[34m⠙[0m Bottle icu4c@78 (78.2)
[K[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle Manifest gettext (1.0)[K[2F[34m⠚[0m Bottle icu4c@78 (78.2)
[K[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle Manifest gettext (1.0)[K[2F[34m⠚[0m Bottle icu4c@78 (78.2)
[K[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle Manifest gettext (1.0)[K[2F[34m⠞[0m Bottle icu4c@78 (78.2)
[K[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle Manifest gettext (1.0)[K[2F[34m⠞[0m Bottle icu4c@78 (78.2)
[K[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle Manifest gettext (1.0)[K[2F[34m⠖[0m Bottle icu4c@78 (78.2)
[K[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle Manifest gettext (1.0)[K[2F[34m⠖[0m Bottle icu4c@78 (78.2)
[K[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle Manifest gettext (1.0)[K[2F[34m⠦[0m Bottle icu4c@78 (78.2)
[K[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle Manifest gettext (1.0)[K[2F[34m⠦[0m Bottle icu4c@78 (78.2)
[K[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle Manifest gettext (1.0)[K[2F[34m⠴[0m Bottle icu4c@78 (78.2)
[K[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle Manifest gettext (1.0)[K[2F[34m⠴[0m Bottle icu4c@78 (78.2)
[K[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle Manifest gettext (1.0)[K[2F[34m⠲[0m Bottle icu4c@78 (78.2)
[K[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle Manifest gettext (1.0)[K[2F[34m⠲[0m Bottle icu4c@78 (78.2)
[K[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle Manifest gettext (1.0)[K[2F[34m⠳[0m Bottle icu4c@78 (78.2)
[K[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle Manifest gettext (1.0)[K[2F[34m⠳[0m Bottle icu4c@78 (78.2)
[K[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle Manifest gettext (1.0)[K[2F[34m⠓[0m Bottle icu4c@78 (78.2)
[K[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle Manifest gettext (1.0)[K[2F[34m⠓[0m Bottle icu4c@78 (78.2)
[K[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle Manifest gettext (1.0)[K[2F[34m⠋[0m Bottle icu4c@78 (78.2)
[K[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle Manifest gettext (1.0)[K[2F[34m⠋[0m Bottle icu4c@78 (78.2)
[K[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle Manifest gettext (1.0)[K[2F[34m⠙[0m Bottle icu4c@78 (78.2)
[K[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle Manifest gettext (1.0)[K[2F[34m⠙[0m Bottle icu4c@78 (78.2)
[K[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle Manifest gettext (1.0)[K[2F[34m⠚[0m Bottle icu4c@78 (78.2)        Downloading 159.7KB/ 31.8MB
[K[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle Manifest gettext (1.0)[K[2F[34m⠚[0m Bottle icu4c@78 (78.2)        Downloading 520.2KB/ 31.8MB
[K[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle Manifest gettext (1.0)[K[2F[34m⠞[0m Bottle icu4c@78 (78.2)        Downloading 831.5KB/ 31.8MB
[K[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle Manifest gettext (1.0)[K[2F[34m⠞[0m Bottle icu4c@78 (78.2)        Downloading   1.3MB/ 31.8MB
[K[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle Manifest gettext (1.0)[K[2F[34m⠖[0m Bottle icu4c@78 (78.2)        Downloading   1.7MB/ 31.8MB
[K[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle Manifest gettext (1.0)[K[2F[34m⠖[0m Bottle icu4c@78 (78.2)        Downloading   2.3MB/ 31.8MB
[K[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle Manifest gettext (1.0)[K[2F[34m⠦[0m Bottle icu4c@78 (78.2)        Downloading   2.5MB/ 31.8MB
[K[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle Manifest gettext (1.0)[K[2F[34m⠦[0m Bottle icu4c@78 (78.2)        Downloading   3.1MB/ 31.8MB
[K[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle Manifest gettext (1.0)[K[2F[34m⠴[0m Bottle icu4c@78 (78.2)        Downloading   3.6MB/ 31.8MB
[K[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle Manifest gettext (1.0)[K[2F[34m⠴[0m Bottle icu4c@78 (78.2)   #    Downloading   4.2MB/ 31.8MB
[K[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle Manifest gettext (1.0)[K[2F[34m⠲[0m Bottle icu4c@78 (78.2)   #    Downloading   4.7MB/ 31.8MB
[K[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle Manifest gettext (1.0)[K[2F[34m⠲[0m Bottle icu4c@78 (78.2)   #    Downloading   5.4MB/ 31.8MB
[K[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle Manifest gettext (1.0)[K[2F[34m⠳[0m Bottle icu4c@78 (78.2)   #    Downloading   5.8MB/ 31.8MB
[K[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle Manifest gettext (1.0)[K[2F[34m⠳[0m Bottle icu4c@78 (78.2)   #    Downloading   6.5MB/ 31.8MB
[K[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle Manifest gettext (1.0)[K[2F[34m⠓[0m Bottle icu4c@78 (78.2)   #    Downloading   6.9MB/ 31.8MB
[K[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle Manifest gettext (1.0)[K[2F[34m⠓[0m Bottle icu4c@78 (78.2)   #    Downloading   7.1MB/ 31.8MB
[K[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle Manifest gettext (1.0)[K[2F[34m⠋[0m Bottle icu4c@78 (78.2)   #    Downloading   7.7MB/ 31.8MB
[K[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle Manifest gettext (1.0)[K[2F[34m⠋[0m Bottle icu4c@78 (78.2)   #    Downloading   8.2MB/ 31.8MB
[K[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle Manifest gettext (1.0)[K[2F[34m⠙[0m Bottle icu4c@78 (78.2)   #    Downloading   8.6MB/ 31.8MB
[K[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle Manifest gettext (1.0)[K[2F[34m⠙[0m Bottle icu4c@78 (78.2)   #    Downloading   8.8MB/ 31.8MB
[K[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle Manifest gettext (1.0)[K[2F[34m⠚[0m Bottle icu4c@78 (78.2)   #    Downloading   9.3MB/ 31.8MB
[K[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle Manifest gettext (1.0)[K[2F[34m⠚[0m Bottle icu4c@78 (78.2)   #    Downloading   9.6MB/ 31.8MB
[K[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle Manifest gettext (1.0)[K[2F[34m⠞[0m Bottle icu4c@78 (78.2)   #    Downloading  10.1MB/ 31.8MB
[K[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle Manifest gettext (1.0)[K[2F[34m⠞[0m Bottle icu4c@78 (78.2)   #    Downloading  10.5MB/ 31.8MB
[K[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle Manifest gettext (1.0)[K[2F[34m⠖[0m Bottle icu4c@78 (78.2)   #    Downloading  11.0MB/ 31.8MB
[K[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle Manifest gettext (1.0)[K[2F[34m⠖[0m Bottle icu4c@78 (78.2)   #    Downloading  11.5MB/ 31.8MB
[K[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle Manifest gettext (1.0)[K[2F[34m⠦[0m Bottle icu4c@78 (78.2)   ##   Downloading  12.1MB/ 31.8MB
[K[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle Manifest gettext (1.0)[K[2F[34m⠦[0m Bottle icu4c@78 (78.2)   ##   Downloading  12.5MB/ 31.8MB
[K[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle Manifest gettext (1.0)[K[2F[34m⠴[0m Bottle icu4c@78 (78.2)   ##   Downloading  12.8MB/ 31.8MB
[K[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle Manifest gettext (1.0)[K[2F[34m⠴[0m Bottle icu4c@78 (78.2)   ##   Downloading  12.8MB/ 31.8MB
[K[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle Manifest gettext (1.0)[K[2F[34m⠲[0m Bottle icu4c@78 (78.2)   ##   Downloading  13.9MB/ 31.8MB
[K[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle Manifest gettext (1.0)[K[2F[34m⠲[0m Bottle icu4c@78 (78.2)   ##   Downloading  14.3MB/ 31.8MB
[K[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle Manifest gettext (1.0)[K[2F[34m⠳[0m Bottle icu4c@78 (78.2)   ##   Downloading  14.9MB/ 31.8MB
[K[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle Manifest gettext (1.0)[K[2F[34m⠳[0m Bottle icu4c@78 (78.2)   ##   Downloading  15.5MB/ 31.8MB
[K[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle Manifest gettext (1.0)[K[2F[34m⠓[0m Bottle icu4c@78 (78.2)   ##   Downloading  16.1MB/ 31.8MB
[K[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle Manifest gettext (1.0)[K[2F[34m⠓[0m Bottle icu4c@78 (78.2)   ##   Downloading  16.7MB/ 31.8MB
[K[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle Manifest gettext (1.0)[K[2F[34m⠋[0m Bottle icu4c@78 (78.2)   ##   Downloading  17.2MB/ 31.8MB
[K[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle Manifest gettext (1.0)[K[2F[34m⠋[0m Bottle icu4c@78 (78.2)   ##   Downloading  17.6MB/ 31.8MB
[K[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle Manifest gettext (1.0)[K[2F[34m⠙[0m Bottle icu4c@78 (78.2)   ##   Downloading  18.1MB/ 31.8MB
[K[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle Manifest gettext (1.0)[K[2F[34m⠙[0m Bottle icu4c@78 (78.2)   ##   Downloading  18.6MB/ 31.8MB
[K[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle Manifest gettext (1.0)[K[2F[34m⠚[0m Bottle icu4c@78 (78.2)   ##   Downloading  19.0MB/ 31.8MB
[K[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle Manifest gettext (1.0)[K[2F[34m⠚[0m Bottle icu4c@78 (78.2)   ##   Downloading  19.4MB/ 31.8MB
[K[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle Manifest gettext (1.0)[K[2F[34m⠞[0m Bottle icu4c@78 (78.2)   ##   Downloading  19.6MB/ 31.8MB
[K[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle Manifest gettext (1.0)[K[2F[34m⠞[0m Bottle icu4c@78 (78.2)   ###  Downloading  20.1MB/ 31.8MB
[K[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle Manifest gettext (1.0)[K[2F[34m⠖[0m Bottle icu4c@78 (78.2)   ###  Downloading  20.5MB/ 31.8MB
[K[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle Manifest gettext (1.0)[K[2F[34m⠖[0m Bottle icu4c@78 (78.2)   ###  Downloading  20.9MB/ 31.8MB
[K[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle Manifest gettext (1.0)[K[2F[34m⠦[0m Bottle icu4c@78 (78.2)   ###  Downloading  21.3MB/ 31.8MB
[K[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle Manifest gettext (1.0)[K[2F[34m⠦[0m Bottle icu4c@78 (78.2)   ###  Downloading  21.8MB/ 31.8MB
[K[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle Manifest gettext (1.0)[K[2F[34m⠴[0m Bottle icu4c@78 (78.2)   ###  Downloading  22.3MB/ 31.8MB
[K[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle Manifest gettext (1.0)[K[2F[34m⠴[0m Bottle icu4c@78 (78.2)   ###  Downloading  22.8MB/ 31.8MB
[K[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle Manifest gettext (1.0)[K[2F[34m⠲[0m Bottle icu4c@78 (78.2)   ###  Downloading  23.3MB/ 31.8MB
[K[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle Manifest gettext (1.0)[K[2F[34m⠲[0m Bottle icu4c@78 (78.2)   ###  Downloading  23.8MB/ 31.8MB
[K[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle Manifest gettext (1.0)[K[2F[34m⠳[0m Bottle icu4c@78 (78.2)   ###  Downloading  24.2MB/ 31.8MB
[K[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle Manifest gettext (1.0)[K[2F[34m⠳[0m Bottle icu4c@78 (78.2)   ###  Downloading  24.8MB/ 31.8MB
[K[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle Manifest gettext (1.0)[K[2F[34m⠓[0m Bottle icu4c@78 (78.2)   ###  Downloading  25.2MB/ 31.8MB
[K[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle Manifest gettext (1.0)[K[2F[34m⠓[0m Bottle icu4c@78 (78.2)   ###  Downloading  25.8MB/ 31.8MB
[K[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle Manifest gettext (1.0)[K[2F[34m⠋[0m Bottle icu4c@78 (78.2)   ###  Downloading  26.4MB/ 31.8MB
[K[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle Manifest gettext (1.0)[K[2F[34m⠋[0m Bottle icu4c@78 (78.2)   ###  Downloading  26.7MB/ 31.8MB
[K[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle Manifest gettext (1.0)[K[2F[34m⠙[0m Bottle icu4c@78 (78.2)   ###  Downloading  27.3MB/ 31.8MB
[K[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle Manifest gettext (1.0)[K[2F[34m⠙[0m Bottle icu4c@78 (78.2)   ###  Downloading  27.7MB/ 31.8MB
[K[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle Manifest gettext (1.0)[K[2F[34m⠚[0m Bottle icu4c@78 (78.2)   #### Downloading  28.2MB/ 31.8MB
[K[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle Manifest gettext (1.0)[K[2F[34m⠚[0m Bottle icu4c@78 (78.2)   #### Downloading  28.7MB/ 31.8MB
[K[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle Manifest gettext (1.0)[K[2F[34m⠞[0m Bottle icu4c@78 (78.2)   #### Downloading  29.0MB/ 31.8MB
[K[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle Manifest gettext (1.0)[K[2F[34m⠞[0m Bottle icu4c@78 (78.2)   #### Downloading  29.4MB/ 31.8MB
[K[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle Manifest gettext (1.0)[K[2F[34m⠖[0m Bottle icu4c@78 (78.2)   #### Downloading  29.7MB/ 31.8MB
[K[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle Manifest gettext (1.0)[K[2F[34m⠖[0m Bottle icu4c@78 (78.2)   #### Downloading  30.0MB/ 31.8MB
[K[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle Manifest gettext (1.0)[K[2F[34m⠦[0m Bottle icu4c@78 (78.2)   #### Downloading  30.3MB/ 31.8MB
[K[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle Manifest gettext (1.0)[K[2F[34m⠦[0m Bottle icu4c@78 (78.2)   #### Downloading  30.7MB/ 31.8MB
[K[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle Manifest gettext (1.0)[K[2F[34m⠴[0m Bottle icu4c@78 (78.2)   #### Downloading  31.0MB/ 31.8MB
[K[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle Manifest gettext (1.0)[K[2F[34m⠴[0m Bottle icu4c@78 (78.2)   #### Downloading  31.5MB/ 31.8MB
[K[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle Manifest gettext (1.0)[K[2F[34m⠲[0m Bottle icu4c@78 (78.2)        Extracting   31.8MB/ 31.8MB
[K[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle Manifest gettext (1.0)[K[2F[34m⠲[0m Bottle icu4c@78 (78.2)        Extracting   31.8MB/ 31.8MB
[K[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle Manifest gettext (1.0)[K[2F[34m⠳[0m Bottle icu4c@78 (78.2)        Extracting   31.8MB/ 31.8MB
[K[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle Manifest gettext (1.0)[K[2F[34m⠳[0m Bottle icu4c@78 (78.2)        Extracting   31.8MB/ 31.8MB
[K[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle Manifest gettext (1.0)[K[2F[34m⠓[0m Bottle icu4c@78 (78.2)        Extracting   31.8MB/ 31.8MB
[K[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle Manifest gettext (1.0)[K[2F[34m⠓[0m Bottle icu4c@78 (78.2)        Extracting   31.8MB/ 31.8MB
[K[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle Manifest gettext (1.0)[K[2F[32m✔︎[0m Bottle icu4c@78 (78.2)        Downloaded   31.8MB/ 31.8MB
[K[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle Manifest gettext (1.0)[K[1F[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle Manifest gettext (1.0)[K[1F[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle Manifest gettext (1.0)[K[1F[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle Manifest gettext (1.0)[K[1F[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle Manifest gettext (1.0)[K[1F[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle Manifest gettext (1.0)[K[1F[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle Manifest gettext (1.0)[K[1F[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle Manifest gettext (1.0)[K[1F[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle Manifest gettext (1.0)[K[1F[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle Manifest gettext (1.0)[K[1F[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle Manifest gettext (1.0)[K[1F[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle Manifest gettext (1.0)[K[1F[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle Manifest gettext (1.0)[K[1F[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle Manifest gettext (1.0)[K[1F[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle Manifest gettext (1.0)[K[1F[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle Manifest gettext (1.0)[K[1F[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle Manifest gettext (1.0)[K[1F[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle Manifest gettext (1.0)[K[1F[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle Manifest gettext (1.0)[K[1F[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle Manifest gettext (1.0)[K[1F[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle Manifest gettext (1.0)[K[1F[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle Manifest gettext (1.0)[K[1F[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle Manifest gettext (1.0)[K[1F[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle Manifest gettext (1.0)[K[1F[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle Manifest gettext (1.0)[K[1F[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle Manifest gettext (1.0)[K[1F[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle Manifest gettext (1.0)[K[1F[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle Manifest gettext (1.0)[K[1F[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle Manifest gettext (1.0)[K[1F[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle Manifest gettext (1.0)[K[1F[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle Manifest gettext (1.0)[K[1F[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle Manifest gettext (1.0)[K[1F[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle Manifest gettext (1.0)[K[1F[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle Manifest gettext (1.0)[K[1F[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle Manifest gettext (1.0)[K[1F[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle Manifest gettext (1.0)[K[1F[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle Manifest gettext (1.0)[K[1F[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle Manifest gettext (1.0)[K[1F[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle Manifest gettext (1.0)[K[1F[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle Manifest gettext (1.0)[K[1F[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle Manifest gettext (1.0)[K[1F[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle Manifest gettext (1.0)[K[1F[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle Manifest gettext (1.0)[K[1F[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle Manifest gettext (1.0)[K[1F[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle Manifest gettext (1.0)[K[1F[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle Manifest gettext (1.0)[K[1F[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle Manifest gettext (1.0)[K[1F[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle Manifest gettext (1.0)[K[1F[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle Manifest gettext (1.0)[K[1F[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle Manifest gettext (1.0)[K[1F[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle Manifest gettext (1.0)[K[1F[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle Manifest gettext (1.0)[K[1F[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle Manifest gettext (1.0)[K[1F[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle Manifest gettext (1.0)[K[1F[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle Manifest gettext (1.0)[K[1F[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle Manifest gettext (1.0)[K[1F[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle Manifest gettext (1.0)[K[1F[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle Manifest gettext (1.0)[K[1F[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle Manifest gettext (1.0)[K[1F[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle Manifest gettext (1.0)[K[1F[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle Manifest gettext (1.0)[K[1F[34m⠋[0m Bottle Manifest xz (5.8.2)
[K[34m⠋[0m Bottle Manifest gettext (1.0)[K[1F[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle Manifest gettext (1.0)[K[1F[34m⠙[0m Bottle Manifest xz (5.8.2)
[K[34m⠙[0m Bottle Manifest gettext (1.0)[K[1F[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle Manifest gettext (1.0)[K[1F[34m⠚[0m Bottle Manifest xz (5.8.2)
[K[34m⠚[0m Bottle Manifest gettext (1.0)[K[1F[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle Manifest gettext (1.0)[K[1F[34m⠞[0m Bottle Manifest xz (5.8.2)
[K[34m⠞[0m Bottle Manifest gettext (1.0)[K[1F[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle Manifest gettext (1.0)[K[1F[34m⠖[0m Bottle Manifest xz (5.8.2)
[K[34m⠖[0m Bottle Manifest gettext (1.0)[K[1F[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle Manifest gettext (1.0)[K[1F[34m⠦[0m Bottle Manifest xz (5.8.2)
[K[34m⠦[0m Bottle Manifest gettext (1.0)[K[1F[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle Manifest gettext (1.0)[K[1F[34m⠴[0m Bottle Manifest xz (5.8.2)
[K[34m⠴[0m Bottle Manifest gettext (1.0)[K[1F[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle Manifest gettext (1.0)[K[1F[34m⠲[0m Bottle Manifest xz (5.8.2)
[K[34m⠲[0m Bottle Manifest gettext (1.0)[K[1F[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle Manifest gettext (1.0)[K[1F[34m⠳[0m Bottle Manifest xz (5.8.2)
[K[34m⠳[0m Bottle Manifest gettext (1.0)[K[1F[34m⠓[0m Bottle Manifest xz (5.8.2)
[K[34m⠓[0m Bottle Manifest gettext (1.0)[K[1F[32m✔︎[0m Bottle Manifest xz (5.8.2)    Downloaded   11.8KB/ 11.8KB
[K[34m⠓[0m Bottle Manifest gettext (1.0)[K[0G[34m⠋[0m Bottle Manifest gettext (1.0)[K[0G[34m⠋[0m Bottle Manifest gettext (1.0)[K[0G[34m⠙[0m Bottle Manifest gettext (1.0)[K[0G[34m⠙[0m Bottle Manifest gettext (1.0)[K[0G[34m⠚[0m Bottle Manifest gettext (1.0)[K[0G[34m⠚[0m Bottle Manifest gettext (1.0)[K[0G[34m⠞[0m Bottle Manifest gettext (1.0)[K[0G[34m⠞[0m Bottle Manifest gettext (1.0)[K[0G[34m⠖[0m Bottle Manifest gettext (1.0)[K[0G[34m⠖[0m Bottle Manifest gettext (1.0)[K[0G[34m⠦[0m Bottle Manifest gettext (1.0)[K[0G[34m⠦[0m Bottle Manifest gettext (1.0)[K[0G[34m⠴[0m Bottle Manifest gettext (1.0)[K[0G[34m⠴[0m Bottle Manifest gettext (1.0) Downloading   8.2KB/-------[K[0G[34m⠲[0m Bottle Manifest gettext (1.0) Downloading   8.2KB/-------[K[0G[32m✔︎[0m Bottle Manifest gettext (1.0) Downloaded   13.7KB/ 13.7KB
[K[?25h[32m==>[0m [1mInstalling dependencies for postgresql@16: [32micu4c@78[39m, [32mopenssl@3[39m, [32mkrb5[39m, [32mxz[39m, [32mzstd[39m, [32mlibunistring[39m and [32mgettext[39m[0m
[32m==>[0m [1mInstalling postgresql@16 dependency: [32micu4c@78[39m[0m
[34m==>[0m [1mPouring icu4c@78--78.2.arm64_tahoe.bottle.1.tar.gz[0m
🍺  /opt/homebrew/Cellar/icu4c@78/78.2: 279 files, 87.7MB
[32m==>[0m [1mInstalling postgresql@16 dependency: [32mopenssl@3[39[0m
[34m==>[0m [1mPouring openssl@3--3.6.1.arm64_tahoe.bottle.tar.gz[0m
🍺  /opt/homebrew/Cellar/openssl@3/3.6.1: 7,624 files, 37.6MB
[32m==>[0m [1mInstalling postgresql@16 dependency: [32mkrb5[39m[0m
[34m==>[0m [1mPouring krb5--1.22.2.arm64_tahoe.bottle.tar.gz[0m
🍺  /opt/homebrew/Cellar/krb5/1.22.2: 163 files, 5.9MB
[32m==>[0m [1mInstalling postgresql@16 dependency: [32mxz[39m[0m
[34m==>[0m [1mPouring xz--5.8.2.arm64_tahoe.bottle.tar.gz[0m
🍺  /opt/homebrew/Cellar/xz/5.8.2: 96 files, 2.7MB
[32m==>[0m [1mInstalling postgresql@16 dependency: [32mzstd[39m[0m
[34m==>[0m [1mPouring zstd--1.5.7_1.arm64_tahoe.bottle.tar.gz[0m
🍺  /opt/homebrew/Cellar/zstd/1.5.7_1: 32 files, 2.3MB
[32m==>[0m [1mInstalling postgresql@16 dependency: [32mlibunistring[0m
[34m==>[0m [1mPouring libunistring--1.4.2.arm64_tahoe.bottle.tar.gz[0m
🍺  /opt/homebrew/Cellar/libunistring/1.4.2: 59 files, 5.8MB
[32m==>[0m [1mInstalling postgresql@16 dependency: [32mgettext[39m[0m
[34m==>[0m [1mPouring gettext--1.0.arm64_tahoe.bottle.tar.gz[0m
🍺  /opt/homebrew/Cellar/gettext/1.0: 2,499 files, 35.3MB
[32m==>[0m [1mInstalling [32mpostgresql@16[39m[0m
[34m==>[0m [1mPouring postgresql@16--16.12.arm64_tahoe.bottle.tar.gz[0m
[34m==>[0m [1m/opt/homebrew/Cellar/postgresql@16/16.12/bin/initdb --l[0m
[34m==>[0m [1mCaveats[0m
This formula has created a default database cluster with:
  initdb --locale=en_US.UTF-8 -E UTF-8 /opt/homebrew/var/postgresql@16

postgresql@16 is keg-only, which means it was not symlinked into /opt/homebrew,
because this is an alternate version of another formula.

If you need to have postgresql@16 first in your PATH, run:
  echo 'export PATH="/opt/homebrew/opt/postgresql@16/bin:$PATH"' >> /Users/harumacmini/.zshrc

For compilers to find postgresql@16 you may need to set:
  export LDFLAGS="-L/opt/homebrew/opt/postgresql@16/lib"
  export CPPFLAGS="-I/opt/homebrew/opt/postgresql@16/include"

For pkgconf to find postgresql@16 you may need to set:
  export PKG_CONFIG_PATH="/opt/homebrew/opt/postgresql@16/lib/pkgconfig"

To start postgresql@16 now and restart at login:
  brew services start postgresql@16
Or, if you don't want/need a background service you can just run:
  LC_ALL="en_US.UTF-8" /opt/homebrew/opt/postgresql@16/bin/postgres -D /opt/homebrew/var/postgresql@16
[34m==>[0m [1mSummary[0m
🍺  /opt/homebrew/Cellar/postgresql@16/16.12: 3,814 files, 72.5MB
[34m==>[0m [1mRunning `brew cleanup postgresql@16`...[0m
Disable this behaviour by setting `HOMEBREW_NO_INSTALL_CLEANUP=1`.
Hide these hints with `HOMEBREW_NO_ENV_HINTS=1` (see `man brew`).
[32m==>[0m [1m`brew cleanup` has not been run in the last 30 days, ru[0m
Disable this behaviour by setting `HOMEBREW_NO_INSTALL_CLEANUP=1`.
Hide these hints with `HOMEBREW_NO_ENV_HINTS=1` (see `man brew`).
Removing: /opt/homebrew/Cellar/icu4c@78/78.1... (279 files, 87.9MB)
Removing: /Users/harumacmini/Library/Caches/Homebrew/icu4c@78_bottle_manifest--78.1... (9.7KB)
Removing: /Users/harumacmini/Library/Caches/Homebrew/icu4c@78--78.1... (32.0MB)
Removing: /opt/homebrew/Cellar/openssl@3/3.6.0... (7,609 files, 37.7MB)
Removing: /Users/harumacmini/Library/Caches/Homebrew/openssl@3_bottle_manifest--3.6.0... (11.8KB)
Removing: /Users/harumacmini/Library/Caches/Homebrew/openssl@3--3.6.0... (10.9MB)
Removing: /opt/homebrew/Cellar/xz/5.8.1... (96 files, 2.6MB)
Removing: /Users/harumacmini/Library/Caches/Homebrew/xz_bottle_manifest--5.8.1... (14.5KB)
Removing: /Users/harumacmini/Library/Caches/Homebrew/xz--5.8.1... (748.1KB)
Removing: /opt/homebrew/Cellar/zstd/1.5.7... (32 files, 2.3MB)
Removing: /Users/harumacmini/Library/Caches/Homebrew/zstd_bottle_manifest--1.5.7-1... (16.2KB)
Removing: /Users/harumacmini/Library/Caches/Homebrew/zstd--1.5.7... (788.5KB)
Removing: /Users/harumacmini/Library/Caches/Homebrew/portable-ruby-3.4.8.arm64_big_sur.bottle.tar.gz... (12.2MB)
Removing: /Users/harumacmini/Library/Caches/Homebrew/api-source/Homebrew/homebrew-cask/bd5995f5721d334420362a860335bf3701b71e20/Cask/libreoffice.rb... (2.8KB)
Removing: /Users/harumacmini/Library/Caches/Homebrew/api-source/Homebrew/homebrew-cask/bf00d83a63aa4009a704f8c13fedc622c3cfa475/Cask/gcloud-cli.rb... (3.4KB)
Removing: /Users/harumacmini/Library/Caches/Homebrew/bootsnap/aa63b2bf9ba1db3c5940867e30a0ca8156f3643776b6b443a3475a2f50fcf4bb... (647 files, 5.7MB)
Removing: /Users/harumacmini/Library/Caches/Homebrew/bootsnap/b72e5134e0c29352f27278b0d0b535599d84c5338f6a429e709b448cf7d3721c... (655 files, 5.8MB)
Removing: /Users/harumacmini/Library/Caches/Homebrew/bootsnap/684c26d8211c6bacd91f8187e94767537f4d3bb555c2f7d7d582cc7ad2221890... (647 files, 5.7MB)
Removing: /Users/harumacmini/Library/Logs/Homebrew/mpdecimal... (64B)
Removing: /Users/harumacmini/Library/Logs/Homebrew/python@3.13... (64B)
Removing: /Users/harumacmini/Library/Logs/Homebrew/readline... (64B)
Removing: /Users/harumacmini/Library/Logs/Homebrew/sqlite... (64B)
Removing: /Users/harumacmini/Library/Logs/Homebrew/cloud-sql-proxy... (64B)
[32m==>[0m [1mCaveats[0m
[34m==>[0m [1mpostgresql@16[0m
This formula has created a default database cluster with:
  initdb --locale=en_US.UTF-8 -E UTF-8 /opt/homebrew/var/postgresql@16

postgresql@16 is keg-only, which means it was not symlinked into /opt/homebrew,
because this is an alternate version of another formula.

If you need to have postgresql@16 first in your PATH, run:
  echo 'export PATH="/opt/homebrew/opt/postgresql@16/bin:$PATH"' >> /Users/harumacmini/.zshrc

For compilers to find postgresql@16 you may need to set:
  export LDFLAGS="-L/opt/homebrew/opt/postgresql@16/lib"
  export CPPFLAGS="-I/opt/homebrew/opt/postgresql@16/include"

For pkgconf to find postgresql@16 you may need to set:
  export PKG_CONFIG_PATH="/opt/homebrew/opt/postgresql@16/lib/pkgconfig"

To start postgresql@16 now and restart at login:
  brew services start postgresql@16
Or, if you don't want/need a background service you can just run:
  LC_ALL="en_US.UTF-8" /opt/homebrew/opt/postgresql@16/bin/postgres -D /opt/homebrew/var/postgresql@16
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[7mbrew services sta[7mr[7mt postgresql@16[27m[K[A[26C[27mb[27mr[27me[27mw[27m [27ms[27me[27mr[27mv[27mi[27mc[27me[27ms[27m [27ms[27mt[27mar[27mt[27m [27mp[27mo[27ms[27mt[27mg[27mr[27me[27ms[27mq[27ml[27m@[27m1[27m6[?2004l
[34m==>[0m [1mSuccessfully started `postgresql@16` (label: homebrew.m[0m
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[7mpsql -h localhost[7m [7m-U postgres[27m[K[A[30C[27mp[27ms[27mq[27ml[27m [27m-[27mh[27m [27ml[27mo[27mc[27ma[27ml[27mh[27mo[27ms[27mt [27m-[27mU[27m [27mp[27mo[27ms[27mt[27mg[27mr[27me[27ms[?2004l
zsh: command not found: psql
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[7mecho 'export PATH[7m=[7m"/opt/homebrew/opt/postgresql@16/bin:$PATH"' >> ~/.zshrc[27m[K
[7msource ~/.zshrc[27m[K[A[A[27C[27me[27mc[27mh[27mo[27m [27m'[27me[27mx[27mp[27mo[27mr[27mt[27m [27mP[27mA[27mT[27mH=[27m"[27m/[27mo[27mp[27mt[27m/[27mh[27mo[27mm[27me[27mb[27mr[27me[27mw[27m/[27mo[27mp[27mt[27m/[27mp[27mo[27ms[27mt[27mg[27mr[27me[27ms[27mq[27ml[27m@[27m1[27m6[27m/[27mb[27mi[27mn[27m:[27m$[27mP[27mA[27mT[27mH[27m"[27m'[27m [27m>[27m>[27m [27m~[27m/[27m.[27mz[27ms[27mh[27mr[27mc[1B[27ms[27mo[27mu[27mr[27mc[27me[27m [27m~[27m/[27m.[27mz[27ms[27mh[27mr[27mc[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[7mpsql -h localhost[7m [7m-d postgres[27m[K[A[30C[27mp[27ms[27mq[27ml[27m [27m-[27mh[27m [27ml[27mo[27mc[27ma[27ml[27mh[27mo[27ms[27mt [27m-[27md[27m [27mp[27mo[27ms[27mt[27mg[27mr[27me[27ms[?2004l
psql (16.12 (Homebrew))
Type "help" for help.

[?2004hpostgres=# 
[?2004l[?2004hpostgres=# 
[?2004l[?2004hpostgres=# 
[?2004l[?2004hpostgres=# 
[?2004l[?2004hpostgres=# [7m\q[27m\q
[?2004l[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[7mexport POSTGRES_P[7mA[7mSSWORD=[27m[K[A[34C[27me[27mx[27mp[27mo[27mr[27mt[27m [27mP[27mO[27mS[27mT[27mG[27mR[27mE[27mS[27m_[27mPA[27mS[27mS[27mW[27mO[27mR[27mD[27m=Legatus2000/[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[7mpython scripts/au[7md[7mit_csv_vs_db_integrity.py --report integrity_report.csv[27m[K[A[14D[27mp[27my[27mt[27mh[27mo[27mn[27m [27ms[27mc[27mr[27mi[27mp[27mt[27ms[27m/[27ma[27mud[27mi[27mt[27m_[27mc[27ms[27mv[27m_[27mv[27ms[27m_[27md[27mb[27m_[27mi[27mn[27mt[27me[27mg[27mr[27mi[27mt[27my[27m.[27mp[27my[27m [27m-[27m-[27mr[27me[27mp[27mo[27mr[27mt[27m [27mi[27mn[27mt[27me[27mg[27mr[27mi[27mt[27my[27m_[27mr[27me[27mp[27mo[27mr[27mt[27m.[27mc[27ms[27mv[?2004l
対象CSV: 182 件（fixed_csv_3/ 配下、/later/ 除外）
Traceback (most recent call last):
  File "/Users/harumacmini/programming/info_companyDetail/scripts/audit_csv_vs_db_integrity.py", line 454, in <module>
    main()
  File "/Users/harumacmini/programming/info_companyDetail/scripts/audit_csv_vs_db_integrity.py", line 415, in main
    conn = psycopg2.connect(
           ^^^^^^^^^^^^^^^^^
  File "/Users/harumacmini/.pyenv/versions/3.12.2/lib/python3.12/site-packages/psycopg2/__init__.py", line 122, in connect
    conn = _connect(dsn, connection_factory=connection_factory, **kwasync)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
psycopg2.OperationalError: connection to server at "127.0.0.1", port 5432 failed: server does not support SSL, but SSL was required

[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[7mexport POSTGRES_S[7mS[7mLMODE='disable'[27m[K[A[26C[27me[27mx[27mp[27mo[27mr[27mt[27m [27mP[27mO[27mS[27mT[27mG[27mR[27mE[27mS[27m_[27mSS[27mL[27mM[27mO[27mD[27mE[27m=[27m'[27md[27mi[27ms[27ma[27mb[27ml[27me[27m'[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[7mexport POSTGRES_P[7mA[7mSSWORD='Legatus2000/'[27m[K
[7mexport POSTGRES_HOST='127.0.0.1'[27m[K[A[A[10C[27me[27mx[27mp[27mo[27mr[27mt[27m [27mP[27mO[27mS[27mT[27mG[27mR[27mE[27mS[27m_[27mPA[27mS[27mS[27mW[27mO[27mR[27mD[27m=[27m'[27mL[27me[27mg[27ma[27mt[27mu[27ms[27m2[27m0[27m0[27m0[27m/[27m'[1B[27me[27mx[27mp[27mo[27mr[27mt[27m [27mP[27mO[27mS[27mT[27mG[27mR[27mE[27mS[27m_[27mH[27mO[27mS[27mT[27m=[27m'[27m1[27m2[27m7[27m.[27m0[27m.[27m0[27m.[27m1[27m'[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[7mpython scripts/au[7md[7mit_csv_vs_db_integrity.py --report integrity_report.csv[27m[K[A[14D[27mp[27my[27mt[27mh[27mo[27mn[27m [27ms[27mc[27mr[27mi[27mp[27mt[27ms[27m/[27ma[27mud[27mi[27mt[27m_[27mc[27ms[27mv[27m_[27mv[27ms[27m_[27md[27mb[27m_[27mi[27mn[27mt[27me[27mg[27mr[27mi[27mt[27my[27m.[27mp[27my[27m [27m-[27m-[27mr[27me[27mp[27mo[27mr[27mt[27m [27mi[27mn[27mt[27me[27mg[27mr[27mi[27mt[27my[27m_[27mr[27me[27mp[27mo[27mr[27mt[27m.[27mc[27ms[27mv[?2004l
対象CSV: 182 件（fixed_csv_3/ 配下、/later/ 除外）
Traceback (most recent call last):
  File "/Users/harumacmini/programming/info_companyDetail/scripts/audit_csv_vs_db_integrity.py", line 454, in <module>
    main()
  File "/Users/harumacmini/programming/info_companyDetail/scripts/audit_csv_vs_db_integrity.py", line 415, in main
    conn = psycopg2.connect(
           ^^^^^^^^^^^^^^^^^
  File "/Users/harumacmini/.pyenv/versions/3.12.2/lib/python3.12/site-packages/psycopg2/__init__.py", line 122, in connect
    conn = _connect(dsn, connection_factory=connection_factory, **kwasync)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
psycopg2.OperationalError: connection to server at "127.0.0.1", port 5432 failed: FATAL:  role "postgres" does not exist

[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[7mexport POSTGRES_U[7mS[7mER='harumacmini'[27m[K
[7mpython scripts/audit_csv_vs_db_integrity.py --report integr[7mi[7mty_report.csv[27m[K[3A[28C[27me[27mx[27mp[27mo[27mr[27mt[27m [27mP[27mO[27mS[27mT[27mG[27mR[27mE[27mS[27m_[27mUS[27mE[27mR[27m=[27m'[27mh[27ma[27mr[27mu[27mm[27ma[27mc[27mm[27mi[27mn[27mi[27m'[1B[27mp[27my[27mt[27mh[27mo[27mn[27m [27ms[27mc[27mr[27mi[27mp[27mt[27ms[27m/[27ma[27mu[27md[27mi[27mt[27m_[27mc[27ms[27mv[27m_[27mv[27ms[27m_[27md[27mb[27m_[27mi[27mn[27mt[27me[27mg[27mr[27mi[27mt[27my[27m.[27mp[27my[27m [27m-[27m-[27mr[27me[27mp[27mo[27mr[27mt[27m [27mi[27mn[27mt[27me[27mg[27mri[27mt[27my[27m_[27mr[27me[27mp[27mo[27mr[27mt[27m.[27mc[27ms[27mv[?2004l
対象CSV: 182 件（fixed_csv_3/ 配下、/later/ 除外）
Traceback (most recent call last):
  File "/Users/harumacmini/programming/info_companyDetail/scripts/audit_csv_vs_db_integrity.py", line 454, in <module>
    main()
  File "/Users/harumacmini/programming/info_companyDetail/scripts/audit_csv_vs_db_integrity.py", line 426, in main
    cache_corp, cache_name_pref = load_id_caches(conn)
                                  ^^^^^^^^^^^^^^^^^^^^
  File "/Users/harumacmini/programming/info_companyDetail/scripts/audit_csv_vs_db_integrity.py", line 223, in load_id_caches
    cur.execute("SELECT corporate_number, id FROM companies WHERE corporate_number IS NOT NULL")
psycopg2.errors.UndefinedTable: relation "companies" does not exist
LINE 1: SELECT corporate_number, id FROM companies WHERE corporate_n...
                                         ^

[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[7mpsql -h localhost[7m [7m-U harumacmini -l[27m[K[A[24C[27mp[27ms[27mq[27ml[27m [27m-[27mh[27m [27ml[27mo[27mc[27ma[27ml[27mh[27mo[27ms[27mt [27m-[27mU[27m [27mh[27ma[27mr[27mu[27mm[27ma[27mc[27mm[27mi[27mn[27mi[27m [27m-[27ml[?2004l
[?1h=                                                             List of databases
   Name    |    Owner    | Encoding | Locale Provider |   C ollate   |    Ctype    | ICU Locale | ICU Rules |      Acce ss privileges      
-----------+-------------+----------+-----------------+---- ---------+-------------+------------+-----------+---------- -------------------
 postgres  | harumacmini | UTF8     | libc            | en_ US.UTF-8 | en_US.UTF-8 |            |           | 
 template0 | harumacmini | UTF8     | libc            | en_ US.UTF-8 | en_US.UTF-8 |            |           | =c/haruma cmini             +
           |             |          |                 |              |             |            |           | harumacmi ni=CTc/harumacmini
 template1 | harumacmini | UTF8     | libc            | en_ US.UTF-8 | en_US.UTF-8 |            |           | =c/haruma cmini             +
           |             |          |                 |              |             |            |           | harumacmi ni=CTc/harumacmini
(3 rows)

[7m(END)[27m[K[K[?1l>[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[7mpsql -h localhost[7m [7m-U harumacmini -d postgres -c "[27m[K
[7mCREATE TABLE companies ([27m[K
[7m    id TEXT PRIMARY KEY,[27m[K
[7m    corporate_number TEXT,[27m[K
[7m    name TEXT,[27m[K
[7m    prefecture TEXT,[27m[K
[7m    address TEXT,[27m[K
[7m    representative_name TEXT,[27m[K
[7m    overview TEXT,[27m[K
[7m    nda_flag TEXT,[27m[K
[7m    ad_flag TEXT,[27m[K
[7m    sb_flag TEXT,[27m[K
[7m    nda_flag_logical BOOLEAN DEFAULT false,[27m[K
[7m    ad_flag_logical BOOLEAN DEFAULT false,[27m[K
[7m    sb_flag_logical BOOLEAN DEFAULT false[27m[K
[7m);"[27m[K[16A[39C[27mp[27ms[27mq[27ml[27m [27m-[27mh[27m [27ml[27mo[27mc[27ma[27ml[27mh[27mo[27ms[27mt [27m-[27mU[27m [27mh[27ma[27mr[27mu[27mm[27ma[27mc[27mm[27mi[27mn[27mi[27m [27m-[27md[27m [27mp[27mo[27ms[27mt[27mg[27mr[27me[27ms[27m [27m-[27mc[27m [27m"[1B[27mC[27mR[27mE[27mA[27mT[27mE[27m [27mT[27mA[27mB[27mL[27mE[27m [27mc[27mo[27mm[27mp[27ma[27mn[27mi[27me[27ms[27m [27m([1B[27m [27m [27m [27m [27mi[27md[27m [27mT[27mE[27mX[27mT[27m [27mP[27mR[27mI[27mM[27mA[27mR[27mY[27m [27mK[27mE[27mY[27m,[1B[27m [27m [27m [27m [27mc[27mo[27mr[27mp[27mo[27mr[27ma[27mt[27me[27m_[27mn[27mu[27mm[27mb[27me[27mr[27m [27mT[27mE[27mX[27mT[27m,[1B[27m [27m [27m [27m [27mn[27ma[27mm[27me[27m [27mT[27mE[27mX[27mT[27m,[1B[27m [27m [27m [27m [27mp[27mr[27me[27mf[27me[27mc[27mt[27mu[27mr[27me[27m [27mT[27mE[27mX[27mT[27m,[1B[27m [27m [27m [27m [27ma[27md[27md[27mr[27me[27ms[27ms[27m [27mT[27mE[27mX[27mT[27m,[1B[27m [27m [27m [27m [27mr[27me[27mp[27mr[27me[27ms[27me[27mn[27mt[27ma[27mt[27mi[27mv[27me[27m_[27mn[27ma[27mm[27me[27m [27mT[27mE[27mX[27mT[27m,[1B[27m [27m [27m [27m [27mo[27mv[27me[27mr[27mv[27mi[27me[27mw[27m [27mT[27mE[27mX[27mT[27m,[1B[27m [27m [27m [27m [27mn[27md[27ma[27m_[27mf[27ml[27ma[27mg[27m [27mT[27mE[27mX[27mT[27m,[1B[27m [27m [27m [27m [27ma[27md[27m_[27mf[27ml[27ma[27mg[27m [27mT[27mE[27mX[27mT[27m,[1B[27m [27m [27m [27m [27ms[27mb[27m_[27mf[27ml[27ma[27mg[27m [27mT[27mE[27mX[27mT[27m,[1B[27m [27m [27m [27m [27mn[27md[27ma[27m_[27mf[27ml[27ma[27mg[27m_[27ml[27mo[27mg[27mi[27mc[27ma[27ml[27m [27mB[27mO[27mO[27mL[27mE[27mA[27mN[27m [27mD[27mE[27mF[27mA[27mU[27mL[27mT[27m [27mf[27ma[27ml[27ms[27me[27m,[1B[27m [27m [27m [27m [27ma[27md[27m_[27mf[27ml[27ma[27mg[27m_[27ml[27mo[27mg[27mi[27mc[27ma[27ml[27m [27mB[27mO[27mO[27mL[27mE[27mA[27mN[27m [27mD[27mE[27mF[27mA[27mU[27mL[27mT[27m [27mf[27ma[27ml[27ms[27me[27m,[1B[27m [27m [27m [27m [27ms[27mb[27m_[27mf[27ml[27ma[27mg[27m_[27ml[27mo[27mg[27mi[27mc[27ma[27ml[27m [27mB[27mO[27mO[27mL[27mE[27mA[27mN[27m [27mD[27mE[27mF[27mA[27mU[27mL[27mT[27m [27mf[27ma[27ml[27ms[27me[1B[27m)[27m;[27m"[?2004l
CREATE TABLE
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[7mdocker ps[27m[9D[27md[27mo[27mc[27mk[27me[27mr[27m [27mp[27ms[?2004l
failed to connect to the docker API at unix:///Users/harumacmini/.docker/run/docker.sock; check if the path is correct and if the daemon is running: dial unix /Users/harumacmini/.docker/run/docker.sock: connect: no such file or directory
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[7mcloud-sql-proxy a[7ml[7mbert-ma:asia-northeast1:companies-db --port 5433[27m[K[A[27mc[27ml[27mo[27mu[27md[27m-[27ms[27mq[27ml[27m-[27mp[27mr[27mo[27mx[27my[27m [27mal[27mb[27me[27mr[27mt[27m-[27mm[27ma[27m:[27ma[27ms[27mi[27ma[27m-[27mn[27mo[27mr[27mt[27mh[27me[27ma[27ms[27mt[27m1[27m:[27mc[27mo[27mm[27mp[27ma[27mn[27mi[27me[27ms[27m-[27md[27mb[27m [27m-[27m-[27mp[27mo[27mr[27mt[27m [27m5[27m4[27m3[27m3[?2004l
2026/02/26 20:07:36 Authorizing with Application Default Credentials
2026/02/26 20:07:36 [albert-ma:asia-northeast1:companies-db] could not listen to address 127.0.0.1:5433: listen tcp 127.0.0.1:5433: bind: address already in use
2026/02/26 20:07:36 Error starting proxy: [albert-ma:asia-northeast1:companies-db] Unable to mount socket: listen tcp 127.0.0.1:5433: bind: address already in use
2026/02/26 20:07:36 The proxy has encountered a terminal error: unable to start: [albert-ma:asia-northeast1:companies-db] Unable to mount socket: listen tcp 127.0.0.1:5433: bind: address already in use
2026/02/26 20:07:36 Sending SIGINT signal for proxy to shutdown.
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[7mlsof -i :5433[27m[13D[27ml[27ms[27mo[27mf[27m [27m-[27mi[27m [27m:[27m5[27m4[27m3[27m3[?2004l
COMMAND     PID        USER   FD   TYPE             DEVICE SIZE/OFF NODE NAME
cloud-sql 99273 harumacmini    6u  IPv4 0xff1614888d2854e4      0t0  TCP localhost:pyrrho (LISTEN)
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[?2004l
[1m[7m%[27m[1m[0m                                                           [0m[27m[24m[Jharumacmini@Mac-mini info_companyDetail % [K[?2004h[7mcloud-sql-proxy a[7ml[7mbert-ma:asia-northeast1:companies-db --port 5434[27m[K[A[27mc[27ml[27mo[27mu[27md[27m-[27ms[27mq[27ml[27m-[27mp[27mr[27mo[27mx[27my[27m [27mal[27mb[27me[27mr[27mt[27m-[27mm[27ma[27m:[27ma[27ms[27mi[27ma[27m-[27mn[27mo[27mr[27mt[27mh[27me[27ma[27ms[27mt[27m1[27m:[27mc[27mo[27mm[27mp[27ma[27mn[27mi[27me[27ms[27m-[27md[27mb[27m [27m-[27m-[27mp[27mo[27mr[27mt[27m [27m5[27m4[27m3[27m4[?2004l
2026/02/26 20:09:18 Authorizing with Application Default Credentials
2026/02/26 20:09:18 [albert-ma:asia-northeast1:companies-db] Listening on 127.0.0.1:5434
2026/02/26 20:09:18 The proxy has started successfully and is ready for new connections!
2026/02/26 20:10:01 [albert-ma:asia-northeast1:companies-db] Accepted connection from 127.0.0.1:54668
2026/02/26 20:11:54 [albert-ma:asia-northeast1:companies-db] client closed the connection
2026/02/26 20:13:52 [albert-ma:asia-northeast1:companies-db] Accepted connection from 127.0.0.1:54762
2026/02/26 22:34:56 [albert-ma:asia-northeast1:companies-db] client closed the connection
2026/02/27 02:31:22 [albert-ma:asia-northeast1:companies-db] Accepted connection from 127.0.0.1:60360
2026/02/27 04:58:02 [albert-ma:asia-northeast1:companies-db] client closed the connection
2026/02/27 07:19:34 [albert-ma:asia-northeast1:companies-db] Accepted connection from 127.0.0.1:64545
2026/02/27 07:25:48 [albert-ma:asia-northeast1:companies-db] client closed the connection
2026/02/27 07:28:12 [albert-ma:asia-northeast1:companies-db] Accepted connection from 127.0.0.1:64675
2026/02/27 07:53:03 [albert-ma:asia-northeast1:companies-db] client closed the connection
2026/02/27 23:28:26 [albert-ma:asia-northeast1:companies-db] Accepted connection from 127.0.0.1:62260
2026/02/27 23:28:53 [albert-ma:asia-northeast1:companies-db] client closed the connection
2026/02/28 00:13:32 [albert-ma:asia-northeast1:companies-db] Accepted connection from 127.0.0.1:63109
2026/02/28 00:29:39 [albert-ma:asia-northeast1:companies-db] client closed the connection
2026/02/28 11:21:41 [albert-ma:asia-northeast1:companies-db] Accepted connection from 127.0.0.1:55729
2026/02/28 12:11:26 [albert-ma:asia-northeast1:companies-db] client closed the connection
2026/03/01 02:36:55 [albert-ma:asia-northeast1:companies-db] Accepted connection from 127.0.0.1:53257
2026/03/01 02:36:56 [albert-ma:asia-northeast1:companies-db] client closed the connection
2026/03/01 02:49:24 [albert-ma:asia-northeast1:companies-db] Accepted connection from 127.0.0.1:53578
2026/03/01 02:49:26 [albert-ma:asia-northeast1:companies-db] client closed the connection
