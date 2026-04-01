# CSV ↔ DB 財務「数字の指紋」・列ズレ分析

- 対象ルート: `/Users/harumacmini/programming/info_companyDetail/fixed_csv_3`
- ファイル数: 182
- 換算: import_full_update_fast に同じ（資本金×1000円、売上/利益はフォルダで×10^6 or ×1000）

## 修正優先度（スコア順）

| 優先 | ファイル | 分類 | alt列一致 | no_match | 売上オフセット分布 |
|---|---|---|---:|---:|---|
| 223189 | `fixed_csv_3/unit_million/import_firstTime_108.csv` | Type-Normal | 1249 | 110970 | {"0": 1612, "-3": 26, "1": 105} |
| 223187 | `fixed_csv_3/unit_million/import_firstTime_107.csv` | Type-Normal | 1251 | 110968 | {"0": 1612, "-3": 26, "1": 105} |
| 223187 | `fixed_csv_3/unit_million/import_firstTime_36.csv` | Type-Normal | 1251 | 110968 | {"0": 1612, "-3": 26, "1": 105} |
| 28594 | `fixed_csv_3/unit_million/import_firstTime_110.csv` | Type-Normal | 90 | 14252 | {"0": 17} |
| 28587 | `fixed_csv_3/unit_million/import_firstTime_115.csv` | Type-Normal | 187 | 14200 | {"0": 17} |
| 28435 | `fixed_csv_3/unit_million/import_firstTime_47.csv` | Type-Normal | 69 | 14183 | {"0": 25, "1": 2} |
| 28399 | `fixed_csv_3/unit_million/import_firstTime_112.csv` | Type-Normal | 115 | 14142 | {"0": 140, "-3": 2, "1": 1} |
| 28258 | `fixed_csv_3/unit_million/import_firstTime_44.csv` | Type-Normal | 102 | 14078 | {"0": 156, "-3": 2, "1": 1} |
| 28233 | `fixed_csv_3/unit_million/import_firstTime_42.csv` | Type-Normal | 123 | 14055 | {"0": 66} |
| 28066 | `fixed_csv_3/unit_million/import_firstTime_116.csv` | Type-Normal | 170 | 13948 | {"0": 94, "1": 17, "-3": 2} |
| 27931 | `fixed_csv_3/unit_million/import_firstTime_46.csv` | Type-Normal | 167 | 13882 | {"0": 108, "-3": 3, "1": 21} |
| 27717 | `fixed_csv_3/unit_million/import_firstTime_113.csv` | Type-Normal | 191 | 13763 | {"0": 113, "1": 42, "-3": 13} |
| 27696 | `fixed_csv_3/unit_million/import_firstTime_48.csv` | Type-Normal | 130 | 13783 | {"0": 110, "1": 15, "-3": 2} |
| 27644 | `fixed_csv_3/unit_million/import_firstTime_114.csv` | Type-Normal | 214 | 13715 | {"0": 93, "-3": 15, "1": 54} |
| 27536 | `fixed_csv_3/unit_million/import_firstTime_45.csv` | Type-Normal | 182 | 13677 | {"0": 112, "1": 37, "-3": 12} |
| 27526 | `fixed_csv_3/unit_million/import_firstTime_43.csv` | Type-Normal | 206 | 13660 | {"0": 140, "-3": 1, "1": 1} |
| 27478 | `fixed_csv_3/unit_million/import_firstTime_111.csv` | Type-Normal | 216 | 13631 | {"0": 146, "-3": 1, "1": 1} |
| 27477 | `fixed_csv_3/unit_million/111.fixed.csv` | Type-Normal | 215 | 13631 | {"0": 146, "-3": 1, "1": 1} |
| 27237 | `fixed_csv_3/unit_million/import_firstTime_117.csv` | Type-Normal | 451 | 13393 | {"0": 200, "1": 30, "-3": 14} |
| 24486 | `fixed_csv_3/unit_million/import_firstTime_49.csv` | Type-Normal | 218 | 12134 | {"0": 608, "-3": 6, "1": 22} |
| 7755 | `fixed_csv_3/unit_million/import_firstTime_119.csv` | Type-Normal | 29 | 3863 | {"0": 10561, "8": 1, "-3": 2} |
| 5838 | `fixed_csv_3/unit_million/import_firstTime_41.csv` | Type-Normal | 20 | 2909 | {"0": 2686, "-3": 16} |
| 3230 | `fixed_csv_3/unit_million/import_firstTime_50.csv` | Type-Normal | 54 | 1588 | {"0": 287, "1": 6} |
| 3001 | `fixed_csv_3/unit_million/9.csv` | Type-Normal | 1 | 1500 | {"0": 578} |
| 2074 | `fixed_csv_3/unit_million/import_firstTime_24.csv` | Type-Normal | 6 | 1034 | {"0": 4958} |
| 1743 | `fixed_csv_3/unit_million/16.csv` | Type-Normal | 1 | 871 | {"0": 1131} |
| 1682 | `fixed_csv_3/unit_yen/24.csv` | Type-Normal | 0 | 841 | {"0": 735} |
| 1648 | `fixed_csv_3/unit_yen/import_firstTime_40.csv` | Type-Normal | 2 | 823 | {"0": 6, "1": 1, "-3": 1} |
| 1593 | `fixed_csv_3/unit_million/import_firstTime_134.csv` | Type-Normal | 9 | 792 | {"0": 4695} |
| 1479 | `fixed_csv_3/unit_yen/20.csv` | Type-Normal | 1 | 739 | {"0": 1055} |
| 1389 | `fixed_csv_3/unit_million/7.csv` | Type-Normal | 3 | 693 | {"0": 1813, "8": 2} |
| 1321 | `fixed_csv_3/unit_million/32.csv` | Type-Normal | 9 | 656 | {"0": 1309, "1": 2, "8": 3, "-4": 1} |
| 1310 | `fixed_csv_3/unit_million/11.csv` | Type-Normal | 0 | 655 | {"0": 754} |
| 1167 | `fixed_csv_3/unit_million/21.csv` | Type-Normal | 1 | 583 | {"0": 858} |
| 1158 | `fixed_csv_3/unit_yen/29.csv` | Type-Normal | 0 | 579 | {"0": 1164} |
| 1078 | `fixed_csv_3/unit_yen/27.csv` | Type-Normal | 0 | 539 | {"0": 866} |
| 1038 | `fixed_csv_3/unit_million/8.csv` | Type-Normal | 2 | 518 | {"0": 906} |
| 1026 | `fixed_csv_3/unit_million/17.csv` | Type-Normal | 0 | 513 | {"0": 962} |
| 1005 | `fixed_csv_3/unit_yen/10.csv` | Type-Normal | 1 | 502 | {"0": 1224} |
| 990 | `fixed_csv_3/unit_million/25.csv` | Type-Normal | 0 | 495 | {"0": 408} |
| 961 | `fixed_csv_3/unit_million/6.csv` | Type-Normal | 3 | 479 | {"0": 1235, "1": 1, "8": 1} |
| 958 | `fixed_csv_3/unit_million/18.csv` | Type-Normal | 2 | 478 | {"0": 1487, "8": 2} |
| 865 | `fixed_csv_3/unit_million/import_firstTime_133.csv` | Type-Normal | 5 | 430 | {"0": 1878} |
| 758 | `fixed_csv_3/unit_yen/16_20251224.csv` | Type-Normal | 0 | 379 | {"0": 2226} |
| 738 | `fixed_csv_3/unit_yen/19.csv` | Type-Normal | 0 | 369 | {"0": 675} |
| 736 | `fixed_csv_3/unit_million/10_20251224.csv` | Type-Normal | 0 | 368 | {"0": 2132} |
| 632 | `fixed_csv_3/unit_million/14.csv` | Type-Normal | 0 | 316 | {"0": 128} |
| 588 | `fixed_csv_3/unit_million/18_20251224.csv` | Type-Normal | 0 | 294 | {"0": 2238} |
| 514 | `fixed_csv_3/unit_yen/23.csv` | Type-Normal | 0 | 257 | {"0": 654} |
| 484 | `fixed_csv_3/unit_million/2_20251224.csv` | Type-Normal | 0 | 242 | {"0": 2000} |
| 461 | `fixed_csv_3/unit_million/12.csv` | Type-Normal | 1 | 230 | {"0": 396} |
| 432 | `fixed_csv_3/unit_yen/30.csv` | Type-Normal | 2 | 215 | {"0": 418, "8": 2} |
| 411 | `fixed_csv_3/unit_million/8_20251224.csv` | Type-Normal | 1 | 205 | {"0": 2021} |
| 402 | `fixed_csv_3/unit_million/7_20251224.csv` | Type-Normal | 0 | 201 | {"0": 2476} |
| 350 | `fixed_csv_3/unit_yen/31.csv` | Type-Normal | 2 | 174 | {"0": 362, "8": 2} |
| 347 | `fixed_csv_3/unit_million/9_20251224.csv` | Type-Normal | 1 | 173 | {"0": 1906} |
| 334 | `fixed_csv_3/unit_million/4_20251224.csv` | Type-Normal | 0 | 167 | {"0": 1447} |
| 322 | `fixed_csv_3/unit_million/5_20251224.csv` | Type-Normal | 0 | 161 | {"0": 2502} |
| 320 | `fixed_csv_3/unit_million/1_20251224.csv` | Type-Normal | 0 | 160 | {"0": 2038} |
| 259 | `fixed_csv_3/unit_yen/13.csv` | Type-Normal | 1 | 129 | {"0": 232, "8": 1} |
| 252 | `fixed_csv_3/unit_million/28.csv` | Type-Normal | 0 | 126 | {"0": 160} |
| 225 | `fixed_csv_3/unit_million/3_20251224.csv` | Type-Normal | 3 | 111 | {"0": 2019, "1": 1} |
| 201 | `fixed_csv_3/unit_yen/15.csv` | Type-Normal | 1 | 100 | {"0": 181} |
| 175 | `fixed_csv_3/unit_million/17_20251224.csv` | Type-Normal | 1 | 87 | {"0": 2329, "1": 1} |
| 114 | `fixed_csv_3/unit_million/2.csv` | Type-Normal | 0 | 57 | {"0": 907} |
| 97 | `fixed_csv_3/unit_million/12_20251224.csv` | Type-Normal | 1 | 48 | {"0": 2701, "-3": 1} |
| 97 | `fixed_csv_3/unit_million/6_20251224.csv` | Type-Normal | 1 | 48 | {"0": 2692, "-3": 1} |
| 80 | `fixed_csv_3/unit_yen/11_20251224.csv` | Type-Normal | 0 | 40 | {"0": 239} |
| 42 | `fixed_csv_3/unit_million/22.csv` | Type-Normal | 0 | 21 | {"0": 23} |
| 0 | `fixed_csv_3/later_2/yuzuri_1.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/later_2/yuzuri_10.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/later_2/yuzuri_12.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/later_2/yuzuri_14.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/later_2/yuzuri_16.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/later_2/yuzuri_17.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/later_2/yuzuri_19.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/later_2/yuzuri_2.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/later_2/yuzuri_21.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/later_2/yuzuri_3.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/later_2/yuzuri_5.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/later_2/yuzuri_6.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/later_2/yuzuri_8.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/later_2/yuzuri_9.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/1.csv` | Type-Normal | 0 | 0 | {"0": 2825} |
| 0 | `fixed_csv_3/unit_million/108.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/118.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/3.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/4.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_1.csv` | Type-NoFinancialMatch | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_10.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_100.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_101.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_102.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_103.csv` | Type-NoFinancialMatch | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_106.csv` | Type-NoFinancialMatch | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_11.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_118.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_12.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_120.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_121.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_122.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_123.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_125.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_13.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_14.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_15.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_16.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_17.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_18.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_19.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_2.csv` | Type-NoFinancialMatch | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_20.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_21.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_22.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_23.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_25.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_26.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_27.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_28.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_29.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_30.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_31.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_32.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_33.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_34.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_35.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_39.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_4.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_52.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_54.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_55.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_56.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_57.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_58.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_59.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_60.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_62.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_63.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_64.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_65.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_66.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_67.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_68.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_69.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_7.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_70.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_71.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_72.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_73.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_74.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_75.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_76.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_77.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_8.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_9.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_million/import_firstTime_90.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_yen/import_firstTime_105.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_yen/import_firstTime_124.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_yen/import_firstTime_3.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_yen/import_firstTime_5.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_yen/import_firstTime_6.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_yen/import_firstTime_78.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_yen/import_firstTime_79.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_yen/import_firstTime_80.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_yen/import_firstTime_81.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_yen/import_firstTime_82.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_yen/import_firstTime_83.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_yen/import_firstTime_84.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_yen/import_firstTime_85.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_yen/import_firstTime_86.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_yen/import_firstTime_87.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_yen/import_firstTime_88.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_yen/import_firstTime_89.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_yen/import_firstTime_91.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_yen/import_firstTime_92.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_yen/import_firstTime_93.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_yen/import_firstTime_94.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_yen/import_firstTime_95.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_yen/import_firstTime_96.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_yen/import_firstTime_97.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_yen/import_firstTime_98.csv` | Type-NoData | 0 | 0 | {} |
| 0 | `fixed_csv_3/unit_yen/import_firstTime_99.csv` | Type-NoData | 0 | 0 | {} |

## 分類の意味

- **Type-Normal**: 支配的オフセットが 0（ヘッダー列のパース値が DB と一致する行が多い）
- **Type-Shift-Lk**: 売上・資本・利益で同じ負のオフセット k が ~72% 以上で一致（データが左にずれている＝本来より左の列に正値）
- **Type-Shift-Unknown**: 行ごとにオフセットがバラバラ、または項目間で食い違い

詳細 JSON: `results.json`