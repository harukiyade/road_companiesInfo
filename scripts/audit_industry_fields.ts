/* eslint-disable no-console */

/**
 * scripts/audit_industry_fields.ts
 *
 * ✅ 目的
 * - companies_new と companies_index の業種系フィールドを実データ分析
 * - 対象フィールド: industryLarge, industryMiddle, industrySmall, industryDetail
 * - 埋まり率、型、値の分布、異常検出
 * - 割り振り判定とサンプル検証
 * - 整合性チェック
 *
 * ✅ 必要ENV
 * - FIREBASE_SERVICE_ACCOUNT_KEY=/absolute/path/to/serviceAccount.json (必須)
 *
 * ✅ 出力
 * - out/industry_audit_summary.json: 集計結果
 * - out/industry_audit_samples.csv: サンプル検証結果
 * - out/industry_audit_anomalies.csv: 異常検出結果
 */

import admin from "firebase-admin";
import * as fs from "fs";
import * as path from "path";

// ------------------------------
// Firebase Admin SDK 初期化
// ------------------------------
if (!admin.apps.length) {
  try {
    const serviceAccountPath = process.env.FIREBASE_SERVICE_ACCOUNT_KEY;

    if (!serviceAccountPath) {
      console.error("❌ エラー: FIREBASE_SERVICE_ACCOUNT_KEY 環境変数が設定されていません。");
      process.exit(1);
    }

    if (!fs.existsSync(serviceAccountPath)) {
      console.error(`❌ エラー: サービスアカウントキーファイルが存在しません: ${serviceAccountPath}`);
      process.exit(1);
    }

    const serviceAccount = JSON.parse(
      fs.readFileSync(serviceAccountPath, "utf8")
    );

    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
      projectId: "albert-ma",
    });

    console.log("[Firebase初期化] ✅ 初期化が完了しました");
  } catch (error) {
    console.error("❌ Firebase初期化エラー:", error);
    process.exit(1);
  }
}

const db = admin.firestore();

// ------------------------------
// 型定義
// ------------------------------

interface FieldStats {
  total: number;
  filled: number;
  null: number;
  empty: number;
  types: {
    string: number;
    array: number;
    number: number;
    boolean: number;
    other: number;
  };
  topValues: Array<{ value: string; count: number }>;
}

interface IndustryFieldAudit {
  fieldName: string;
  stats: FieldStats;
}

interface HierarchyIssue {
  docId: string;
  name: string;
  issue: string;
  details: any;
}

interface SampleRecord {
  docId: string;
  name: string;
  industryLarge: string | null;
  industryMiddle: string | null;
  industrySmall: string | null;
  industryDetail: string | null;
  normalizedLarge: string | null;
  normalizedMiddle: string | null;
  normalizedSmall: string | null;
  normalizedDetail: string | null;
  classificationStatus: "OK" | "NG";
  classificationReason: string;
}

interface AuditResult {
  collection: string;
  totalCount: number;
  fieldAudits: IndustryFieldAudit[];
  hierarchyIssues: HierarchyIssue[];
  typeIssues: Array<{ docId: string; field: string; expectedType: string; actualType: string }>;
  consistencyCheck: {
    companiesNewCount: number;
    companiesIndexCount: number;
    matchedCount: number;
    mismatchCount: number;
    missingInIndex: number;
  };
}

// ------------------------------
// 正規化関数
// ------------------------------

/**
 * テキストを正規化（NFKC、空白除去、全角/半角統一）
 */
function normalizeText(text: string | null | undefined): string | null {
  if (!text || typeof text !== "string") {
    return null;
  }

  return text
    .trim()
    .replace(/\s+/g, " ") // 連続空白を1つに
    .replace(/　/g, " ") // 全角スペースを半角に
    .normalize("NFKC"); // NFKC正規化
}

/**
 * 値の型を判定
 */
function getValueType(value: any): string {
  if (value === null) return "null";
  if (value === undefined) return "undefined";
  if (Array.isArray(value)) return "array";
  if (typeof value === "string") return "string";
  if (typeof value === "number") return "number";
  if (typeof value === "boolean") return "boolean";
  return "other";
}

/**
 * 値が空かどうかを判定
 */
function isEmpty(value: any): boolean {
  if (value === null || value === undefined) return true;
  if (typeof value === "string" && value.trim().length === 0) return true;
  if (Array.isArray(value) && value.length === 0) return true;
  return false;
}

// ------------------------------
// フィールド統計の収集
// ------------------------------

/**
 * フィールドの統計を収集
 */
function collectFieldStats(
  fieldName: string,
  values: Map<string, any>,
  totalCount: number
): FieldStats {
  const stats: FieldStats = {
    total: totalCount,
    filled: 0,
    null: 0,
    empty: 0,
    types: {
      string: 0,
      array: 0,
      number: 0,
      boolean: 0,
      other: 0,
    },
    topValues: [],
  };

  const valueCounts = new Map<string, number>();

  for (const value of values.values()) {
    const type = getValueType(value);
    stats.types[type as keyof typeof stats.types]++;

    if (isEmpty(value)) {
      stats.empty++;
      if (value === null) {
        stats.null++;
      }
    } else {
      stats.filled++;

      // 値のカウント（文字列と配列のみ）
      if (type === "string") {
        const normalized = normalizeText(value) || "";
        if (normalized.length > 0) {
          valueCounts.set(normalized, (valueCounts.get(normalized) || 0) + 1);
        }
      } else if (type === "array") {
        for (const item of value) {
          if (typeof item === "string") {
            const normalized = normalizeText(item) || "";
            if (normalized.length > 0) {
              valueCounts.set(normalized, (valueCounts.get(normalized) || 0) + 1);
            }
          }
        }
      }
    }
  }

  // Top 50を取得
  stats.topValues = Array.from(valueCounts.entries())
    .map(([value, count]) => ({ value, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 50);

  return stats;
}

// ------------------------------
// 異常検出
// ------------------------------

/**
 * 階層崩れを検出
 */
function detectHierarchyIssues(
  docs: admin.firestore.QueryDocumentSnapshot[]
): HierarchyIssue[] {
  const issues: HierarchyIssue[] = [];

  for (const doc of docs) {
    const data = doc.data();
    const docId = doc.id;
    const name = data.name || "";

    // DetailがあるのにLargeがない
    if (data.industryDetail && !data.industryLarge) {
      issues.push({
        docId,
        name,
        issue: "industryDetail exists but industryLarge is missing",
        details: {
          industryDetail: data.industryDetail,
          industryLarge: data.industryLarge,
        },
      });
    }

    // SmallがあるのにMiddleがない
    if (data.industrySmall && !data.industryMiddle) {
      issues.push({
        docId,
        name,
        issue: "industrySmall exists but industryMiddle is missing",
        details: {
          industrySmall: data.industrySmall,
          industryMiddle: data.industryMiddle,
        },
      });
    }

    // MiddleがあるのにLargeがない
    if (data.industryMiddle && !data.industryLarge) {
      issues.push({
        docId,
        name,
        issue: "industryMiddle exists but industryLarge is missing",
        details: {
          industryMiddle: data.industryMiddle,
          industryLarge: data.industryLarge,
        },
      });
    }
  }

  return issues;
}

/**
 * 型崩れを検出
 */
function detectTypeIssues(
  docs: admin.firestore.QueryDocumentSnapshot[],
  expectedTypes: { [field: string]: string }
): Array<{ docId: string; field: string; expectedType: string; actualType: string }> {
  const issues: Array<{ docId: string; field: string; expectedType: string; actualType: string }> = [];

  for (const doc of docs) {
    const data = doc.data();
    const docId = doc.id;

    for (const [field, expectedType] of Object.entries(expectedTypes)) {
      if (data[field] !== undefined && data[field] !== null) {
        const actualType = getValueType(data[field]);
        if (actualType !== expectedType && actualType !== "null" && actualType !== "undefined") {
          issues.push({
            docId,
            field,
            expectedType,
            actualType,
          });
        }
      }
    }
  }

  return issues;
}

// ------------------------------
// 割り振り判定
// ------------------------------

/**
 * 業種分類の割り振りが正しいかを判定
 */
function classifyIndustryStatus(
  data: any,
  docId: string
): { status: "OK" | "NG"; reason: string } {
  const large = normalizeText(data.industryLarge);
  const middle = normalizeText(data.industryMiddle);
  const small = normalizeText(data.industrySmall);
  const detail = normalizeText(data.industryDetail);

  // 全て空の場合は判定不能
  if (!large && !middle && !small && !detail) {
    return {
      status: "NG",
      reason: "全ての階層フィールドが空",
    };
  }

  // 階層の整合性チェック
  if (detail && !small) {
    return {
      status: "NG",
      reason: "細分類があるのに小分類がない",
    };
  }
  if (small && !middle) {
    return {
      status: "NG",
      reason: "小分類があるのに中分類がない",
    };
  }
  if (middle && !large) {
    return {
      status: "NG",
      reason: "中分類があるのに大分類がない",
    };
  }

  // 最低限大分類があればOK
  if (large) {
    return {
      status: "OK",
      reason: "大分類が設定されている",
    };
  }

  return {
    status: "NG",
    reason: "大分類が設定されていない",
  };
}

// ------------------------------
// メイン処理
// ------------------------------

async function auditCollection(collectionName: string): Promise<AuditResult> {
  console.log(`\n📊 ${collectionName} コレクションの監査を開始...`);

  const BATCH_SIZE = 1000;
  const industryFields = [
    "industryLarge",
    "industryMiddle",
    "industrySmall",
    "industryDetail",
  ];

  // 全フィールドの値を収集
  const fieldValues = new Map<string, Map<string, any>>();
  for (const field of industryFields) {
    fieldValues.set(field, new Map());
  }

  const allDocs: admin.firestore.QueryDocumentSnapshot[] = [];
  let totalCount = 0;
  let lastDoc: admin.firestore.QueryDocumentSnapshot | null = null;

  // 全ドキュメントを取得
  while (true) {
    let query = db.collection(collectionName).limit(BATCH_SIZE);
    if (lastDoc) {
      query = query.startAfter(lastDoc);
    }

    const snapshot = await query.get();
    if (snapshot.empty) {
      break;
    }

    for (const doc of snapshot.docs) {
      allDocs.push(doc);
      const data = doc.data();

      for (const field of industryFields) {
        if (data[field] !== undefined) {
          fieldValues.get(field)!.set(doc.id, data[field]);
        }
      }
    }

    totalCount += snapshot.size;
    lastDoc = snapshot.docs[snapshot.docs.length - 1];
    console.log(`  取得済み: ${totalCount} 件`);
  }

  console.log(`  総件数: ${totalCount} 件`);

  // フィールド統計を収集
  const fieldAudits: IndustryFieldAudit[] = [];
  for (const field of industryFields) {
    const stats = collectFieldStats(field, fieldValues.get(field)!, totalCount);
    fieldAudits.push({
      fieldName: field,
      stats,
    });
  }

  // 異常検出
  const hierarchyIssues = detectHierarchyIssues(allDocs);
  const expectedTypes: { [field: string]: string } = {
    industryLarge: "string",
    industryMiddle: "string",
    industrySmall: "string",
    industryDetail: "string",
  };
  const typeIssues = detectTypeIssues(allDocs, expectedTypes);

  return {
    collection: collectionName,
    totalCount,
    fieldAudits,
    hierarchyIssues,
    typeIssues,
    consistencyCheck: {
      companiesNewCount: 0,
      companiesIndexCount: 0,
      matchedCount: 0,
      mismatchCount: 0,
      missingInIndex: 0,
    },
  };
}

/**
 * サンプル検証（100件）
 */
async function sampleValidation(collectionName: string): Promise<SampleRecord[]> {
  console.log(`\n🔍 ${collectionName} のサンプル検証を開始...`);

  const snapshot = await db
    .collection(collectionName)
    .limit(100)
    .get();

  const samples: SampleRecord[] = [];

  for (const doc of snapshot.docs) {
    const data = doc.data();
    const classification = classifyIndustryStatus(data, doc.id);

    samples.push({
      docId: doc.id,
      name: data.name || "",
      industryLarge: data.industryLarge || null,
      industryMiddle: data.industryMiddle || null,
      industrySmall: data.industrySmall || null,
      industryDetail: data.industryDetail || null,
      normalizedLarge: normalizeText(data.industryLarge),
      normalizedMiddle: normalizeText(data.industryMiddle),
      normalizedSmall: normalizeText(data.industrySmall),
      normalizedDetail: normalizeText(data.industryDetail),
      classificationStatus: classification.status,
      classificationReason: classification.reason,
    });
  }

  return samples;
}

/**
 * companies_new と companies_index の整合性チェック
 */
async function checkConsistency(
  companiesNewAudit: AuditResult,
  companiesIndexAudit: AuditResult
): Promise<void> {
  console.log(`\n🔗 整合性チェックを開始...`);

  // companies_new の全ドキュメントを取得
  const newDocs = new Map<string, any>();
  let lastDoc: admin.firestore.QueryDocumentSnapshot | null = null;

  while (true) {
    let query = db.collection("companies_new").limit(1000);
    if (lastDoc) {
      query = query.startAfter(lastDoc);
    }

    const snapshot = await query.get();
    if (snapshot.empty) {
      break;
    }

    for (const doc of snapshot.docs) {
      newDocs.set(doc.id, doc.data());
    }

    lastDoc = snapshot.docs[snapshot.docs.length - 1];
  }

  // companies_index の全ドキュメントを取得
  const indexDocs = new Map<string, any>();
  lastDoc = null;

  while (true) {
    let query = db.collection("companies_index").limit(1000);
    if (lastDoc) {
      query = query.startAfter(lastDoc);
    }

    const snapshot = await query.get();
    if (snapshot.empty) {
      break;
    }

    for (const doc of snapshot.docs) {
      indexDocs.set(doc.id, doc.data());
    }

    lastDoc = snapshot.docs[snapshot.docs.length - 1];
  }

  let matchedCount = 0;
  let mismatchCount = 0;
  let missingInIndex = 0;

  for (const [docId, newData] of newDocs.entries()) {
    const indexData = indexDocs.get(docId);

    if (!indexData) {
      missingInIndex++;
      continue;
    }

    // 業種フィールドの比較
    const fields = ["industryLarge", "industryMiddle", "industrySmall", "industryDetail"];
    let isMatch = true;

    for (const field of fields) {
      const newValue = normalizeText(newData[field]);
      const indexValue = normalizeText(indexData[field]);

      if (newValue !== indexValue) {
        isMatch = false;
        break;
      }
    }

    if (isMatch) {
      matchedCount++;
    } else {
      mismatchCount++;
    }
  }

  companiesNewAudit.consistencyCheck = {
    companiesNewCount: newDocs.size,
    companiesIndexCount: indexDocs.size,
    matchedCount,
    mismatchCount,
    missingInIndex,
  };

  companiesIndexAudit.consistencyCheck = companiesNewAudit.consistencyCheck;
}

/**
 * CSV出力
 */
function writeCSV(filePath: string, headers: string[], rows: any[][]): void {
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }

  const lines = [headers.join(",")];
  for (const row of rows) {
    const escaped = row.map((cell) => {
      const str = String(cell || "");
      if (str.includes(",") || str.includes('"') || str.includes("\n")) {
        return `"${str.replace(/"/g, '""')}"`;
      }
      return str;
    });
    lines.push(escaped.join(","));
  }

  fs.writeFileSync(filePath, lines.join("\n"), "utf8");
}

/**
 * メイン実行
 */
async function main() {
  try {
    console.log("業種フィールド監査を開始...");

    // 出力ディレクトリを作成
    const outDir = path.join(process.cwd(), "out");
    if (!fs.existsSync(outDir)) {
      fs.mkdirSync(outDir, { recursive: true });
    }

    // companies_new の監査
    const companiesNewAudit = await auditCollection("companies_new");
    const companiesNewSamples = await sampleValidation("companies_new");

    // companies_index の監査（存在する場合）
    let companiesIndexAudit: AuditResult | null = null;
    let companiesIndexSamples: SampleRecord[] = [];

    try {
      const indexSnapshot = await db.collection("companies_index").limit(1).get();
      if (!indexSnapshot.empty) {
        companiesIndexAudit = await auditCollection("companies_index");
        companiesIndexSamples = await sampleValidation("companies_index");

        // 整合性チェック
        await checkConsistency(companiesNewAudit, companiesIndexAudit);
      } else {
        console.log("\n⚠️  companies_index コレクションが見つかりませんでした。");
      }
    } catch (error) {
      console.log("\n⚠️  companies_index コレクションへのアクセスエラー:", error);
    }

    // 結果をJSONに出力
    const summary = {
      companiesNew: companiesNewAudit,
      companiesIndex: companiesIndexAudit,
      timestamp: new Date().toISOString(),
    };

    fs.writeFileSync(
      path.join(outDir, "industry_audit_summary.json"),
      JSON.stringify(summary, null, 2),
      "utf8"
    );

    // サンプルをCSVに出力
    const sampleHeaders = [
      "docId",
      "name",
      "industryLarge",
      "industryMiddle",
      "industrySmall",
      "industryDetail",
      "normalizedLarge",
      "normalizedMiddle",
      "normalizedSmall",
      "normalizedDetail",
      "classificationStatus",
      "classificationReason",
    ];

    const newSampleRows = companiesNewSamples.map((s) => [
      s.docId,
      s.name,
      s.industryLarge || "",
      s.industryMiddle || "",
      s.industrySmall || "",
      s.industryDetail || "",
      s.normalizedLarge || "",
      s.normalizedMiddle || "",
      s.normalizedSmall || "",
      s.normalizedDetail || "",
      s.classificationStatus,
      s.classificationReason,
    ]);

    writeCSV(path.join(outDir, "industry_audit_samples.csv"), sampleHeaders, newSampleRows);

    // 異常検出結果をCSVに出力
    const anomalyHeaders = ["docId", "name", "issue", "details"];
    const anomalyRows: any[][] = [];

    for (const issue of companiesNewAudit.hierarchyIssues) {
      anomalyRows.push([
        issue.docId,
        issue.name,
        issue.issue,
        JSON.stringify(issue.details),
      ]);
    }

    for (const issue of companiesNewAudit.typeIssues) {
      anomalyRows.push([
        issue.docId,
        "",
        `Type mismatch: ${issue.field} (expected: ${issue.expectedType}, actual: ${issue.actualType})`,
        "",
      ]);
    }

    if (companiesIndexAudit) {
      for (const issue of companiesIndexAudit.hierarchyIssues) {
        anomalyRows.push([
          issue.docId,
          issue.name,
          `[companies_index] ${issue.issue}`,
          JSON.stringify(issue.details),
        ]);
      }

      for (const issue of companiesIndexAudit.typeIssues) {
        anomalyRows.push([
          issue.docId,
          "",
          `[companies_index] Type mismatch: ${issue.field} (expected: ${issue.expectedType}, actual: ${issue.actualType})`,
          "",
        ]);
      }
    }

    writeCSV(path.join(outDir, "industry_audit_anomalies.csv"), anomalyHeaders, anomalyRows);

    // 結果を表示
    console.log("\n✅ 監査完了");
    console.log(`\n📊 結果サマリー:`);
    console.log(`  companies_new:`);
    console.log(`    総件数: ${companiesNewAudit.totalCount}`);
    console.log(`    階層崩れ: ${companiesNewAudit.hierarchyIssues.length} 件`);
    console.log(`    型崩れ: ${companiesNewAudit.typeIssues.length} 件`);

    if (companiesIndexAudit) {
      console.log(`  companies_index:`);
      console.log(`    総件数: ${companiesIndexAudit.totalCount}`);
      console.log(`    階層崩れ: ${companiesIndexAudit.hierarchyIssues.length} 件`);
      console.log(`    型崩れ: ${companiesIndexAudit.typeIssues.length} 件`);

      console.log(`  整合性チェック:`);
      console.log(`    一致: ${companiesNewAudit.consistencyCheck.matchedCount} 件`);
      console.log(`    不一致: ${companiesNewAudit.consistencyCheck.mismatchCount} 件`);
      console.log(`    indexに存在しない: ${companiesNewAudit.consistencyCheck.missingInIndex} 件`);
    }

    console.log(`\n📁 出力ファイル:`);
    console.log(`  - out/industry_audit_summary.json`);
    console.log(`  - out/industry_audit_samples.csv`);
    console.log(`  - out/industry_audit_anomalies.csv`);

  } catch (error) {
    console.error("エラー:", error);
    process.exit(1);
  }
}

// ------------------------------
// 実行
// ------------------------------
main()
  .then(() => {
    console.log("処理完了");
    process.exit(0);
  })
  .catch((error) => {
    console.error("エラー:", error);
    process.exit(1);
  });
