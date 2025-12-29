import * as admin from "firebase-admin";
import * as fs from "fs";

// Firebase Admin SDKの初期化
// 既に初期化されている場合はスキップ
if (!admin.apps.length) {
  try {
    // 環境変数からサービスアカウントキーのパスを取得
    const serviceAccountPath = process.env.FIREBASE_SERVICE_ACCOUNT_KEY;
    
    if (serviceAccountPath && fs.existsSync(serviceAccountPath)) {
      // サービスアカウントキーファイルから初期化
      const serviceAccount = JSON.parse(
        fs.readFileSync(serviceAccountPath, "utf8")
      );
      admin.initializeApp({
        credential: admin.credential.cert(serviceAccount),
      });
    } else if (process.env.GOOGLE_APPLICATION_CREDENTIALS) {
      // GOOGLE_APPLICATION_CREDENTIALS環境変数から初期化
      admin.initializeApp({
        credential: admin.credential.applicationDefault(),
      });
    } else {
      // 環境変数から直接認証情報を取得（GCP環境など）
      admin.initializeApp({
        credential: admin.credential.applicationDefault(),
      });
    }
  } catch (error) {
    console.error("Firebase Admin SDK初期化エラー:", error);
    // 開発環境ではエラーをスローしない場合もある
    // throw error;
  }
}

// Firestoreインスタンスをエクスポート
export const db = admin.firestore();

