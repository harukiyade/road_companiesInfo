import { NextRequest, NextResponse } from "next/server";
import { db } from "@/libs/firebaseAdmin";
import { fetchCompanyWebInfo, CompanyBasic } from "@/libs/webinfo/fetchCompanyWebInfo";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

/**
 * POST /api/companies/webinfo
 * 企業のWeb情報を取得してFirestoreに保存する
 */
export async function POST(request: NextRequest) {
  try {
    // リクエストボディを取得
    const body = await request.json();
    const { companyId } = body;

    // companyIdのバリデーション
    if (!companyId || typeof companyId !== "string" || companyId.trim() === "") {
      return NextResponse.json(
        { error: "companyId is required and must be a non-empty string" },
        { status: 400 }
      );
    }

    // 企業基本情報を取得
    const companyDoc = await db
      .collection("companies_new")
      .doc(companyId)
      .get();

    if (!companyDoc.exists) {
      return NextResponse.json(
        { error: `Company with id ${companyId} not found` },
        { status: 404 }
      );
    }

    const companyData = companyDoc.data();
    if (!companyData) {
      return NextResponse.json(
        { error: `Company data is empty for id ${companyId}` },
        { status: 404 }
      );
    }

    // CompanyBasic型に変換
    const company: CompanyBasic = {
      id: companyId,
      name: companyData.name || "",
      corporateNumber: companyData.corporateNumber || undefined,
      headquartersAddress: companyData.headquartersAddress || undefined,
    };

    // Web情報を取得＆保存
    const result = await fetchCompanyWebInfo(company);

    // レスポンスを返す
    return NextResponse.json(
      {
        success: true,
        result,
      },
      { status: 200 }
    );
  } catch (error) {
    console.error("APIエラー:", error);
    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "An unexpected error occurred",
      },
      { status: 500 }
    );
  }
}

