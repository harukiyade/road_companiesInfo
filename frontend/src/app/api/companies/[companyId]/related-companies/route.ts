import { NextRequest, NextResponse } from "next/server";
import { pgPool } from "@/libs/postgres";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function orderByClause(sort: string) {
  if (sort === "ratio") {
    return "COALESCE(rc.voting_rights_ratio, 0) DESC, relation_strength DESC, rc.child_company_name ASC";
  }
  return "relation_strength DESC, COALESCE(rc.voting_rights_ratio, 0) DESC, rc.child_company_name ASC";
}

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ companyId: string }> }
) {
  const { companyId } = await params;
  const sort = request.nextUrl.searchParams.get("sort") ?? "strength";

  const sql = `
    SELECT
      rc.child_company_id,
      rc.child_company_name,
      rc.relation_type,
      rc.voting_rights_ratio,
      rc.source_doc_id,
      CASE
        WHEN rc.relation_type = '連結子会社' THEN 400
        WHEN rc.relation_type = '非連結子会社' THEN 300
        WHEN rc.relation_type = '子会社' THEN 250
        WHEN rc.relation_type = '持分法適用関連会社' THEN 200
        WHEN rc.relation_type = '関連会社' THEN 100
        ELSE 0
      END AS relation_strength
    FROM related_companies rc
    WHERE rc.parent_company_id = $1
    ORDER BY ${orderByClause(sort)}
  `;

  try {
    const result = await pgPool.query(sql, [companyId]);
    const items = result.rows.map((r) => ({
      childCompanyId: r.child_company_id as string | null,
      childCompanyName: r.child_company_name as string,
      relationType: (r.relation_type as string | null) ?? "",
      votingRightsRatio: r.voting_rights_ratio ? Number(r.voting_rights_ratio) : null,
      sourceDocId: (r.source_doc_id as string | null) ?? "",
      companyLink: r.child_company_id ? `/companies/${r.child_company_id}` : null,
    }));
    return NextResponse.json({ items });
  } catch (error) {
    console.error("related-companies api error", error);
    return NextResponse.json({ error: "failed to load related companies" }, { status: 500 });
  }
}

