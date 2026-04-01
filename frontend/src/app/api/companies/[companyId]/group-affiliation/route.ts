import { NextResponse } from "next/server";
import { pgPool } from "@/libs/postgres";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function relationStrength(relationType: string | null): number {
  if (relationType === "連結子会社") return 400;
  if (relationType === "非連結子会社") return 300;
  if (relationType === "子会社") return 250;
  if (relationType === "持分法適用関連会社") return 200;
  if (relationType === "関連会社") return 100;
  return 0;
}

export async function GET(
  _request: Request,
  { params }: { params: Promise<{ companyId: string }> }
) {
  const { companyId } = await params;

  try {
    const companyRes = await pgPool.query(
      "SELECT id, name FROM companies WHERE id = $1 LIMIT 1",
      [companyId]
    );
    if (companyRes.rows.length === 0) {
      return NextResponse.json({ groupAffiliation: null }, { status: 200 });
    }
    const companyName = (companyRes.rows[0].name as string | null) ?? "";

    const relationRes = await pgPool.query(
      `
      SELECT
        cr.parent_company_id,
        p.name AS parent_company_name,
        cr.relation_type,
        cr.voting_rights_ratio
      FROM company_relations cr
      LEFT JOIN companies p ON p.id::text = cr.parent_company_id::text
      WHERE cr.child_company_id::text = $1
         OR ($2 <> '' AND cr.child_company_name = $2)
      `,
      [companyId, companyName]
    );

    if (relationRes.rows.length === 0) {
      return NextResponse.json({ groupAffiliation: null }, { status: 200 });
    }

    const best = relationRes.rows
      .map((r) => ({
        parentCompanyId: (r.parent_company_id as string | null) ?? "",
        parentCompanyName: (r.parent_company_name as string | null) ?? "",
        relationType: (r.relation_type as string | null) ?? "",
        votingRightsRatio:
          r.voting_rights_ratio != null ? Number(r.voting_rights_ratio) : null,
        strength: relationStrength((r.relation_type as string | null) ?? null),
      }))
      .sort((a, b) => {
        if (b.strength !== a.strength) return b.strength - a.strength;
        return (b.votingRightsRatio ?? 0) - (a.votingRightsRatio ?? 0);
      })[0];

    return NextResponse.json(
      {
        groupAffiliation: {
          parentCompanyId: best.parentCompanyId,
          parentCompanyName: best.parentCompanyName,
          relationType: best.relationType,
          votingRightsRatio: best.votingRightsRatio,
          parentCompanyLink: best.parentCompanyId
            ? `/companies/${best.parentCompanyId}`
            : null,
        },
      },
      { status: 200 }
    );
  } catch (error) {
    console.error("group-affiliation api error", error);
    return NextResponse.json(
      { error: "failed to load group affiliation" },
      { status: 500 }
    );
  }
}

