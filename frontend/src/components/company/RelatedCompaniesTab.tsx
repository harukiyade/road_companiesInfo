"use client";

import { useEffect, useMemo, useState } from "react";

type RelatedCompanyItem = {
  childCompanyId: string | null;
  childCompanyName: string;
  relationType: string;
  votingRightsRatio: number | null;
  sourceDocId: string;
  companyLink: string | null;
};

type Props = {
  companyId: string;
  initialSort?: "strength" | "ratio";
};

export default function RelatedCompaniesTab({ companyId, initialSort = "strength" }: Props) {
  const [sort, setSort] = useState<"strength" | "ratio">(initialSort);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [items, setItems] = useState<RelatedCompanyItem[]>([]);

  useEffect(() => {
    const run = async () => {
      setLoading(true);
      setError(null);
      try {
        const res = await fetch(`/api/companies/${companyId}/related-companies?sort=${sort}`, {
          cache: "no-store",
        });
        if (!res.ok) {
          throw new Error("API request failed");
        }
        const data = await res.json();
        setItems((data.items ?? []) as RelatedCompanyItem[]);
      } catch (e) {
        setError(e instanceof Error ? e.message : "failed");
      } finally {
        setLoading(false);
      }
    };
    void run();
  }, [companyId, sort]);

  const rows = useMemo(() => items, [items]);

  return (
    <section>
      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 12 }}>
        <h3 style={{ margin: 0 }}>関連企業</h3>
        <select value={sort} onChange={(e) => setSort(e.target.value as "strength" | "ratio")}>
          <option value="strength">関係性が強い順</option>
          <option value="ratio">議決権比率が高い順</option>
        </select>
      </div>

      {loading && <p>読み込み中...</p>}
      {error && <p style={{ color: "red" }}>取得エラー: {error}</p>}
      {!loading && !error && rows.length === 0 && <p>関連企業データはありません。</p>}

      {!loading && !error && rows.length > 0 && (
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr>
              <th style={{ textAlign: "left", borderBottom: "1px solid #ddd" }}>子会社/関連会社名</th>
              <th style={{ textAlign: "left", borderBottom: "1px solid #ddd" }}>関係区分</th>
              <th style={{ textAlign: "right", borderBottom: "1px solid #ddd" }}>議決権所有比率</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row, idx) => (
              <tr key={`${row.childCompanyName}-${row.sourceDocId}-${idx}`}>
                <td style={{ padding: "8px 0" }}>
                  {row.companyLink ? (
                    <a href={row.companyLink}>{row.childCompanyName}</a>
                  ) : (
                    <span>{row.childCompanyName}</span>
                  )}
                </td>
                <td>{row.relationType || "-"}</td>
                <td style={{ textAlign: "right" }}>
                  {row.votingRightsRatio == null ? "-" : `${row.votingRightsRatio}%`}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}

