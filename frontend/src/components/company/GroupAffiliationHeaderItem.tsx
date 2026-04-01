"use client";

import { useEffect, useState } from "react";

type GroupAffiliation = {
  parentCompanyId: string;
  parentCompanyName: string;
  relationType: string;
  votingRightsRatio: number | null;
  parentCompanyLink: string | null;
};

type Props = {
  companyId: string;
};

export default function GroupAffiliationHeaderItem({ companyId }: Props) {
  const [loading, setLoading] = useState(true);
  const [groupAffiliation, setGroupAffiliation] = useState<GroupAffiliation | null>(
    null
  );

  useEffect(() => {
    const run = async () => {
      setLoading(true);
      try {
        const res = await fetch(`/api/companies/${companyId}/group-affiliation`, {
          cache: "no-store",
        });
        if (!res.ok) {
          setGroupAffiliation(null);
          return;
        }
        const data = await res.json();
        setGroupAffiliation((data.groupAffiliation ?? null) as GroupAffiliation | null);
      } finally {
        setLoading(false);
      }
    };
    void run();
  }, [companyId]);

  if (loading) {
    return <div>所属グループ: 読み込み中...</div>;
  }

  if (!groupAffiliation) {
    return <div>所属グループ: -</div>;
  }

  return (
    <div>
      所属グループ:{" "}
      {groupAffiliation.parentCompanyLink ? (
        <a href={groupAffiliation.parentCompanyLink}>
          {groupAffiliation.parentCompanyName}
        </a>
      ) : (
        <span>{groupAffiliation.parentCompanyName}</span>
      )}
      {groupAffiliation.relationType ? `（${groupAffiliation.relationType}）` : ""}
    </div>
  );
}

