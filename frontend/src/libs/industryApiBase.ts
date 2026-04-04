/**
 * 業種カスケード API のベース URL（FastAPI 側を正とする）。
 * ブラウザからは NEXT_PUBLIC_* のみ参照可能。
 */
export function getDefaultIndustryApiBase(): string {
  const fromEnv =
    typeof process !== "undefined"
      ? process.env.NEXT_PUBLIC_INDUSTRY_API_BASE?.trim()
      : undefined;
  if (fromEnv) {
    return fromEnv.replace(/\/$/, "");
  }
  return "http://localhost:8000/api/industries";
}
