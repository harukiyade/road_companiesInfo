"use client";

import { FormEvent } from "react";
import { getDefaultIndustryApiBase } from "@/libs/industryApiBase";
import { useIndustryCascade } from "./useIndustryCascade";

export type SearchFormProps = {
  /** 未指定時は NEXT_PUBLIC_INDUSTRY_API_BASE または http://localhost:8000/api/industries（FastAPI） */
  apiBase?: string;
  /** 業種ほか検索パラメータを親へ渡す（キーは API 側の camelCase に合わせる） */
  onSearch?: (params: Record<string, string>) => void;
};

/**
 * 業種プルダウンは FastAPI（/api/industries/*）の DB 実データのみを表示する。
 */
export function SearchForm({
  apiBase = getDefaultIndustryApiBase(),
  onSearch,
}: SearchFormProps) {
  const {
    values,
    options,
    loading,
    error,
    onLargeChange,
    onMiddleChange,
    onSmallChange,
    onDetailChange,
    toSearchPayload,
  } = useIndustryCascade(apiBase);

  const handleSubmit = (e: FormEvent) => {
    e.preventDefault();
    onSearch?.(toSearchPayload());
  };

  return (
    <form onSubmit={handleSubmit} style={{ display: "flex", flexDirection: "column", gap: 12 }}>
      <fieldset style={{ border: "none", padding: 0, margin: 0 }}>
        <legend style={{ fontWeight: 600, marginBottom: 8 }}>業種</legend>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 12 }}>
          <label style={{ display: "flex", flexDirection: "column", gap: 4, minWidth: 160 }}>
            <span>大分類</span>
            <select
              value={values.industryLarge}
              onChange={(e) => onLargeChange(e.target.value)}
              disabled={loading.large}
            >
              <option value="">選択してください</option>
              {options.large.map((v) => (
                <option key={v} value={v}>
                  {v}
                </option>
              ))}
            </select>
          </label>

          <label style={{ display: "flex", flexDirection: "column", gap: 4, minWidth: 160 }}>
            <span>中分類</span>
            <select
              value={values.industryMiddle}
              onChange={(e) => onMiddleChange(e.target.value)}
              disabled={!values.industryLarge || loading.middle}
            >
              <option value="">選択してください</option>
              {options.middle.map((v) => (
                <option key={v} value={v}>
                  {v}
                </option>
              ))}
            </select>
          </label>

          <label style={{ display: "flex", flexDirection: "column", gap: 4, minWidth: 160 }}>
            <span>小分類</span>
            <select
              value={values.industrySmall}
              onChange={(e) => onSmallChange(e.target.value)}
              disabled={!values.industryMiddle || loading.small}
            >
              <option value="">選択してください</option>
              {options.small.map((v) => (
                <option key={v} value={v}>
                  {v}
                </option>
              ))}
            </select>
          </label>

          <label style={{ display: "flex", flexDirection: "column", gap: 4, minWidth: 160 }}>
            <span>細分類</span>
            <select
              value={values.industryDetail}
              onChange={(e) => onDetailChange(e.target.value)}
              disabled={!values.industrySmall || loading.detail}
            >
              <option value="">選択してください</option>
              {options.detail.map((v) => (
                <option key={v} value={v}>
                  {v}
                </option>
              ))}
            </select>
          </label>
        </div>
      </fieldset>

      {error ? (
        <p role="alert" style={{ color: "crimson", margin: 0 }}>
          {error}
        </p>
      ) : null}

      {onSearch ? (
        <button type="submit" disabled={loading.large}>
          この条件で検索
        </button>
      ) : null}
    </form>
  );
}

export default SearchForm;
