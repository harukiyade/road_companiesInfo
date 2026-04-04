"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { getDefaultIndustryApiBase } from "@/libs/industryApiBase";

export type IndustryCascadeValues = {
  industryLarge: string;
  industryMiddle: string;
  industrySmall: string;
  industryDetail: string;
};

export type IndustryCascadeOptions = {
  large: string[];
  middle: string[];
  small: string[];
  detail: string[];
};

export type IndustryCascadeLoading = {
  large: boolean;
  middle: boolean;
  small: boolean;
  detail: boolean;
};

function joinUrl(base: string, path: string, q?: URLSearchParams) {
  const u = `${base.replace(/\/$/, "")}${path}`;
  if (!q || [...q].length === 0) return u;
  return `${u}?${q.toString()}`;
}

async function fetchStringList(url: string): Promise<string[]> {
  const res = await fetch(url);
  let data: unknown;
  try {
    data = await res.json();
  } catch {
    data = null;
  }
  if (!res.ok) {
    const msg =
      typeof data === "object" &&
      data !== null &&
      "error" in data &&
      typeof (data as { error: unknown }).error === "string"
        ? (data as { error: string }).error
        : `request failed: ${res.status}`;
    throw new Error(msg);
  }
  if (!Array.isArray(data) || !data.every((x) => typeof x === "string")) {
    throw new Error("invalid response: expected string[]");
  }
  return data;
}

export function useIndustryCascade(apiBase?: string) {
  const resolvedBase = apiBase ?? getDefaultIndustryApiBase();
  const base = useMemo(() => resolvedBase.replace(/\/$/, ""), [resolvedBase]);

  const [values, setValues] = useState<IndustryCascadeValues>({
    industryLarge: "",
    industryMiddle: "",
    industrySmall: "",
    industryDetail: "",
  });

  const [options, setOptions] = useState<IndustryCascadeOptions>({
    large: [],
    middle: [],
    small: [],
    detail: [],
  });

  const [loading, setLoading] = useState<IndustryCascadeLoading>({
    large: true,
    middle: false,
    small: false,
    detail: false,
  });

  const [error, setError] = useState<string | null>(null);

  const valuesRef = useRef(values);
  valuesRef.current = values;

  useEffect(() => {
    let cancelled = false;
    setLoading((s) => ({ ...s, large: true }));
    setError(null);
    fetchStringList(joinUrl(base, "/large"))
      .then((list) => {
        if (!cancelled) {
          setOptions((o) => ({ ...o, large: list }));
        }
      })
      .catch((e: unknown) => {
        if (!cancelled) {
          setError(e instanceof Error ? e.message : String(e));
        }
      })
      .finally(() => {
        if (!cancelled) {
          setLoading((s) => ({ ...s, large: false }));
        }
      });
    return () => {
      cancelled = true;
    };
  }, [base]);

  const onLargeChange = useCallback(
    (industryLarge: string) => {
      setValues({
        industryLarge,
        industryMiddle: "",
        industrySmall: "",
        industryDetail: "",
      });
      setOptions((o) => ({
        ...o,
        middle: [],
        small: [],
        detail: [],
      }));
      if (!industryLarge) return;

      setLoading((s) => ({ ...s, middle: true }));
      setError(null);
      const q = new URLSearchParams({ large: industryLarge });
      fetchStringList(joinUrl(base, "/middle", q))
        .then((list) => {
          setOptions((o) => ({
            ...o,
            middle: list,
            small: [],
            detail: [],
          }));
        })
        .catch((e: unknown) => {
          setError(e instanceof Error ? e.message : String(e));
        })
        .finally(() => {
          setLoading((s) => ({ ...s, middle: false }));
        });
    },
    [base]
  );

  const onMiddleChange = useCallback(
    (industryMiddle: string) => {
      setValues((v) => ({
        ...v,
        industryMiddle,
        industrySmall: "",
        industryDetail: "",
      }));
      setOptions((o) => ({
        ...o,
        small: [],
        detail: [],
      }));
      const { industryLarge } = valuesRef.current;
      if (!industryLarge || !industryMiddle) return;

      setLoading((s) => ({ ...s, small: true }));
      setError(null);
      const q = new URLSearchParams({
        large: industryLarge,
        middle: industryMiddle,
      });
      fetchStringList(joinUrl(base, "/small", q))
        .then((list) => {
          setOptions((o) => ({
            ...o,
            small: list,
            detail: [],
          }));
        })
        .catch((e: unknown) => {
          setError(e instanceof Error ? e.message : String(e));
        })
        .finally(() => {
          setLoading((s) => ({ ...s, small: false }));
        });
    },
    [base]
  );

  const onSmallChange = useCallback(
    (industrySmall: string) => {
      setValues((v) => ({
        ...v,
        industrySmall,
        industryDetail: "",
      }));
      setOptions((o) => ({ ...o, detail: [] }));
      const { industryLarge, industryMiddle } = valuesRef.current;
      if (!industryLarge || !industryMiddle || !industrySmall) return;

      setLoading((s) => ({ ...s, detail: true }));
      setError(null);
      const q = new URLSearchParams({
        large: industryLarge,
        middle: industryMiddle,
        small: industrySmall,
      });
      fetchStringList(joinUrl(base, "/detail", q))
        .then((list) => {
          setOptions((o) => ({ ...o, detail: list }));
        })
        .catch((e: unknown) => {
          setError(e instanceof Error ? e.message : String(e));
        })
        .finally(() => {
          setLoading((s) => ({ ...s, detail: false }));
        });
    },
    [base]
  );

  const onDetailChange = useCallback((industryDetail: string) => {
    setValues((v) => ({ ...v, industryDetail }));
  }, []);

  const toSearchPayload = useCallback((): Record<string, string> => {
    const p: Record<string, string> = {};
    if (values.industryLarge) p.industryLarge = values.industryLarge;
    if (values.industryMiddle) p.industryMiddle = values.industryMiddle;
    if (values.industrySmall) p.industrySmall = values.industrySmall;
    if (values.industryDetail) p.industryDetail = values.industryDetail;
    return p;
  }, [values]);

  return {
    values,
    options,
    loading,
    error,
    onLargeChange,
    onMiddleChange,
    onSmallChange,
    onDetailChange,
    toSearchPayload,
  };
}
