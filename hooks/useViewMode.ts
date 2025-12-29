'use client';

import { useState, useEffect, useMemo } from 'react';
import { useMediaQuery, useTheme } from '@mui/material';

export type ViewMode = 'auto' | 'mobile' | 'desktop';

const STORAGE_KEY = 'company-detail-view-mode';

export function useViewMode() {
  const theme = useTheme();
  const isMobile = useMediaQuery(theme.breakpoints.down('sm'));
  const [viewMode, setViewMode] = useState<ViewMode>('auto');

  // localStorageから初期値を読み込む
  useEffect(() => {
    if (typeof window !== 'undefined') {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved && (saved === 'auto' || saved === 'mobile' || saved === 'desktop')) {
        setViewMode(saved as ViewMode);
      }
    }
  }, []);

  // 実際の表示モードを計算（autoの場合は端末幅で判定）
  const effectiveViewMode = useMemo(() => {
    if (viewMode === 'auto') {
      return isMobile ? 'mobile' : 'desktop';
    }
    return viewMode;
  }, [viewMode, isMobile]);

  // 表示モードを変更し、localStorageに保存
  const changeViewMode = (mode: ViewMode) => {
    setViewMode(mode);
    if (typeof window !== 'undefined') {
      localStorage.setItem(STORAGE_KEY, mode);
    }
  };

  return {
    viewMode,
    effectiveViewMode,
    changeViewMode,
    isMobile,
  };
}

