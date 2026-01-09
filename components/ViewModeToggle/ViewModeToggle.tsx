'use client';

import React from 'react';
import { ToggleButton, ToggleButtonGroup, Tooltip, Box } from '@mui/material';
import { PhoneAndroid, Computer, AutoAwesome } from '@mui/icons-material';
import { ViewMode, useViewMode } from '../../hooks/useViewMode';

export function ViewModeToggle() {
  const { viewMode, changeViewMode } = useViewMode();

  const handleChange = (
    event: React.MouseEvent<HTMLElement>,
    newMode: ViewMode | null,
  ) => {
    if (newMode !== null) {
      changeViewMode(newMode);
    }
  };

  return (
    <Box
      sx={{
        position: 'fixed',
        top: 16,
        right: 16,
        zIndex: 1000,
        backgroundColor: 'background.paper',
        borderRadius: 2,
        boxShadow: 2,
        p: 0.5,
      }}
    >
      <ToggleButtonGroup
        value={viewMode}
        exclusive
        onChange={handleChange}
        size="small"
        aria-label="表示モード切替"
      >
        <Tooltip title="自動（端末幅で判定）">
          <ToggleButton value="auto" aria-label="自動">
            <AutoAwesome fontSize="small" />
          </ToggleButton>
        </Tooltip>
        <Tooltip title="モバイルUI">
          <ToggleButton value="mobile" aria-label="モバイル">
            <PhoneAndroid fontSize="small" />
          </ToggleButton>
        </Tooltip>
        <Tooltip title="PC UI">
          <ToggleButton value="desktop" aria-label="デスクトップ">
            <Computer fontSize="small" />
          </ToggleButton>
        </Tooltip>
      </ToggleButtonGroup>
    </Box>
  );
}

