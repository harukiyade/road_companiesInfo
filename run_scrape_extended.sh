#!/bin/bash
cd "/Users/harukishiroyama/Library/Mobile Documents/com~apple~CloudDocs/info_companyDetail"
export FIREBASE_SERVICE_ACCOUNT_KEY='/Users/harukishiroyama/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json'
export FAST_MODE=true
export REVERSE_ORDER=true
export PARALLEL_WORKERS=6
export SLEEP_MS=250
export PAGE_WAIT_MODE=domcontentloaded
export PAGE_TIMEOUT=12000
export NAVIGATION_TIMEOUT=15000
export SKIP_ON_ERROR=true
npx ts-node scripts/scrape_extended_fields.ts
