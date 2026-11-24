#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
fi

TABLES="admin_users,warming_scripts,platform_settings,rfb_cnaes,rfb_municipios,rfb_naturezas_juridicas,organizations,users,cron_organization_states,cron_channel_instances,cron_channel_bindings,affiliate_connections,affiliate_coupons,b2b_prospect_lists,b2b_saved_lead_lists,b2b_saved_leads"

python conduit.py transfer \
  --source local \
  --target production \
  --tables "$TABLES" \
  --upsert \
  --chunk-size 2000 "$@"
