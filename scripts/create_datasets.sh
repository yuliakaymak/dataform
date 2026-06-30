#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

source "${SCRIPT_DIR}/common.sh"

require_env PR_NUMBER

log "Creating datasets for PR #${PR_NUMBER}"

while IFS= read -r schema
do
    schema="$(normalize_line "$schema")"

    ci_dataset="$(get_ci_dataset_name "$schema")"

    printf "%-15s -> %s\n" "$schema" "$ci_dataset"

done < <(
    python "${SCRIPT_DIR}/lib/graph_parser.py" --schemas
)

log "Dataset discovery completed."