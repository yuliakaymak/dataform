#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "${SCRIPT_DIR}/common.sh"

require_env PR_NUMBER
check_bq_cli

PROJECT_ID="$(get_default_project)"

log "Creating CI datasets for PR #${PR_NUMBER}"
log "Google Cloud project: ${PROJECT_ID}"

created=0
existing=0

while IFS= read -r schema
do
    schema="$(normalize_line "$schema")"

    ci_dataset="$(get_ci_dataset_name "$schema")"

    log "Processing schema '${schema}'..."

    if bq show --dataset "${PROJECT_ID}:${ci_dataset}" >/dev/null 2>&1; then
        log "Dataset '${ci_dataset}' already exists."
        ((++existing))
        continue
    fi

    log "Creating dataset '${ci_dataset}'..."

    if ! bq mk \
        --dataset \
        "${PROJECT_ID}:${ci_dataset}" >/dev/null
    then
        error "Failed to create dataset '${ci_dataset}'."
        exit 1
    fi

    log "Dataset '${ci_dataset}' created."

    ((++created))

done < <(
    python "${SCRIPT_DIR}/lib/graph_parser.py" --schemas
)

echo
log "Dataset creation completed."
log "Created : ${created}"
log "Existing: ${existing}"