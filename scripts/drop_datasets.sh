#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "${SCRIPT_DIR}/common.sh"

require_env PR_NUMBER
check_bq_cli

PROJECT_ID="$(get_default_project)"

log "Dropping CI datasets for PR #${PR_NUMBER}"
log "Google Cloud project : ${PROJECT_ID}"

dropped=0
missing=0

while IFS= read -r schema
do
    schema="$(normalize_line "$schema")"

    ci_dataset="$(get_ci_dataset_name "$schema")"

    log "Processing dataset '${ci_dataset}'..."

    if ! bq show --dataset "${PROJECT_ID}:${ci_dataset}" >/dev/null 2>&1; then
        warn "Dataset '${ci_dataset}' does not exist."
        ((++missing))
        continue
    fi

    log "Dropping dataset '${ci_dataset}'..."

    if ! output=$(
        bq rm \
            --dataset \
            --recursive \
            --force \
            "${PROJECT_ID}:${ci_dataset}" \
            2>&1
    )
    then
        error "Failed to drop dataset '${ci_dataset}'."
        echo "${output}"
        exit 1
    fi

    log "Dataset '${ci_dataset}' dropped."

    ((++dropped))

done < <(
    python "${SCRIPT_DIR}/lib/graph_parser.py" --schemas
)

echo
log "Dataset cleanup completed."
log "Dropped: ${dropped}"
log "Missing: ${missing}"
