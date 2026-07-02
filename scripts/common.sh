#!/usr/bin/env bash

log() {
    echo "[INFO] $*"
}

normalize_line() {
    local line="$1"
    printf '%s' "${line%$'\r'}"
}

require_env() {
    local var_name="$1"

    if [[ -z "${!var_name:-}" ]]; then
        echo "Missing required environment variable: ${var_name}"
        exit 1
    fi
}

get_dataform_version() {
    local version

    version=$(grep "^dataformCoreVersion:" workflow_settings.yaml | awk '{print $2}')

    if [[ -z "$version" ]]; then
        echo "Could not determine Dataform Core version from workflow_settings.yaml."
        exit 1
    fi

    echo "$version"
}

get_pr_number() {
    require_env PR_NUMBER
    echo "$PR_NUMBER"
}

get_ci_dataset_name() {
    require_env PR_NUMBER

    local dataset="$1"

    echo "${dataset}__ci_pr_${PR_NUMBER}"
}

warn() {
    echo "[WARNING] $*"
}

error() {
    echo "[ERROR] $*" >&2
}

get_default_project() {
    grep "^defaultProject:" workflow_settings.yaml | awk '{print $2}'
}

get_bigquery_location() {
    grep "^  location:" ci_settings.yaml | awk '{print $2}'
}

check_bq_cli() {
    command -v bq >/dev/null 2>&1 || {
        error "bq CLI is not installed."
        exit 1
    }
}