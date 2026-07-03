import os

from lib import (
    BuildReport,
    Config,
    ConsoleFormatter,
    DataformClient,
)


EXPECTED_ASSERTION_FAILURE = (
    "Assertion failed, expected zero rows"
)


def _is_expected_assertion_failure(action) -> bool:
    reason = action.failure_reason or ""

    return EXPECTED_ASSERTION_FAILURE in reason


def main() -> None:

    config = Config.from_file()
    workflow_invocation = os.environ["WORKFLOW_INVOCATION"]

    client = DataformClient(
        project_id=config.project_id,
        region=config.dataform_region,
        repository_id=config.dataform_repository,
    )

    actions = client.list_workflow_actions(
        workflow_invocation_name=workflow_invocation,
    )

    report = BuildReport(actions=actions)
    formatter = ConsoleFormatter()

    model_failures = [
        action
        for action in report.failed_actions
        if action.is_model
    ]

    assertion_failures = [
        action
        for action in report.failed_actions
        if action.is_assertion
    ]

    blocking_assertion_failures = [
        action
        for action in assertion_failures
        if not _is_expected_assertion_failure(action)
    ]

    warning_assertion_failures = [
        action
        for action in assertion_failures
        if _is_expected_assertion_failure(action)
    ]

    blocking_failures = (
        model_failures
        + blocking_assertion_failures
    )

    if blocking_failures:
        formatter.print_errors(
            blocking_failures,
            title="Errors",
            color=formatter.RED,
        )
        raise SystemExit(1)

    if warning_assertion_failures:
        formatter.print_errors(
            warning_assertion_failures,
            title="Warning",
            color=formatter.YELLOW,
        )
        return

    formatter.print_colored_message(
        "The workflow is successful",
        formatter.GREEN,
    )


if __name__ == "__main__":
    main()
