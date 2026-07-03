import os
import time

from dataclasses import replace

from lib import (
    BuildReport,
    Config,
    ConsoleFormatter,
    DataformClient,
)


TERMINAL_STATES = {
    "SUCCEEDED",
    "FAILED",
    "CANCELLED",
}

FINAL_ACTION_STATES = {
    "SUCCEEDED",
    "FAILED",
}

SKIPPED_ACTION_STATES = {
    "CANCELLED",
    "DISABLED",
    "SKIPPED",
}


def _action_key(action):
    return (
        action.target.database,
        action.target.schema,
        action.target.name,
        action.action_type,
    )


def _sort_by_start_time(actions):
    return sorted(
        actions,
        key=lambda action: (
            action.start_time is None,
            action.start_time,
            action.target.schema,
            action.target.name,
        ),
    )


def _is_started_or_finished(action):
    if not (
        action.is_model
        or action.is_assertion
    ):
        return False

    return (
        action.start_time is not None
        or action.state in SKIPPED_ACTION_STATES
        or action.state in FINAL_ACTION_STATES
    )


def _running_action(action):
    return replace(
        action,
        state="RUNNING",
        end_time=None,
        duration_seconds=None,
    )


def _print_action(
    formatter,
    action,
) -> None:

    if action.is_model:
        formatter.print_model(action)
        return

    formatter.print_assertion(action)


def main():

    config = Config.from_file()
    workflow_invocation = os.environ["WORKFLOW_INVOCATION"]
    poll_interval = int(os.getenv("POLL_INTERVAL", "5"))
    timeout = int(os.getenv("WORKFLOW_TIMEOUT", "600"))

    client = DataformClient(
        project_id=config.project_id,
        region=config.dataform_region,
        repository_id=config.dataform_repository,
    )

    formatter = ConsoleFormatter()
    completed_actions = set()
    active_action_key = None
    active_action_started = False
    start_time = time.time()
    actions = []
    previous_state = None
    assertions_header_printed = False

    formatter.print_header()
    formatter.print_section_header("Models")

    while True:

        invocation = client.get_workflow_invocation(
            workflow_invocation_name=workflow_invocation,
        )

        if invocation.state != previous_state:
            print(f"Workflow state: {invocation.state}", flush=True)
            previous_state = invocation.state

        actions = client.list_workflow_actions(
            workflow_invocation_name=workflow_invocation,
        )

        while True:
            active_action = None

            if active_action_key is not None:
                active_action = next(
                    (
                        action
                        for action in actions
                        if _action_key(action) == active_action_key
                    ),
                    None,
                )

            if active_action is None:
                candidates = [
                    action
                    for action in actions
                    if (
                        _action_key(action) not in completed_actions
                        and _is_started_or_finished(action)
                    )
                ]

                if not candidates:
                    break

                active_action = _sort_by_start_time(candidates)[0]
                active_action_key = _action_key(active_action)
                active_action_started = False

            if active_action.is_assertion and not assertions_header_printed:
                print()
                formatter.print_section_header("Assertions")
                assertions_header_printed = True

            if active_action.state in SKIPPED_ACTION_STATES:
                _print_action(
                    formatter,
                    active_action,
                )
                completed_actions.add(active_action_key)
                active_action_key = None
                active_action_started = False
                continue

            if not active_action_started:
                _print_action(
                    formatter,
                    _running_action(active_action),
                )
                active_action_started = True

            if active_action.state in FINAL_ACTION_STATES:
                _print_action(
                    formatter,
                    active_action,
                )
                completed_actions.add(active_action_key)
                active_action_key = None
                active_action_started = False
                continue

            break

        if (
            invocation.state in TERMINAL_STATES
            and active_action_key is None
        ):
            break

        if time.time() - start_time > timeout:
            raise TimeoutError(
                "Workflow Invocation timed out."
            )

        time.sleep(poll_interval)

    print()
    print("Workflow finished.")
    print(f"State: {previous_state}")

    report = BuildReport(actions=actions)

    print()

    formatter.print_build_report(
        report,
        include_models=False,
        include_assertions=False,
        include_header=False,
    )


if __name__ == "__main__":
    main()
