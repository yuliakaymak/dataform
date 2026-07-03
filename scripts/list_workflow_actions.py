import os
import time

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
    printed_models = set()
    start_time = time.time()
    actions = []
    previous_state = None

    print()
    print("=" * formatter.SECTION_WIDTH)
    print("Models")
    print("=" * formatter.SECTION_WIDTH)

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

        started_models = [
            action
            for action in actions
            if action.is_model and action.start_time is not None
        ]

        for action in _sort_by_start_time(started_models):
            action_key = _action_key(action)

            if action_key in printed_models:
                continue

            formatter.print_model(action)
            printed_models.add(action_key)

        if invocation.state in TERMINAL_STATES:
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

    formatter.print_build_report(
        report,
        include_models=False,
    )


if __name__ == "__main__":
    main()
