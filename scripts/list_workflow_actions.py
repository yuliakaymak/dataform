import os

from lib import Config
from lib import DataformClient


def main():

    config = Config.from_file()

    workflow = os.environ["WORKFLOW_INVOCATION"]

    client = DataformClient(
        project_id=config.project_id,
        region=config.dataform_region,
        repository_id=config.dataform_repository,
    )

    actions = client.list_workflow_actions(
        workflow,
    )

    actions.sort(
        key=lambda action: (
            action.start_time is None,
            action.start_time,
        )
    )
    
    print()
    print(f"Retrieved {len(actions)} actions")
    print()

    for action in actions:
        start = (
            action.start_time.strftime("%H:%M:%S")
            if action.start_time
            else "--:--:--"
        )

        duration = (
            f"{action.duration_seconds:.2f}s"
            if action.duration_seconds is not None
            else "-"
        )

        status = {
            "SUCCEEDED": "✓",
            "FAILED": "✗",
            "RUNNING": "…",
        }.get(action.state, "?")

        print(
            f"{start}  "
            f"{status:<2}"
            f"{action.action_type:<10}"
            f"{action.target.schema}.{action.target.name:<45}"
            f"{duration:>8}"
        )

if __name__ == "__main__":
    main()