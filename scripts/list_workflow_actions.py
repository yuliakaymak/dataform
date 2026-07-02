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

    print()
    print(f"Retrieved {len(actions)} actions")
    print()

    for action in actions:
        print(
            action.state,
            action.action_type,
            f"{action.target.schema}.{action.target.name}",
        )


if __name__ == "__main__":
    main()