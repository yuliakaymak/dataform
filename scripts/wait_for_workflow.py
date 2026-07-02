import os

from lib import Config
from lib import DataformClient


def main() -> None:

    config = Config.from_file()

    workflow_invocation = os.environ["WORKFLOW_INVOCATION"]

    client = DataformClient(
        project_id=config.project_id,
        region=config.dataform_region,
        repository_id=config.dataform_repository,
    )

    print()
    print("Waiting for workflow completion...")

    result = client.wait_for_workflow(
        workflow_invocation_name=workflow_invocation,
    )

    print()
    print("Workflow finished.")
    print(f"State: {result.state}")

    github_output = os.getenv("GITHUB_OUTPUT")

    if github_output:
        with open(
            github_output,
            "a",
            encoding="utf-8",
        ) as file:
            file.write(
                f"workflow_state={result.state}\n"
            )


if __name__ == "__main__":
    main()