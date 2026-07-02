import os

from lib import Config
from lib import DataformClient


def main():

    config = Config.from_file()

    compilation_result = os.environ["COMPILATION_RESULT"]

    client = DataformClient(
        project_id=config.project_id,
        region=config.dataform_region,
        repository_id=config.dataform_repository,
    )

    result = client.create_workflow_invocation(
        compilation_result_name=compilation_result,
    )

    print()
    print("Workflow Invocation created successfully.")
    print(result.name)

    print()
    print("Waiting for completion...")

    result = client.wait_for_workflow(
        result.name,
    )

    print()
    print("Workflow finished.")
    print(result.state)

    if result.state != "SUCCEEDED":
        raise RuntimeError(
            f"Workflow failed with state {result.state}"
        )

    github_output = os.getenv("GITHUB_OUTPUT")

    if github_output:
        with open(github_output, "a", encoding="utf-8") as file:
            file.write(
                f"workflow_invocation={result.name}\n"
            )

if __name__ == "__main__":
    main()