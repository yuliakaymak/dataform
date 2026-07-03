import os

from lib import Config
from lib import DataformClient


def main() -> None:

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

    github_output = os.getenv("GITHUB_OUTPUT")

    if github_output:
        with open(
            github_output,
            "a",
            encoding="utf-8",
        ) as file:
            file.write(
                f"workflow_invocation={result.name}\n"
            )


if __name__ == "__main__":
    main()