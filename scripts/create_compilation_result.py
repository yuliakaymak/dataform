import os

from lib import Config
from lib import DataformClient


def main():

    config = Config.from_file()

    git_commitish = os.environ["GIT_COMMITISH"]
    schema_suffix = os.environ["SCHEMA_SUFFIX"]

    print("=== Configuration ===")
    print(f"Project      : {config.project_id}")
    print(f"Region       : {config.dataform_region}")
    print(f"Repository   : {config.dataform_repository}")
    print(f"Commit       : {git_commitish}")
    print(f"SchemaSuffix : {schema_suffix}")
    print()

    client = DataformClient(
        project_id=config.project_id,
        region=config.dataform_region,
        repository_id=config.dataform_repository,
    )

    print("Repository path:")
    print(client.repository_path)
    print()

    result = client.create_compilation_result(
        git_commitish=git_commitish,
        schema_suffix=schema_suffix,
    )

    print()
    print("Compilation Result created successfully.")
    print(result.name)

    github_output = os.getenv("GITHUB_OUTPUT")

    if github_output:
        with open(github_output, "a", encoding="utf-8") as file:
            file.write(
                f"compilation_result={result.name}\n"
            )


if __name__ == "__main__":
    main()