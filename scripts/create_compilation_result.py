import os

from lib import Config
from lib import DataformClient


def main():

    config = Config.from_file()

    git_commitish = os.environ["GIT_COMMITISH"]
    schema_suffix = os.environ["SCHEMA_SUFFIX"]

    client = DataformClient(
        project_id=config.project_id,
        region=config.dataform_region,
        repository_id=config.dataform_repository,
    )

    result = client.create_compilation_result(
        git_commitish=git_commitish,
        schema_suffix=schema_suffix,
    )

    print()
    print("Compilation Result created successfully.")
    print(result.name)


if __name__ == "__main__":
    main()