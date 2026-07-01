import os

from google.api_core.exceptions import NotFound
from lib import Config
from lib import DataformClient


def main():

    config = Config.from_file()

    client = DataformClient(
        project_id=config.project_id,
        region=config.dataform_region,
        repository_id=config.dataform_repository,
    )

    print("Repository path:")
    print(client.repository_path)
    print()

    print("Checking repository...")

    try:
        repository = client.client.get_repository(
            name=client.repository_path
        )

        print("Repository found!")
        print(repository.name)

    except NotFound as e:
        print("Repository NOT FOUND.")
        print(e)
        raise


if __name__ == "__main__":
    main()