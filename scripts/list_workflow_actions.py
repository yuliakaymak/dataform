import os

from lib import (
    BuildReport,
    Config,
    ConsoleFormatter,
    DataformClient,
)


def main():

    config = Config.from_file()

    client = DataformClient(
        project_id=config.project_id,
        region=config.dataform_region,
        repository_id=config.dataform_repository,
    )

    actions = client.list_workflow_actions(
        workflow_invocation_name=os.environ["WORKFLOW_INVOCATION"],
    )

    report = BuildReport(
        actions=actions,
    )

    ConsoleFormatter().print_build_report(
        report,
    )


if __name__ == "__main__":
    main()