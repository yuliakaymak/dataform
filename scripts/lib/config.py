from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


DEFAULT_CONFIG_PATH = Path("workflow_settings.yaml")


@dataclass(frozen=True)
class Config:
    """
    Represents the Dataform CI configuration loaded from
    workflow_settings.yaml.
    """

    project_id: str
    default_dataset: str
    default_assertion_dataset: str

    dataform_region: str
    dataform_repository: str

    dataform_core_version: str

    @classmethod
    def from_file(
        cls,
        config_path: Path = DEFAULT_CONFIG_PATH,
    ) -> "Config":

        if not config_path.exists():
            raise FileNotFoundError(
                f"Configuration file not found: {config_path}"
            )

        with config_path.open(
            "r",
            encoding="utf-8",
        ) as file:
            config = yaml.safe_load(file)

        return cls(
            project_id=config["defaultProject"],
            default_dataset=config["defaultDataset"],
            default_assertion_dataset=config["defaultAssertionDataset"],
            dataform_region=config["dataform"]["region"],
            dataform_repository=config["dataform"]["repository"],
            dataform_core_version=str(config["dataformCoreVersion"]),
        )