from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from .models import WorkflowAction


@dataclass(frozen=True)
class BuildReport:
    """
    Represents a completed Dataform build.
    """

    actions: list[WorkflowAction]

    @property
    def models(self) -> list[WorkflowAction]:
        return [
            action
            for action in self.actions
            if action.is_model
        ]

    @property
    def assertions(self) -> list[WorkflowAction]:
        return [
            action
            for action in self.actions
            if action.is_assertion
        ]

    @staticmethod
    def _count_succeeded(
        actions: list[WorkflowAction],
    ) -> int:
        return sum(
            action.state == "SUCCEEDED"
            for action in actions
        )

    @staticmethod
    def _count_failed(
        actions: list[WorkflowAction],
    ) -> int:
        return sum(
            action.state == "FAILED"
            for action in actions
        )

    @property
    def model_count(self) -> int:
        return len(self.models)

    @property
    def assertion_count(self) -> int:
        return len(self.assertions)

    @property
    def succeeded_models(self) -> int:
        return self._count_succeeded(self.models)

    @property
    def failed_models(self) -> int:
        return self._count_failed(self.models)

    @property
    def succeeded_assertions(self) -> int:
        return self._count_succeeded(self.assertions)

    @property
    def failed_assertions(self) -> int:
        return self._count_failed(self.assertions)

    @property
    def elapsed_time(self) -> float:

        actions = self.models + self.assertions

        started = [
            action.start_time
            for action in actions
            if action.start_time is not None
        ]

        finished = [
            action.end_time
            for action in actions
            if action.end_time is not None
        ]

        if not started or not finished:
            return 0.0

        return round(
            (
                max(finished)
                - min(started)
            ).total_seconds(),
            2,
        )
    @property
    def failed_actions(self) -> list[WorkflowAction]:
        """
        Returns all failed workflow actions.
        """

        return [
            action
            for action in self.actions
            if action.state == "FAILED"
        ]


    @property
    def has_failures(self) -> bool:
        return bool(self.failed_actions)
    
    @property
    def skipped_models(self) -> int:
        return sum(
            action.state == "SKIPPED"
            for action in self.models
        )


    @property
    def skipped_assertions(self) -> int:
        return sum(
            action.state == "SKIPPED"
            for action in self.assertions
        )
