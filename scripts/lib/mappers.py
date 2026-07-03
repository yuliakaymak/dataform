from __future__ import annotations

import json
from datetime import datetime

from .models import Target, WorkflowAction


class WorkflowActionMapper:
    """
    Maps Dataform WorkflowInvocationAction protobuf objects
    into domain models.
    """

    @staticmethod
    def _to_datetime(timestamp) -> datetime | None:

        if timestamp is None:
            return None

        if (
            getattr(timestamp, "seconds", 0) == 0
            and getattr(timestamp, "nanos", 0) == 0
        ):
            return None

        return timestamp.ToDatetime()

    @classmethod
    def _calculate_duration_seconds(
        cls,
        invocation_timing,
    ) -> float | None:

        if (
            not invocation_timing
            or not invocation_timing.start_time
            or not invocation_timing.end_time
        ):
            return None

        start = cls._to_datetime(
            invocation_timing.start_time
        )

        end = cls._to_datetime(
            invocation_timing.end_time
        )

        if start is None or end is None:
            return None

        return round(
            (end - start).total_seconds(),
            2,
        )

    @staticmethod
    def _extract_failure_reason(
        action,
        metadata: dict,
    ) -> str | None:

        for field_name in (
            "failure_reason",
            "failureReason",
        ):
            value = getattr(
                action,
                field_name,
                None,
            )

            if value:
                return str(value)

        for field_name in (
            "failureReason",
            "failure_reason",
            "errorMessage",
            "error_message",
            "error",
        ):
            value = metadata.get(field_name)

            if value:
                return str(value)

        return None

    @classmethod
    def from_api(cls, action) -> WorkflowAction:

        metadata = {}

        if action.internal_metadata:
            metadata = json.loads(
                action.internal_metadata
            )

        action_type = (
            metadata.get("labels", {})
            .get(
                "dataform-action-type",
                "unknown",
            )
        )

        target = (
            action.canonical_target
            or action.target
        )

        return WorkflowAction(
            target=Target(
                database=target.database,
                schema=target.schema,
                name=target.name,
            ),
            state=action.state.name,
            action_type=action_type,
            start_time=cls._to_datetime(
                action.invocation_timing.start_time
            ),
            end_time=cls._to_datetime(
                action.invocation_timing.end_time
            ),
            duration_seconds=cls._calculate_duration_seconds(
                action.invocation_timing
            ),
            failure_reason=cls._extract_failure_reason(
                action,
                metadata,
            ),
        )
