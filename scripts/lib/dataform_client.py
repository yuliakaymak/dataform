import time
import json

from datetime import datetime

from google.cloud import dataform_v1beta1

from .models import (
    CompilationResult,
    Target,
    WorkflowAction,
    WorkflowInvocation,
)


class DataformClient:

    def __init__(
        self,
        project_id: str,
        region: str,
        repository_id: str,
    ) -> None:

        self.project_id = project_id
        self.region = region
        self.repository_id = repository_id

        self.client = dataform_v1beta1.DataformClient()

        self.repository_path = self.client.repository_path(
            project_id,
            region,
            repository_id,
        )

    def create_compilation_result(
        self,
        git_commitish: str,
        schema_suffix: str,
    ) -> CompilationResult:
        """
        Creates a Compilation Result in Dataform.
        """

        compilation_result = dataform_v1beta1.CompilationResult(
            git_commitish=git_commitish,
            code_compilation_config=dataform_v1beta1.CodeCompilationConfig(
                schema_suffix=schema_suffix,
            ),
        )

        response = self.client.create_compilation_result(
            parent=self.repository_path,
            compilation_result=compilation_result,
        )

        return CompilationResult(
            name=response.name,
            git_commitish=git_commitish,
            resolved_git_commit_sha=getattr(
                response,
                "resolved_git_commit_sha",
                None,
            ),
            create_time=getattr(
                response,
                "create_time",
                None,
            ),
        )
    

    def create_workflow_invocation(
        self,
        compilation_result_name: str,
    ) -> WorkflowInvocation:
        """
        Creates a Workflow Invocation.
        """

        workflow_invocation = dataform_v1beta1.WorkflowInvocation(
            compilation_result=compilation_result_name,
            invocation_config=dataform_v1beta1.InvocationConfig(
                service_account=(
                    "github-dataform-ci@my-project-19032025.iam.gserviceaccount.com"
                ),
            ),
        )

        response = self.client.create_workflow_invocation(
            parent=self.repository_path,
            workflow_invocation=workflow_invocation,
        )

        return WorkflowInvocation(
            name=response.name,
            state=response.state.name,
            compilation_result=compilation_result_name,
            invocation_time=getattr(
                response,
                "invocation_time",
                None,
            ),
        )
    
    def get_workflow_invocation(
        self,
        workflow_invocation_name: str,
    ) -> WorkflowInvocation:
        """
        Returns the latest Workflow Invocation state.
        """

        response = self.client.get_workflow_invocation(
            name=workflow_invocation_name,
        )

        return WorkflowInvocation(
            name=response.name,
            state=response.state.name,
            compilation_result=response.compilation_result,
            invocation_time=getattr(
                response,
                "invocation_time",
                None,
            ),
        )
    
    def wait_for_workflow(
        self,
        workflow_invocation_name: str,
        poll_interval: int = 5,
        timeout: int = 600,
    ) -> WorkflowInvocation:
        """
        Waits until the Workflow Invocation finishes.
        """

        start_time = time.time()

        while True:

            invocation = self.get_workflow_invocation(
                workflow_invocation_name,
            )

            print(f"Current state: {invocation.state}")

            if invocation.state in (
                "SUCCEEDED",
                "FAILED",
                "CANCELLED",
            ):
                return invocation

            if time.time() - start_time > timeout:
                raise TimeoutError(
                    "Workflow Invocation timed out."
                )

            time.sleep(poll_interval)

    def _calculate_duration_seconds(
        self,
        invocation_timing,
    ) -> float | None:

        if (
            not invocation_timing
            or not invocation_timing.start_time
            or not invocation_timing.end_time
        ):
            return None

        start = invocation_timing.start_time.timestamp()
        end = invocation_timing.end_time.timestamp()

        return round(end - start, 2)

    def _to_datetime(self, timestamp) -> datetime | None:
        """
        Converts a protobuf Timestamp into datetime.
        """

        if not timestamp:
            return None

        return timestamp.ToDatetime()
    
    def list_workflow_actions(
        self,
        workflow_invocation_name: str,
    ) -> list[WorkflowAction]:
        """
        Returns all executed Dataform actions.
        """

        actions: list[WorkflowAction] = []

        pager = self.client.query_workflow_invocation_actions(
            request={
                "name": workflow_invocation_name,
            }
        )

        for action in pager:

            metadata = {}

            if action.internal_metadata:
                metadata = json.loads(action.internal_metadata)

            action_type = (
                metadata.get("labels", {})
                .get("dataform-action-type", "unknown")
            )

            target = action.canonical_target or action.target

            actions.append(
                WorkflowAction(
                    target=Target(
                        database=target.database,
                        schema=target.schema,
                        name=target.name,
                    ),
                    state=action.state.name,
                    action_type=action_type,
                    start_time=self._to_datetime(
                        action.invocation_timing.start_time,
                    ),
                    end_time=self._to_datetime(
                        action.invocation_timing.end_time,
                    ),
                    duration_seconds=self._calculate_duration_seconds(
                        action.invocation_timing,
                    ),
                    failure_reason=None,
                )
            )

        return actions