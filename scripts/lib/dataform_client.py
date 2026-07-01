from google.cloud import dataform_v1beta1

from .models import (
    CompilationResult,
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