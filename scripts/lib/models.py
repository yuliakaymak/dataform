from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class Target:
    database: str
    schema: str
    name: str


@dataclass(frozen=True)
class CompilationResult:
    """
    Represents a Dataform Compilation Result.
    """

    name: str
    git_commitish: str
    resolved_git_commit_sha: str | None = None
    create_time: datetime | None = None


@dataclass(frozen=True)
class WorkflowInvocation:
    """
    Represents a Dataform Workflow Invocation.
    """

    name: str
    state: str
    compilation_result: str
    invocation_time: Optional[datetime] = None


@dataclass(frozen=True)
class WorkflowAction:
    """
    Represents one executed Dataform action.
    """

    target: Target
    state: str
    action_type: str

    start_time: datetime | None = None
    end_time: datetime | None = None

    duration_seconds: float | None = None
    failure_reason: str | None = None

@dataclass(frozen=True)
class AssertionResult:
    name: str
    status: str
    row_count: int

@property
def is_model(self) -> bool:
    return self.action_type in {
        "view",
        "table",
        "incremental",
    }


@property
def is_assertion(self) -> bool:
    return self.action_type == "assertion"