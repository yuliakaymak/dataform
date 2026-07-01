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
    name: str
    git_commitish: str
    create_time: Optional[datetime] = None


@dataclass(frozen=True)
class WorkflowInvocation:
    name: str
    state: str
    invocation_time: Optional[datetime] = None


@dataclass(frozen=True)
class AssertionResult:
    name: str
    status: str
    row_count: int