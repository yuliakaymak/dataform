from __future__ import annotations

from .models import WorkflowAction
from .report import BuildReport


class ConsoleFormatter:
    """
    Formats a Dataform build report for the console.
    """

    HEADER_WIDTH = 62
    SECTION_WIDTH = 60
    NAME_WIDTH = 100
    RED = "\033[31m"
    YELLOW = "\033[33m"
    GREEN = "\033[32m"
    RESET = "\033[0m"

    STATUS = {
        "SUCCEEDED": "[ OK ]",
        "FAILED": "[FAIL]",
        "PENDING": "[RUN ]",
        "RUNNING": "[RUN ]",
        "CANCELLED": "[SKIP]",
        "DISABLED": "[SKIP]",
        "SKIPPED": "[SKIP]",
    }

    def print_build_report(
        self,
        report: BuildReport,
        include_models: bool = True,
        include_assertions: bool = True,
        include_header: bool = True,
    ) -> None:

        if include_header:
            self.print_header()

        if include_models:
            self._print_models(report)

        if include_assertions:
            self._print_assertions(report)

        self._print_summary(report)

        if report.has_failures:
            self.print_errors(report.failed_actions)

    def print_header(self) -> None:
        print("─" * self.HEADER_WIDTH)
        print("Dataform Build")
        print("─" * self.HEADER_WIDTH)
        print()

    def print_section_header(
        self,
        title: str,
    ) -> None:
        print("=" * self.SECTION_WIDTH)
        print(title)
        print("=" * self.SECTION_WIDTH)

    def _print_models(
        self,
        report: BuildReport,
    ) -> None:
        """
        Prints executed models.
        """

        self.print_section_header("Models")

        models = sorted(
            report.models,
            key=lambda action: (
                action.start_time is None,
                action.start_time,
            ),
        )

        for action in models:

            display_name = (
                f"{action.target.schema}."
                f"{action.target.name}"
                f" ({action.action_type})"
            )

            self._print_action(
                action=action,
                display_name=display_name,
            )

        print()

    def print_model(
        self,
        action: WorkflowAction,
    ) -> None:
        """
        Prints one executed model.
        """

        display_name = (
            f"{action.target.schema}."
            f"{action.target.name}"
            f" ({action.action_type})"
        )

        self._print_action(
            action=action,
            display_name=display_name,
        )

    def print_assertion(
        self,
        action: WorkflowAction,
    ) -> None:
        """
        Prints one executed assertion.
        """

        display_name = (
            f"{action.target.schema}."
            f"{action.target.name}"
        )

        self._print_action(
            action=action,
            display_name=display_name,
        )

    def _print_assertions(
        self,
        report: BuildReport,
    ) -> None:
        """
        Prints executed assertions.
        """

        self.print_section_header("Assertions")

        assertions = sorted(
            report.assertions,
            key=lambda action: (
                action.start_time is None,
                action.start_time,
            ),
        )

        for action in assertions:

            display_name = (
                f"{action.target.schema}."
                f"{action.target.name}"
            )

            self._print_action(
                action=action,
                display_name=display_name,
            )

        print()

    def _print_summary(
        self,
        report: BuildReport,
    ) -> None:
        """
        Prints build summary.
        """

        print("─" * self.HEADER_WIDTH)
        print("Summary")
        print("─" * self.HEADER_WIDTH)
        print()

        print(
            f"{'Models total':<18}: "
            f"{report.model_count}"
        )

        print(
            f"{'Succeeded':<18}: "
            f"{report.succeeded_models}"
        )

        print(
            f"{'Failed':<18}: "
            f"{report.failed_models}"
        )

        print(
            f"{'Skipped':<18}: "
            f"{report.skipped_models}"
        )

        print()
        print("-" * self.HEADER_WIDTH)
        print()

        print(
            f"{'Assertions total':<18}: "
            f"{report.assertion_count}"
        )

        print(
            f"{'Succeeded':<18}: "
            f"{report.succeeded_assertions}"
        )

        print(
            f"{'Failed':<18}: "
            f"{report.failed_assertions}"
        )

        print(
            f"{'Skipped':<18}: "
            f"{report.skipped_assertions}"
        )

        print()
        print()

        print(
            f"{'Elapsed time':<18}: "
            f"{report.elapsed_time:.2f} s"
        )

        print()

    def print_errors(
        self,
        actions: list[WorkflowAction],
        title: str = "Errors",
        color: str | None = None,
    ) -> None:
        """
        Prints failed workflow actions.
        """

        if not actions:
            return

        title_text = title

        if color:
            title_text = f"{color}{title}{self.RESET}"

        print("─" * self.HEADER_WIDTH)
        print(title_text)
        print("─" * self.HEADER_WIDTH)
        print()

        for index, action in enumerate(actions):

            display_name = (
                f"{action.target.schema}."
                f"{action.target.name}"
            )

            if action.is_model:
                display_name += (
                    f" ({action.action_type})"
                )

            print(
                f"{self._format_time(action)}  "
                f"{self._format_status(action)}  "
                f"{display_name}"
            )

            print()

            print(
                action.failure_reason
                or "No failure reason was provided."
            )

            if index < len(actions) - 1:
                print()
                print("-" * self.HEADER_WIDTH)
                print()

    def print_colored_message(
        self,
        message: str,
        color: str,
    ) -> None:
        print(f"{color}{message}{self.RESET}")

    def _print_action(
        self,
        action: WorkflowAction,
        display_name: str,
    ) -> None:
        """
        Prints one workflow action.
        """

        print(
            f"{self._format_time(action)}  "
            f"{self._format_status(action)}  "
            f"{display_name:<{self.NAME_WIDTH}}"
            f"{self._format_duration(action)}",
            flush=True,
        )

    def _format_time(
        self,
        action: WorkflowAction,
    ) -> str:

        if action.start_time is None:
            return "--:--:--"

        return action.start_time.strftime("%H:%M:%S")

    def _format_status(
        self,
        action: WorkflowAction,
    ) -> str:

        return self.STATUS.get(
            action.state,
            "[????]",
        )

    def _format_duration(
        self,
        action: WorkflowAction,
    ) -> str:

        if action.state not in {
            "SUCCEEDED",
            "FAILED",
        }:
            return ""

        if action.duration_seconds is None:
            return f"{'-':>8}"

        return f"{action.duration_seconds:.2f}s".rjust(8)
