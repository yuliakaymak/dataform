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

    STATUS = {
        "SUCCEEDED": "[ OK ]",
        "FAILED": "[FAIL]",
        "RUNNING": "[RUN ]",
        "CANCELLED": "[SKIP]",
    }

    def print_build_report(
        self,
        report: BuildReport,
    ) -> None:

        print("─" * self.HEADER_WIDTH)
        print("Dataform Build")
        print("─" * self.HEADER_WIDTH)
        print()

        self._print_models(report)
        self._print_assertions(report)
        self._print_summary(report)

    def _print_models(
        self,
        report: BuildReport,
    ) -> None:
        """
        Prints executed models.
        """

        print("=" * self.SECTION_WIDTH)
        print("Models")
        print("=" * self.SECTION_WIDTH)

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

    def _print_assertions(
        self,
        report: BuildReport,
    ) -> None:
        """
        Prints executed assertions.
        """

        print("=" * self.SECTION_WIDTH)
        print("Assertions")
        print("=" * self.SECTION_WIDTH)

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
            f"{'Models executed':<18}: "
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

        print()

        print(
            f"{'Assertions':<18}: "
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

        print()

        print(
            f"{'Elapsed time':<18}: "
            f"{report.elapsed_time:.2f} s"
        )

        print()

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
            f"{self._format_duration(action):>8}"
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

        if action.duration_seconds is None:
            return "-"

        return f"{action.duration_seconds:.2f}s"