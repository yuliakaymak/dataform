#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any
from dataclasses import dataclass

@dataclass(frozen=True)
class Target:
    database: str
    schema: str
    name: str

DEFAULT_GRAPH_PATH = Path("artifacts/graph.json")

class GraphParser:
    """Parses a compiled Dataform graph."""

    def __init__(self, graph_path: Path):
        self.graph_path = graph_path
        self.graph = self._load_graph()

    def _load_graph(self) -> dict[str, Any]:
        """Load and validate the compiled Dataform graph."""

        if not self.graph_path.exists():
            raise FileNotFoundError(
                f"Graph file not found: {self.graph_path.resolve()}"
            )

        with self.graph_path.open("r", encoding="utf-8") as file:
            return json.load(file)

    def get_models(self) -> list[dict[str, Any]]:
        """Return all compiled models."""

        return self.graph.get("tables", [])

    def get_assertions(self) -> list[dict[str, Any]]:
        """Return all compiled assertions."""

        return self.graph.get("assertions", [])

    def get_schemas(self) -> set[str]:
        """Return all unique schemas used by models and assertions."""

        schemas: set[str] = set()

        for model in self.get_models():
            schemas.add(model["target"]["schema"])

        for assertion in self.get_assertions():
            schemas.add(assertion["target"]["schema"])

        return schemas

    def get_tags(self) -> set[str]:
        """Return all unique tags."""

        tags: set[str] = set()

        for model in self.get_models():
            tags.update(model.get("tags", []))

        for assertion in self.get_assertions():
            tags.update(assertion.get("tags", []))

        return tags


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Parse a compiled Dataform graph."
    )

    parser.add_argument(
        "--graph",
        type=Path,
        default=DEFAULT_GRAPH_PATH,
        help=f"Path to graph.json (default: {DEFAULT_GRAPH_PATH})"
    )

    parser.add_argument(
        "--schemas",
        action="store_true",
        help="Print unique schemas."
    )

    parser.add_argument(
        "--tags",
        action="store_true",
        help="Print unique tags."
    )

    parser.add_argument(
        "--summary",
        action="store_true",
        help="Print graph summary."
    )

    args = parser.parse_args()

    graph = GraphParser(args.graph)

    if args.schemas:
        for schema in sorted(graph.get_schemas()):
            print(schema)
        return

    if args.tags:
        for tag in sorted(graph.get_tags()):
            print(tag)
        return

    if args.summary:
        print("Dataform Graph Summary")
        print("----------------------")
        print(f"Models      : {len(graph.get_models())}")
        print(f"Assertions  : {len(graph.get_assertions())}")
        print(f"Schemas     : {len(graph.get_schemas())}")
        print(f"Tags        : {len(graph.get_tags())}")
        return

    parser.print_help()


if __name__ == "__main__":
    main()