#!/usr/bin/env python3
"""Fail when an Autobahn report contains failed or unimplemented cases."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Iterator


FAILED_BEHAVIORS = {"FAILED", "UNIMPLEMENTED"}


def walk(value: Any) -> Iterator[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk(child)


def main() -> int:
    if len(sys.argv) != 2:
        print(f"Uso: {Path(sys.argv[0]).name} DIRECTORIO", file=sys.stderr)
        return 2

    report_dir = Path(sys.argv[1])
    report_files = sorted(report_dir.rglob("*.json"))
    if not report_files:
        print(f"No se encontraron reportes JSON en {report_dir}.", file=sys.stderr)
        return 2

    failures: list[tuple[Path, str, str]] = []
    behavior_records = 0
    for report_file in report_files:
        try:
            report = json.loads(report_file.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            print(f"No se pudo leer {report_file}: {error}", file=sys.stderr)
            return 2

        for record in walk(report):
            behavior = record.get("behavior")
            if behavior is not None:
                behavior_records += 1
            if behavior not in FAILED_BEHAVIORS:
                continue
            case_id = next(
                (
                    str(record[key])
                    for key in ("case", "caseId", "id")
                    if key in record
                ),
                "caso desconocido",
            )
            failures.append((report_file, case_id, behavior))

    if behavior_records == 0:
        print(
            "Los reportes no contienen resultados Autobahn; compruebe la conexion.",
            file=sys.stderr,
        )
        return 2

    if failures:
        for report_file, case_id, behavior in failures:
            print(f"{behavior}: {case_id} ({report_file})", file=sys.stderr)
        return 1

    print(
        f"Autobahn: {behavior_records} resultados en {len(report_files)} "
        "reportes JSON sin fallos."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
