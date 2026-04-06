"""Assemble and write JSON output."""

import json
from datetime import datetime, timezone
from pathlib import Path


def write_output(
    crate_name: str,
    categorized: dict,
    output_dir: str = "docs",
    npm_dependents: list[dict] | None = None,
):
    """Write categorized dependants to a JSON file."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Build summary
    summary = {}
    total = 0
    for list_name, entries in categorized.items():
        summary[list_name] = len(entries)
        total += len(entries)

    output = {
        "crate": crate_name,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "total": total,
        "summary": summary,
        "lists": categorized,
    }

    if npm_dependents:
        output["npm_dependents"] = npm_dependents

    path = Path(output_dir) / f"{crate_name}.json"
    path.write_text(json.dumps(output, indent=2) + "\n")
    return str(path)
