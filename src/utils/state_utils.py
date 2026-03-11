from __future__ import annotations

import json
from pathlib import Path
from typing import Set


def read_processed_months(state_file: str) -> Set[str]:
    path = Path(state_file)
    if not path.exists():
        return set()
    return set(json.loads(path.read_text(encoding="utf-8")).get("processed_months", []))


def write_processed_months(state_file: str, months: Set[str]) -> None:
    path = Path(state_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"processed_months": sorted(months)}
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
