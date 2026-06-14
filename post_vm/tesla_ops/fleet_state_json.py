#!/usr/bin/env python3
from __future__ import annotations
import json
import os
from pathlib import Path
from common import dry_run_response, emit, env_bool, run_command_template


def main() -> None:
    sample_file = os.getenv("TESLA_FLEET_STATE_SAMPLE_FILE", "").strip()
    if sample_file:
        path = Path(sample_file)
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                emit(data)
        except Exception as e:
            emit({"status": "error", "stage": "sample_file", "msg": str(e)}, 1)

    if env_bool("TESLA_OPS_DRY_RUN", True):
        emit(dry_run_response(
            "fleet_state",
            classification="unknown",
            charge_state={
                "battery_level": None,
                "charging_state": "Unknown",
                "charge_limit_soc": None,
            },
            note="Dry-run placeholder. Configure TESLA_FLEET_STATE_COMMAND or TESLA_FLEET_STATE_SAMPLE_FILE for real data.",
        ))

    emit(run_command_template("TESLA_FLEET_STATE_COMMAND"))


if __name__ == "__main__":
    main()
