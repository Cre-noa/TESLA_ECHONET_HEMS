#!/usr/bin/env python3
from __future__ import annotations
import sys
from common import dry_run_response, emit, env_bool, run_command_template


def main() -> None:
    if len(sys.argv) < 2:
        emit({"status": "error", "msg": "Usage: set_charge_amps.py <amps>"}, 2)
    try:
        amps = int(sys.argv[1])
    except ValueError:
        emit({"status": "error", "msg": "amps must be an integer"}, 2)
    if amps < 0 or amps > 80:
        emit({"status": "error", "msg": "amps is outside safety bounds"}, 2)

    if env_bool("TESLA_OPS_DRY_RUN", True):
        emit(dry_run_response("set_charge_amps", amps=amps))

    emit(run_command_template("TESLA_SET_AMPS_COMMAND", amps=amps))


if __name__ == "__main__":
    main()
