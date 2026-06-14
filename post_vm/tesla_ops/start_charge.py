#!/usr/bin/env python3
from __future__ import annotations
from common import dry_run_response, emit, env_bool, run_command_template


def main() -> None:
    if env_bool("TESLA_OPS_DRY_RUN", True):
        emit(dry_run_response("start_charge"))
    emit(run_command_template("TESLA_START_COMMAND"))


if __name__ == "__main__":
    main()
