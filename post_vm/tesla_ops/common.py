#!/usr/bin/env python3
"""Shared helpers for Tesla operation adapter scripts.

These scripts are intentionally small wrappers. Keep Tesla/Fleet tokens and any
real vendor API implementation outside the public repository, then connect it by
setting TESLA_*_COMMAND environment variables in .env.
"""
from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
    load_dotenv()
except Exception:
    pass


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def emit(payload: dict[str, Any], status_code: int = 0) -> None:
    print(json.dumps(payload, ensure_ascii=False))
    raise SystemExit(status_code)


def run_command_template(env_name: str, **values: Any) -> dict[str, Any]:
    template = os.getenv(env_name, "").strip()
    if not template:
        return {
            "status": "error",
            "stage": "not_configured",
            "msg": f"{env_name} is not configured. Set TESLA_OPS_DRY_RUN=true for simulation or provide a local command.",
        }

    command = template.format(**values)
    result = subprocess.run(
        shlex.split(command),
        cwd=os.getenv("TESLA_COMMAND_CWD") or None,
        capture_output=True,
        text=True,
        timeout=int(os.getenv("TESLA_COMMAND_TIMEOUT_SEC", "60")),
    )
    if result.returncode != 0:
        return {
            "status": "error",
            "stage": "subprocess",
            "returncode": result.returncode,
            "stdout": result.stdout[-2000:],
            "stderr": result.stderr[-2000:],
        }

    stdout = result.stdout.strip()
    if stdout:
        try:
            parsed = json.loads(stdout)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass
    return {"status": "success", "stdout": stdout, "stderr": result.stderr[-2000:]}


def dry_run_response(action: str, **extra: Any) -> dict[str, Any]:
    payload = {"status": "success", "dry_run": True, "action": action}
    payload.update(extra)
    return payload
