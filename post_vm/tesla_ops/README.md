# Tesla operation adapters

`control_proxy.py` calls these scripts from POST VM when READ VM requests Tesla operations.

The public repository intentionally does not include Tesla OAuth refresh tokens, vehicle IDs, VINs, or a vendor-specific private implementation. Instead, these adapters provide a stable interface and can either run in dry-run mode or delegate to local commands defined in `.env`.

## Scripts

| Script | Purpose | Called by |
|---|---|---|
| `set_charge_amps.py <amps>` | Set charge current | `POST /api/command` with `action=tesla` |
| `start_charge.py` | Start charging | `action=tesla_start` |
| `stop_charge.py` | Stop charging | `action=tesla_stop` |
| `fleet_state_json.py` | Return Fleet/Tesla state JSON | `GET /api/tesla/fleet_state` |

Every script prints JSON to stdout. `control_proxy.py` treats `{"status":"success"}` as success.

## Dry-run mode

Dry-run is enabled by default for safety:

```env
TESLA_OPS_DRY_RUN=true
```

For real operation, set it to `false` and provide local command templates:

```env
TESLA_OPS_DRY_RUN=false
TESLA_SET_AMPS_COMMAND=/opt/private-tesla/set_amps --amps {amps}
TESLA_START_COMMAND=/opt/private-tesla/start_charge
TESLA_STOP_COMMAND=/opt/private-tesla/stop_charge
TESLA_FLEET_STATE_COMMAND=/opt/private-tesla/fleet_state_json
```

The command outputs should be JSON. Keep any Tesla/Fleet tokens and vehicle-specific IDs outside this repository.
