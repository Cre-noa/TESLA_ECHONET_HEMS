# Third Party Notices

このリポジトリで利用・参照している第三者成果物を記録するためのファイルです。

## ECHONET Lite / HEMS関連

このアプリは、`HEMS_WS_URL` で指定したWebSocketから、太陽光・蓄電池などのECHONET Lite由来データを受け取る前提です。

### koizuka/echonet-list

- Project / Author: `koizuka/echonet-list` / `@koizuka`
- URL: <https://github.com/koizuka/echonet-list>
- License: MIT License
- Copyright notice: `Copyright (c) 2025 @koizuka`
- Used for: ECHONET Lite device discovery/control, device/property list handling, and WebSocket source for HEMS/ECHONET Lite values used by this READ VM
- Included in this repository: No
- Modified: No
- Runtime relationship: This Smart HEMS READ VM connects to an external `echonet-list` process via `HEMS_WS_URL` such as `wss://<echonet-list-host>:8080/ws`.
- Notes:
  - This repository does not include `echonet-list` source code, generated bundles, certificates, configuration files, or copied device/property definition data.
  - If any `echonet-list` code, generated assets, configuration examples, or substantial portions are copied into this repository in the future, keep the MIT license/copyright notice with the copied material.
  - Users should follow the upstream repository's setup and license terms.

## Python packages

Python依存パッケージは `requirements.txt` を参照してください。各パッケージのライセンスは、それぞれの配布元に従います。
