# POST VM Guide

POST VMは、READ VMから受けたHTTP要求を、Tesla/Fleet API用スクリプトとECHONET Lite制御へ橋渡しする制御プロキシです。
ユーザー提供の `control4-3.py` を元に、公開用として秘密情報・自宅内IP・具体的な契約時刻を `.env` へ逃がした `post_vm/control_proxy.py` を同梱しています。

## なぜREAD VMとPOST VMを分けるのか

```text
ECHONET Lite devices
  ↓
koizuka/echonet-list
  ↓ WebSocket
READ VM: smart_hems_app.py
  - 状態監視
  - UI
  - 発電予測
  - 制御判断
  - Wall Connector読取
  ↓ HTTP only
POST VM: post_vm/control_proxy.py
  - Tesla/Fleet tokenを持つ
  - Tesla start/stop/ampsを実行
  - 蓄電池mode変更を送信
  - Fleet状態取得を代理実行
```

分離する理由は次の通りです。

1. Tesla/Fleet token、webhook、機器制御情報をPOST VMへ隔離するため。
2. READ VMを公開しやすくし、UIや判断ロジックだけを安全に共有するため。
3. 制御系APIをREAD VMからのLAN内アクセスだけに制限するため。
4. READ側に不具合が出ても、POST側の操作面を切り分けやすくするため。

## POST VMの公開用コード

同梱ファイル:

```text
post_vm/control_proxy.py
post_vm/tesla_ops/
samples/post_config.example.json
samples/fleet_state.sample.json
```

元のPOST側プログラムには以下が直書きされていたため、公開版では環境変数へ移しています。

| 元の種類 | 公開版の設定先 |
|---|---|
| Discord webhook URL | `DISCORD_WEBHOOK_URL` |
| ECHONET Lite WebSocket URL | `HEMS_WS_URL` |
| 蓄電池コントローラ識別子 | `TARGET_BATTERY_CONTROLLER` |
| Tesla制御スクリプト配置先 | `TESLA_SCRIPT_DIR` / `TESLA_*_SCRIPT` |
| 契約上の充電制御枠 | `PRIMARY_CHARGE_WINDOW` / `SOLAR_ASSIST_CHARGE_WINDOW` |
| 定期再起動時刻 | `POST_RESTART_AT` |
| Flask listen先 | `POST_FLASK_HOST` / `POST_FLASK_PORT` |

公開リポジトリでは具体的な契約時刻を明記しません。利用者の契約に合わせてローカル `.env` にだけ書きます。

## POST VM `.env` 例

```env
HEMS_WS_URL=wss://<echonet-list-host>:8080/ws
TARGET_BATTERY_CONTROLLER=<battery-controller-device-id>

POST_FLASK_HOST=0.0.0.0
POST_FLASK_PORT=5000
POST_CONFIG_FILE=config.json

PRIMARY_CHARGE_WINDOW=<HH:MM-HH:MM>
SOLAR_ASSIST_CHARGE_WINDOW=<HH:MM-HH:MM>
POST_RESTART_AT=<HH:MM>

TESLA_SCRIPT_DIR=post_vm/tesla_ops
TESLA_SET_AMPS_SCRIPT=post_vm/tesla_ops/set_charge_amps.py
TESLA_START_SCRIPT=post_vm/tesla_ops/start_charge.py
TESLA_STOP_SCRIPT=post_vm/tesla_ops/stop_charge.py
TESLA_FLEET_STATE_SCRIPT=post_vm/tesla_ops/fleet_state_json.py
TESLA_OPS_DRY_RUN=true
TESLA_SET_AMPS_COMMAND=
TESLA_START_COMMAND=
TESLA_STOP_COMMAND=
TESLA_FLEET_STATE_COMMAND=

DISCORD_WEBHOOK_URL=
```


## Tesla操作スクリプトの扱い

POST VMの `control_proxy.py` は、Tesla操作を直接内蔵せず、次の4つのスクリプトを実行します。

| スクリプト | 役割 | 期待する出力 |
|---|---|---|
| `set_charge_amps.py <amps>` | 充電電流変更 | JSON |
| `start_charge.py` | 充電開始 | JSON |
| `stop_charge.py` | 充電停止 | JSON |
| `fleet_state_json.py` | Fleet/Tesla状態取得 | JSON |

公開版には `post_vm/tesla_ops/` として安全なアダプタを同梱しています。初期値では `TESLA_OPS_DRY_RUN=true` なので、実車には何も送らずJSONだけ返します。

実運用では、Tesla/Fleet token、vehicle ID、VIN、refresh tokenをこのリポジトリへ入れず、POST VM内のプライベート実装へ委譲します。

```env
TESLA_OPS_DRY_RUN=false
TESLA_SET_AMPS_COMMAND=/opt/private-tesla/set_amps --amps {amps}
TESLA_START_COMMAND=/opt/private-tesla/start_charge
TESLA_STOP_COMMAND=/opt/private-tesla/stop_charge
TESLA_FLEET_STATE_COMMAND=/opt/private-tesla/fleet_state_json
```

`TESLA_FLEET_STATE_SAMPLE_FILE=samples/fleet_state.sample.json` を使うと、Fleet APIなしでREAD/POST連携の疎通確認ができます。

## READ VMから見たPOST VM API

### `GET /api/status`

蓄電池・制御状態を返します。

### `POST /api/command`

主なaction:

| action | value | 意味 |
|---|---:|---|
| `auto` | 省略 | 蓄電池を自動運転へ戻す |
| `standby` | 省略 | 蓄電池を待機 |
| `charge` | 省略 | 蓄電池充電 |
| `thre_solar_assist` | 数値 | solar assist window側の目標SOC変更 |
| `thre_primary` | 数値 | primary charge window側の目標SOC変更 |
| `thre_day` / `thre_night` | 数値 | 互換用エイリアス |
| `tesla` | A値 | Tesla充電電流変更 |
| `tesla_start` | 省略 | Tesla充電開始 |
| `tesla_stop` | 省略 | Tesla充電停止 |
| `update_tesla_state` | bool | READ VMからTesla充電状態を同期 |

### `GET /api/tesla/fleet_state`

POST VM内のFleet取得スクリプトを実行し、READ VMへ結果を返します。

## systemd構成例

```ini
[Unit]
Description=Smart HEMS POST Control Proxy
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/smart-hems/post_vm
EnvironmentFile=/opt/smart-hems/.env
ExecStart=/opt/smart-hems/.venv/bin/python /opt/smart-hems/post_vm/control_proxy.py
Restart=always
RestartSec=10
User=smarthems
Group=smarthems

[Install]
WantedBy=multi-user.target
```

## ネットワーク制限

POST VMはインターネットへ公開しないでください。最低限、READ VMからPOST VMのAPIポートだけを許可します。

```text
READ VM  ── allowed ──▶ POST VM TCP <POST_FLASK_PORT>
その他LAN ── denied  ─▶ POST VM TCP <POST_FLASK_PORT>
Internet ─ denied  ─▶ POST VM TCP <POST_FLASK_PORT>
```

## Gitに入れないもの

- `.env`
- `config.json`
- Tesla/Fleet token・refresh token
- VIN・vehicle ID・account ID
- Discord webhook URL
- ECHONET Liteの検出済み機器一覧
- 自宅LAN IPや実デバイス識別子
- 実行ログ・APIレスポンスキャッシュ
