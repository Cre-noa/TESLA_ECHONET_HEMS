# Setup Guide

## 1. 前提

この手順は、READ VMとPOST VMを分ける構成を前提にしています。検証だけなら1台でも動かせますが、本番ではTesla/Fleet認証情報をPOST VM側へ閉じ込める構成がおすすめです。

推奨環境:

- Ubuntu 22.04 / 24.04 または Raspberry Pi OS
- Python 3.10+
- Tesla Wall Connector Local API にアクセスできるLAN
- HEMS/ECHONET Lite情報をWebSocketで配信するプロセス
  - ECHONET Lite機器一覧・EPC定義・WebSocketブリッジ等に第三者成果物を使う場合は、`THIRD_PARTY_NOTICES.md` にクレジットとライセンスを明記してください
- 蓄電池/Tesla操作を受け付けるPOST側API
- 任意: InfluxDB / Grafana / TeslaMate / Discord Webhook

## 2. インストール

```bash
git clone <your-repository-url>
cd smart-hems-public

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 3. READ VM側の設定

READ VMにこのリポジトリを配置します。

```bash
cp .env.example .env
cp samples/smart_hems_settings.example.json smart_hems_settings.json
vim .env
```

最低限、次を自分の環境に合わせてください。特に `POST_CONTROL_BASE_URL` はPOST VMのIPを指定します。

| 変数 | 内容 |
|---|---|
| `HEMS_WS_URL` | HEMS/ECHONET Lite WebSocket URL |
| `TARGET_SOLAR` | 太陽光デバイス識別子 |
| `TARGET_BATTERY` | 蓄電池デバイス識別子 |
| `POST_CONTROL_BASE_URL` | 蓄電池/Tesla制御を受けるPOST VM側API。例: `http://<post-vm-host>:<port>` |
| `FLEET_STATE_ENDPOINT` | POST VMが提供するFleet状態取得API。通常は `${POST_CONTROL_BASE_URL}/api/tesla/fleet_state` |
| `WALL_CONNECTOR_IP` | Tesla Wall Connector の固定IP |
| `HOME_LATITUDE` / `HOME_LONGITUDE` | 発電予測に使う緯度経度 |
| `INFLUX_*` | InfluxDBに保存する場合のみ設定 |



### VM分離時の設定例

READ VM側 `.env`:

```env
APP_HOST=0.0.0.0
APP_PORT=5000
POST_CONTROL_BASE_URL=http://<post-vm-host>:<port>
FLEET_STATE_ENDPOINT=http://<post-vm-host>:<port>/api/tesla/fleet_state
WALL_CONNECTOR_IP=<wall-connector-ip>
HEMS_WS_URL=wss://<hems-read-source>:8080/ws
```

POST VM側は、このリポジトリとは別の制御プロキシを動かします。POST VMにはTesla/Fleet tokenや蓄電池操作に必要な秘密情報を置き、READ VMからのHTTPアクセスだけを許可してください。

POST VMに最低限必要な想定API:

```text
GET  /api/status
POST /api/command
GET  /api/tesla/fleet_state
```

`POST_CONTROL_BASE_URL=http://127.0.0.1:5000` は、READとPOSTを同一VMで試す場合だけ使います。VM分離時にlocalhostのままだと、READ VMが自分自身へPOSTしようとして、実機制御先に届きません。

## 4. ECHONET Lite / echonet-list の準備

このリポジトリのREAD VMは、`HEMS_WS_URL` で指定したWebSocketから太陽光・蓄電池などの値を受け取ります。想定している外部プロセスは `koizuka/echonet-list` です。

`echonet-list` 側は別リポジトリとして取得・セットアップしてください。この公開版には `echonet-list` のソースコード、生成物、証明書、設定ファイル、機器一覧、EPC定義データは同梱していません。

参考構成:

```text
ECHONET Lite機器 ── multicast/LAN ── echonet-list ── WebSocket ── Smart HEMS READ VM
```

READ VM側 `.env` の例:

```env
HEMS_WS_URL=wss://<echonet-list-host>:8080/ws
TARGET_SOLAR=<echonet-listで見える太陽光デバイス識別子>
TARGET_BATTERY=<echonet-listで見える蓄電池デバイス識別子>
```

`echonet-list` はTLS/WSSで使う構成が基本です。証明書やmkcert、systemd化などは upstream のREADME/docsに従ってください。

公開前に確認すること:

1. `THIRD_PARTY_NOTICES.md` に `koizuka/echonet-list`、URL、MIT License、著作権表示を書いたか
2. このリポジトリへ `echonet-list` のコードや生成物をコピーしていないか
3. コピーしている場合は、MIT License本文と著作権表示を保持しているか
4. 自宅固有の `config.toml`、証明書、検出済みデバイス一覧などをコミットしていないか

ライセンスや再配布条件が不明な追加コード・データは同梱せず、README上では「別途用意する外部プロセス」として扱ってください。

## 4.5 契約時間帯・固有値の設定

具体的な契約時間帯、自宅LAN IP、機器識別子、DB名、Webhook URLは公開リポジトリに書きません。`.env` または `smart_hems_settings.json` にだけ保存します。設定対象の一覧は `docs/CONFIGURATION.md` を参照してください。

契約時間帯は次の抽象名で扱います。

- `PRIMARY_CHARGE_WINDOW`
- `SOLAR_ASSIST_CHARGE_WINDOW`
- `SOLAR_PLANNING_POLL_WINDOW`

形式は `HH:MM-HH:MM` ですが、具体時刻は各利用者の契約に合わせてローカルでのみ設定してください。

## 5. 起動

```bash
source .venv/bin/activate
python smart_hems_app.py
```

ブラウザで次を開きます。

```text
http://<server-ip>:5000/
```

## 6. systemd化する例

`/etc/systemd/system/smart-hems.service`:

```ini
[Unit]
Description=Smart HEMS Controller
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/smart-hems
EnvironmentFile=/opt/smart-hems/.env
ExecStart=/opt/smart-hems/.venv/bin/python /opt/smart-hems/smart_hems_app.py
Restart=always
RestartSec=10
User=smarthems
Group=smarthems

[Install]
WantedBy=multi-user.target
```

反映:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now smart-hems
sudo journalctl -u smart-hems -f
```

## 7. 初回検証のおすすめ順

1. READ VMの `.env` と `smart_hems_settings.json` を設定する
2. POST VMのAPIを停止したまま、READ VMのUIが起動するか確認する
3. Wall Connectorの値だけ見えるか確認する
4. POST VMを起動し、READ VMから `GET /api/status` が通るか確認する
5. READ VMの読み取り専用モードをONにしたまま、Fleet状態取得だけ確認する
6. 蓄電池/Teslaの制御は低リスクなコマンドから始める
7. Tesla電流は低Aから試す
8. 契約充電枠の自動制御はログを見ながら1日単位で調整する

## 8. トラブルシュート

### `ModuleNotFoundError`

仮想環境を有効化してから `pip install -r requirements.txt` を実行してください。

### InfluxDBに書き込まれない

`INFLUX_TOKEN` が空の場合、メトリクス保存はスキップされます。公開用には空が安全です。

### Tesla制御が飛ばない

このアプリは自宅判定ができない場合、Teslaへのstart/stop/amp変更を抑止します。`WALL_CONNECTOR_IP`、Wall Connector接続状態、`TESLA_HOME_*` を確認してください。

加えて、VM分離構成では `POST_CONTROL_BASE_URL` がPOST VMを向いているか確認してください。READ VM上で `curl http://<post-vm-host>:<port>/api/status` が成功する状態が目安です。

### Grafana画面が空

Grafana iframeはローカル環境に依存します。公開時は具体URLやdashboard UIDを残さず、必要に応じて `.env` 側のURLへ逃がしてください。

## POST VMも立ち上げる場合

POST VM側は `post_vm/control_proxy.py` を使います。

```bash
cd /opt/smart-hems
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
cp samples/post_config.example.json config.json
```

`.env` にはPOST VM固有の値を入れます。

```env
HEMS_WS_URL=wss://<echonet-list-host>:8080/ws
TARGET_BATTERY_CONTROLLER=<battery-controller-device-id>
POST_FLASK_HOST=0.0.0.0
POST_FLASK_PORT=5000
PRIMARY_CHARGE_WINDOW=<HH:MM-HH:MM>
SOLAR_ASSIST_CHARGE_WINDOW=<HH:MM-HH:MM>
TESLA_SCRIPT_DIR=/opt/tesla-control
DISCORD_WEBHOOK_URL=
```

READ VM側では、POST VMのURLだけを指定します。

```env
POST_CONTROL_BASE_URL=http://<post-vm-host>:5000
FLEET_STATE_ENDPOINT=http://<post-vm-host>:5000/api/tesla/fleet_state
```

具体的な契約時刻、自宅内IP、機器識別子、DB名、webhookは公開リポジトリに書かず、`.env` とローカル設定ファイルだけに置いてください。


## Tesla操作アダプタの疎通確認

公開版のPOST VMは、初期状態では実車操作を行わないdry-runアダプタを使います。

```bash
python post_vm/tesla_ops/set_charge_amps.py 5
python post_vm/tesla_ops/start_charge.py
python post_vm/tesla_ops/stop_charge.py
python post_vm/tesla_ops/fleet_state_json.py
```

実運用に切り替える場合は `.env` で `TESLA_OPS_DRY_RUN=false` にし、`TESLA_SET_AMPS_COMMAND` などをPOST VM内のプライベート実装へ向けます。
