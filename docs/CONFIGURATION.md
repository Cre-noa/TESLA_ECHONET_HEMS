# Configuration Reference

この公開版では、家庭固有値・契約固有値・秘密情報をソースコードに直書きしない方針です。
`.env.example` を `.env` にコピーし、各自の環境でのみ値を入れてください。`.env` はGit管理外です。

## 置換・設定が必要な主な値

| 区分 | 変数 / ファイル | 内容 | 公開リポジトリに入れない理由 |
|---|---|---|---|
| ECHONET Lite | `HEMS_WS_URL` | echonet-list等のWebSocket URL | LAN構成が分かるため |
| ECHONET Lite | `TARGET_SOLAR` | 太陽光デバイス識別子 | 機器構成・識別子が分かるため |
| ECHONET Lite | `TARGET_BATTERY` | 蓄電池デバイス識別子 | 機器構成・識別子が分かるため |
| POST VM | `POST_CONTROL_BASE_URL` | POST VM制御APIのベースURL | LAN構成が分かるため |
| POST VM | `FLEET_STATE_ENDPOINT` | Fleet状態取得API | LAN構成・API構成が分かるため |
| Wall Connector | `WALL_CONNECTOR_IP` | Wall ConnectorのLAN IP | 自宅ネットワーク情報のため |
| 位置情報 | `HOME_LATITUDE` / `HOME_LONGITUDE` | 発電予測用の緯度経度 | 正確な居住地推定につながるため |
| Tesla | `TESLA_HOME_*` | 自宅判定用のgeofence/座標/半径 | 居住地・車両運用情報のため |
| 契約 | `PRIMARY_CHARGE_WINDOW` | 契約上の充電優先枠 | 具体的な契約運用を公開しないため |
| 契約 | `SOLAR_ASSIST_CHARGE_WINDOW` | 太陽光併用・充電優先枠 | 具体的な契約運用を公開しないため |
| 契約 | `SOLAR_PLANNING_POLL_WINDOW` | Fleet計画取得の許可枠 | 具体的な契約運用を公開しないため |
| InfluxDB | `INFLUX_URL` / `INFLUX_TOKEN` / `INFLUX_ORG` / `INFLUX_BUCKET` | メトリクス保存先 | token・DB名・構成情報のため |
| 通知 | `DISCORD_WEBHOOK_URL` | Discord通知先 | webhookはURL自体が秘密情報のため |
| Grafana | `GRAFANA_*_DASHBOARD_URL` | ローカルダッシュボードURL | UIDやLAN構成が分かるため |

## Tesla操作アダプタ

POST VMはTesla操作用に `post_vm/tesla_ops/` の4スクリプトを参照します。公開版の初期設定はdry-runです。

| 変数 | 内容 | 公開リポジトリでの扱い |
|---|---|---|
| `TESLA_SCRIPT_DIR` | Tesla操作スクリプトの場所 | 既定は同梱アダプタ |
| `TESLA_SET_AMPS_SCRIPT` | A変更スクリプト | 同梱アダプタまたはローカル実装 |
| `TESLA_START_SCRIPT` | 充電開始スクリプト | 同梱アダプタまたはローカル実装 |
| `TESLA_STOP_SCRIPT` | 充電停止スクリプト | 同梱アダプタまたはローカル実装 |
| `TESLA_FLEET_STATE_SCRIPT` | Fleet状態取得スクリプト | 同梱アダプタまたはローカル実装 |
| `TESLA_OPS_DRY_RUN` | 実車操作を行わない安全モード | 公開版は `true` 推奨 |
| `TESLA_*_COMMAND` | プライベート実装への委譲コマンド | `.env` のみに記載 |

Gitに入れないもの: Tesla/Fleet token、refresh token、vehicle ID、VIN、アカウントID、実APIレスポンス、実車の位置情報。

## 契約時間帯の扱い

公開リポジトリには、具体的な時刻を書かない方針です。コード上では以下の抽象名だけを使います。

| 抽象名 | 意味 |
|---|---|
| `primary_charge_window` | 主に充電を積極利用する契約枠 |
| `solar_assist_charge_window` | 太陽光・蓄電池・EVの配分を重点判断する契約枠 |
| `solar_planning_poll_window` | Fleet APIを計画目的で取得してよい枠 |

`.env` では `HH:MM-HH:MM` の形式で設定します。具体時刻は公開せず、利用者自身の契約に合わせます。

## 設定ファイルとGit管理

Gitに入れてよいもの:

- `.env.example`
- `samples/smart_hems_settings.example.json`
- `docs/CONFIGURATION.md`

Gitに入れないもの:

- `.env`
- `smart_hems_settings.json`
- `tesla_range_cache.json`
- `SmartHEMS_log*.txt`
- `*.token`
- `*.secret`
- 証明書・秘密鍵・mkcert生成物
- echonet-listの検出済みデバイス一覧やローカルconfig

## 確認用grep

公開前に最低限、次を実行します。

```bash
grep -R "192\\.168\\|10\\.0\\|172\\.16" -n . --exclude-dir=.git
grep -R "35\\.|139\\.|140\\." -n . --exclude-dir=.git
grep -R "TOKEN\\|WEBHOOK\\|SECRET\\|PASSWORD" -n . --exclude-dir=.git
grep -R "INFLUX_.*=" -n . --exclude-dir=.git
```

ヒットした場合でも、`.env.example` の空欄やドキュメント上の変数名であれば問題ありません。実値・token・自宅IP・正確な座標が残っていないことを確認してください。
