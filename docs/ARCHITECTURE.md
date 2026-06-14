# Architecture

## READ VM / POST VM 分離構成

このアプリは、実運用では **READ側VM** と **POST側VM** を分ける前提の設計です。

```text
                         ┌────────────────────────────┐
                         │          READ VM            │
                         │  smart_hems_app.py          │
                         │  - 監視 / 集計              │
HEMS WebSocket ─────────▶│  - Wall Connector読取       │
Wall Connector Local API▶│  - 発電予測                 │
Open-Meteo / pvlib ─────▶│  - 制御判断                 │
                         │  - Flask UI / JSON API      │
                         └───────────┬────────────────┘
                                     │ HTTP only
                                     │ GET  /api/status
                                     │ POST /api/command
                                     │ GET  /api/tesla/fleet_state
                                     ▼
                         ┌────────────────────────────┐
                         │          POST VM            │
                         │  Control / Fleet Proxy      │
                         │  - 蓄電池コマンド送信       │
                         │  - Tesla start/stop/amps    │
                         │  - Fleet API OAuth/token    │
                         │  - Fleet state取得/分類補助 │
                         └────────────────────────────┘
```

### なぜ分けるか

この構成では、READ VMとPOST VMを意図的に分離しています。理由は次のとおりです。

#### 1. 秘密情報をPOST VMへ閉じ込めるため

Tesla/Fleet APIのOAuth token、refresh token、車両操作に必要な情報、蓄電池操作に必要な情報は、漏れると実害があります。READ VMはUI・予測・判断ロジックを持つため、改修頻度が高く、GitHub公開対象にもなりやすい領域です。

そのため、READ VMには原則として操作系の秘密情報を置かず、POST VMにだけ保持します。READ側コードを公開しても、Tesla/Fleet tokenを含まない構成にするのが目的です。

#### 2. 実機操作の入口を小さくするため

READ VMは多数の入力を読みます。例: HEMS/ECHONET Lite WebSocket、Wall Connector Local API、Open-Meteo、InfluxDB、UI操作など。

一方、POST VMは実機へコマンドを送る小さなAPIに絞ります。ファイアウォールで **READ VMからPOST VMへのHTTPだけ許可** すれば、Teslaや蓄電池に届く経路をかなり限定できます。

#### 3. 障害時の影響範囲を分けるため

- READ VM停止: 自動判断・UIは止まるが、Tesla/Fleet tokenはREAD側に無いため露出しにくい
- POST VM停止: 実機制御は止まるが、READ VMの監視・UI・ログ・予測は継続できる
- Fleet API障害: READ VMはWall Connector実測値やlast-known cacheで安全側に倒せる

「読めるけど操作できない」と「操作できるけど判断しない」を分けることで、障害時の挙動を理解しやすくしています。

#### 4. GitHub公開と再利用をしやすくするため

公開するREAD側は、家庭固有のtokenや実操作コードを含めずに済みます。POST側は各家庭・各機器に強く依存するため、公開版ではAPI仕様だけを示し、実装は別管理にします。

#### 5. 役割を単純化して保守しやすくするため

- READ VM: 状態収集、可視化、予測、判断
- POST VM: 認証情報保持、実機操作、Fleet API代理取得

ざっくり言うと、READ VMは「頭脳と画面」、POST VMは「鍵を持った手」です。鍵を持った手を小さく隔離して、頭脳側は安全に改修しやすくしています。

### 責務分担

| VM | 主な責務 | 秘密情報 | 外部公開 |
|---|---|---|---|
| READ VM | 監視、予測、UI、制御判断、InfluxDB保存 | 原則なし。Influx token等の任意情報のみ | LAN内のみ推奨 |
| POST VM | 実機操作、Tesla/Fleet API、蓄電池制御プロキシ | Tesla/Fleet token、機器操作用情報 | READ VMからのみ許可 |

### READ VMからPOST VMへ要求するAPI

このリポジトリはREAD側です。POST側実装は含めていませんが、次のHTTP APIを想定しています。

| method | path | 用途 |
|---|---|---|
| `GET` | `/api/status` | 蓄電池制御状態、Day/Night Targetなどの取得 |
| `POST` | `/api/command` | 蓄電池・Teslaへの実コマンド送信 |
| `GET` | `/api/tesla/fleet_state` | Fleet APIから取得したTesla状態/SOC/km/分類情報の取得 |

`POST_CONTROL_BASE_URL` はPOST VMのベースURLです。例: `http://<post-vm-host>:<port>`。



```text
HEMS / ECHONET Lite WS ─┐
                        ├─ READ VM: smart_hems_app.py ── Flask UI / API
Tesla Wall Connector ───┤        │
Open-Meteo / pvlib ─────┤        ├─ HTTP ── POST VM: Control / Fleet Proxy
TeslaMate MQTT(optional) ┘        │              ├─ battery command
                                 │              ├─ Tesla command
                                 │              └─ Fleet API
                                 └─ InfluxDB(optional)
```

## 主な責務

### `smart_hems_app.py`

- 状態収集
- 発電予測
- 充電/蓄電池制御判断
- UI/API提供
- InfluxDB保存
- Discord通知

### POST control API

POST側APIは別VMで動かす前提です。このリポジトリには含めていません。以下のAPIを想定しています。POST VMはREAD VMからのアクセスだけを許可し、インターネットへ直接公開しないでください。

- `GET /api/status`
- `POST /api/command`
- `GET /api/tesla/fleet_state` または同等のFleet取得プロキシ

`POST /api/command` の主な action 例:

| action | 意味 |
|---|---|
| `auto` | 蓄電池を自動へ戻す |
| `standby` | 蓄電池を待機 |
| `charge` | 蓄電池充電 |
| `discharge` | 蓄電池放電 |
| `thre_day` | Day Target変更 |
| `thre_night` | Night Target変更 |
| `tesla` | Tesla充電電流変更 |
| `tesla_start` | Tesla充電開始 |
| `tesla_stop` | Tesla充電停止 |

## ECHONET Lite / echonet-list の扱い

このREAD VMは、ECHONET Lite機器と直接低レベル通信する実装ではなく、HEMS/ECHONET Lite情報をWebSocketで配信する外部プロセスから値を受け取る前提です。標準想定は `koizuka/echonet-list` です。

```text
ECHONET Lite devices
        │
        ▼
koizuka/echonet-list  ← MIT License / third-party work
        │ WebSocket: wss://<host>:8080/ws
        ▼
READ VM: smart_hems_app.py
```

READ VM側は `HEMS_WS_URL` に `echonet-list` のWebSocket URLを設定し、受け取ったJSONから太陽光・蓄電池などの値を抽出します。`echonet-list` の機器探索、デバイス一覧、EPC/プロパティ処理、WebSocket配信部分は第三者成果物として扱います。

この公開版では、`echonet-list` のソースコード・生成物・設定ファイル・証明書・検出済みデバイス一覧・EPC定義データを同梱しません。利用関係は `THIRD_PARTY_NOTICES.md` に明記し、実体は別プロセスとして各利用者がセットアップします。

もし将来 `echonet-list` 由来のコードや substantial portions をこのリポジトリへ取り込む場合は、MIT Licenseの著作権表示とライセンス本文を保持してください。ライセンス不明のコード・辞書・一覧データはコミットしないでください。

## 設定値とPOST VM

家庭固有値・契約固有値・DB名・Webhook URLはソースコードではなく `.env` / `smart_hems_settings.json` に置きます。詳しい設定項目は `CONFIGURATION.md`、POST VM側の必須APIは `POST_VM.md` を参照してください。

## 安全設計メモ

- Tesla制御は、自宅充電と判定できる場合だけ送信します。
- Wall Connectorの実測電流を優先して、Fleet/TeslaMateの反映遅延を吸収します。
- 買電が警告/危険域に入った場合、Tesla電流を段階降下します。
- 契約上の充電優先時間帯は、ローカル設定で定義します。公開リポジトリには具体的な契約時間帯を書かない方針です。
- 契約充電枠外では、Teslaが充電中でない場合に残留A設定を安全側へ戻します。

## 今後の分割候補

現状は公開しやすいよう1ファイルを維持しています。長期的には次の分割がおすすめです。

```text
smart_hems/
├── app.py
├── config.py
├── state.py
├── wall_connector.py
├── tesla_control.py
├── battery_control.py
├── forecast.py
├── influx.py
└── templates/
```

制御ロジックを分けると、テストしやすくなってGitHub Issueでも追いやすくなります。


## ネットワーク分離の推奨

- READ VM → POST VM: TCP 5000など、POST API用ポートのみ許可
- POST VM → Tesla/Fleet/蓄電池: 必要な宛先のみ許可
- POST VMのTesla/Fleet tokenは `.env` やsystemd EnvironmentFileで管理し、Gitへ入れない
- READ VMのUIはLAN/VPN内限定にする
- まずREAD VMの読み取り専用モードで1日ログを取り、意図しないPOSTが飛ばないことを確認する

## 障害時の考え方

- POST VM停止: READ VMのUI・監視・予測は継続できるが、実機制御は失敗/抑止される
- READ VM停止: 自動判断は止まる。POST VM単体では勝手に制御判断しない構成が安全
- Fleet API失敗: Wall Connector実測値とlast-known cacheで表示・安全側判断を継続する
- Wall Connector取得失敗: Tesla自宅判定が弱くなるため、Teslaコマンドは抑止寄りにする

## Full VM Layout

```text
                 ┌──────────────────────────┐
                 │ ECHONET Lite devices      │
                 │ solar / battery / meter   │
                 └─────────────┬────────────┘
                               │
                               ▼
                 ┌──────────────────────────┐
                 │ koizuka/echonet-list      │
                 │ WebSocket provider        │
                 └─────────────┬────────────┘
                               │ WebSocket
                               ▼
┌────────────────────────────────────────────────────────┐
│ READ VM                                                 │
│ smart_hems_app.py                                       │
│ - UI / monitoring                                       │
│ - forecast / decision logic                             │
│ - Wall Connector local API read                         │
│ - last-known Tesla SOC/range cache                      │
└─────────────┬──────────────────────────────────────────┘
              │ HTTP: /api/status /api/command /api/tesla/fleet_state
              ▼
┌────────────────────────────────────────────────────────┐
│ POST VM                                                 │
│ post_vm/control_proxy.py                                │
│ - Tesla/Fleet scripts                                   │
│ - battery mode commands via echonet-list WebSocket      │
│ - webhook notifications                                 │
│ - local config and tokens                               │
└────────────────────────────────────────────────────────┘
```

READ VMは判断と表示、POST VMは実行という境界です。POST VMのAPIはREAD VMからのアクセスだけを許可し、インターネットには公開しません。


## Tesla operation adapters

POST VMはTesla操作を `post_vm/tesla_ops/` の4スクリプトへ分離しています。公開版ではdry-runアダプタを同梱し、実運用時はPOST VM内のプライベート実装へ委譲します。

```text
READ VM
  ↓ HTTP /api/command
POST VM control_proxy.py
  ↓ subprocess
post_vm/tesla_ops/*.py
  ↓ optional local private command
private Tesla/Fleet implementation outside Git
```

この分離により、GitHub側にはTesla/Fleet token、vehicle ID、VIN、refresh tokenを含めずに構成を共有できます。
