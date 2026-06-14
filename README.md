# Smart HEMS Controller

太陽光・蓄電池・Tesla Wall Connector・Tesla/Fleet情報を組み合わせて、家庭内の電力利用を見える化・制御する Flask ベースの HEMS コントローラーです。

> ⚠️ このアプリは実機の蓄電池やTesla充電制御へコマンドを送れる構成です。公開版では秘密情報を環境変数へ退避していますが、実運用では必ず読み取り専用モードや小さい電流設定から検証してください。ブレーカーは本番のCI/CDより容赦ないです。

## できること

- ECHONET Lite / WebSocket 経由の太陽光・蓄電池・買電/売電状態の監視
- Tesla Wall Connector Local API の実測値監視
- Fleet API 取得済みデータによる Tesla 充電・空調負荷の分類
- 契約上の充電優先時間帯に合わせた Tesla 充電制御
- 蓄電池の Day/Night Target 調整
- Open-Meteo + pvlib による簡易発電予測
- InfluxDB へのメトリクス保存
- Flask UI / JSON API

## 構成

この公開版は、元環境と同じく **READ VM / POST VM 分離構成** を前提にしています。

- **READ VM**: このリポジトリの `smart_hems_app.py` を動かすVMです。HEMS・ECHONET Lite由来データ・Wall Connector・発電予測・UI・制御判断を担当します。
- **POST VM**: 蓄電池操作・Tesla操作・Fleet API取得を受け持つVMです。秘密情報やOAuth/token類はPOST VM側に閉じ込めます。

READ VMはPOST VMのHTTP APIへ `GET /api/status`、`POST /api/command`、`GET /api/tesla/fleet_state` を呼び出します。これにより、公開リポジトリ側へTesla/Fleet認証情報を含めずに済みます。

POST VMには `post_vm/tesla_ops/` の安全なTesla操作アダプタを同梱しています。初期状態はdry-runで、実車操作を行う場合は `.env` の `TESLA_*_COMMAND` からPOST VM内のプライベート実装へ委譲してください。

### なぜREADとPOSTを分けているか

この構成は単なる好みではなく、安全側に倒すための設計です。

- **秘密情報の隔離**: Tesla/Fleet token、蓄電池操作に必要な情報、OAuth関連情報をPOST VM側へ閉じ込めます。READ VM側をGitHub公開・頻繁な改修対象にしても、操作系の秘密情報を含めない構成にできます。
- **実機操作面の最小化**: READ VMは「読む・判断する」役、POST VMは「実コマンドを送る」役に分けます。POST VMのAPIをREAD VMからだけ許可すれば、Teslaや蓄電池へ直接届く経路を絞れます。
- **障害時の安全性**: READ VMが落ちてもPOST VMの認証情報は露出しにくく、POST VMが落ちた場合もREAD VMは監視・UI・ログを継続できます。
- **公開しやすさ**: GitHubへ出すREAD側コードから、家庭固有の操作APIやtoken処理を切り離せます。
- **保守しやすさ**: 監視/UI/予測ロジックと、Tesla/Fleet/蓄電池の実操作ロジックを別々に直せます。

つまり、READ VMは「頭脳と画面」、POST VMは「手」。手に鍵を持たせて、頭脳側のコードは公開しやすくする構成です。

このリポジトリには、1ファイル構成のREAD側アプリ本体と、再利用のための設定テンプレート・セットアップ手順を含めています。

```text
.
├── smart_hems_app.py
├── requirements.txt
├── .env.example
├── .gitignore
├── THIRD_PARTY_NOTICES.md
├── samples/
│   └── smart_hems_settings.example.json
└── docs/
    ├── SETUP.md
    ├── ARCHITECTURE.md
    └── PUBLISHING_CHECKLIST.md
```

## VM分離時の最短設定

READ VM側の `.env` では、POST VMのIP/ポートを指定します。

```env
POST_CONTROL_BASE_URL=http://<post-vm-ip>:5000
FLEET_STATE_ENDPOINT=http://<post-vm-ip>:5000/api/tesla/fleet_state
```

同一VMで試す場合だけ `127.0.0.1` を使えます。VMを分ける本番構成では、ここをREAD VM自身に向けると制御が自己ループしてしまうので注意してください。

## 最短起動

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# .env を自宅環境に合わせて編集

python smart_hems_app.py
```

ブラウザで `http://<server-ip>:5000/` を開きます。

## 重要な公開前メモ

この公開用版では、トークン・自宅LAN IP・緯度経度・個人識別子を環境変数化しています。元ファイルに含まれていた InfluxDB token は **必ず再発行・失効** してください。GitHubに一度でも載せた秘密情報は「消したからOK」ではなく「漏れた前提でローテーション」が安全です。

## 第三者成果物・クレジット

このアプリは、HEMS/ECHONET Lite情報をWebSocketで配信する外部プロセスとして `koizuka/echonet-list` を利用する想定です。`echonet-list` は `@koizuka` さんによる ECHONET Lite device discovery/control tool で、MIT License で公開されています。

本リポジトリでは `echonet-list` のソースコードや生成物を同梱せず、READ VMが `HEMS_WS_URL` 経由で外部プロセスへ接続する構成にしています。利用関係・ライセンス・著作権表示は `THIRD_PARTY_NOTICES.md` に明記しています。

将来、`echonet-list` のコード・設定例・生成物・機器一覧・EPC定義などをこのリポジトリへコピーする場合は、MIT Licenseの著作権表示とライセンス文を削らないでください。

## ライセンス

MIT License

## READ VM / POST VM を含む公開構成

このリポジトリには、READ VM側の `smart_hems_app.py` に加えて、POST VM側の公開用サンプル `post_vm/control_proxy.py` も含めています。

```text
ECHONET Lite devices
  ↓
koizuka/echonet-list
  ↓ WebSocket
READ VM: smart_hems_app.py
  ↓ HTTP
POST VM: post_vm/control_proxy.py
  ↓
Tesla/Fleet scripts, battery mode commands
```

POST VMは、Tesla/Fleet token、webhook、蓄電池制御対象など、より機微な情報を扱います。実環境値は `.env` とローカル `config.json` に置き、GitHubへは入れないでください。

また、契約上の充電制御枠は `PRIMARY_CHARGE_WINDOW` / `SOLAR_ASSIST_CHARGE_WINDOW` として抽象化しています。公開READMEやソースコメントでは具体的な時刻を前面に出さず、利用者が自分の契約に合わせてローカル設定する形にしています。
