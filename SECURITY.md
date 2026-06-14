# Security Policy

## Supported Versions

このリポジトリは個人利用のHEMS実験コードです。公開利用する場合は自己責任で検証してください。

## Reporting a Vulnerability

IssueまたはPull Requestで報告してください。ただし、公開Issueへトークン・IP・緯度経度・Webhook URLなどの秘密情報は貼らないでください。

## Runtime Secrets

以下は必ず環境変数または未追跡ファイルで管理してください。

- InfluxDB token
- Discord Webhook URL
- Tesla/Fleet API関連情報
- 自宅LAN IP
- 正確な緯度経度
- Grafana dashboard URL


## 第三者成果物の扱い

`koizuka/echonet-list` は外部プロセスとして利用し、このリポジトリには同梱しません。コード・生成物・設定・証明書・検出済みデバイス一覧を取り込む場合は、MIT Licenseの著作権表示とライセンス本文を保持し、`THIRD_PARTY_NOTICES.md` に明記してください。

ライセンス不明のコードやデータは、このリポジトリへ含めないでください。公開後に権利関係が不明な成果物が見つかった場合は、削除だけでなく、Git履歴やリリースアーカイブからも除去する必要がある場合があります。
