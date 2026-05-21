# PRSK Supporter Bot

プロセカイベントの支援者向け Discord bot。ランキング自動取得・スプレッドシート記録・シフト管理を行う。

## 構成

```
prsk_supporter/
├── compose.yml
├── app/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── .env                # 要作成（後述）
│   ├── keys/               # Google サービスアカウントキー（要配置）
│   │   └── *.json
│   └── src/
│       └── main.py 他
```

設定は `/setup` コマンドで各 Discord サーバーごとにスプレッドシート ID を渡す。設定は `config_store.json`（コンテナ内 `/app/config_store.json`）に JSON で永続化される。

## セットアップ

### 1. 環境変数ファイルの作成

`app/.env` を作成する。

```env
DISCORD_TOKEN=<Discord Bot Token>
SERVICE_ACCOUNT_KEY=/app/keys/<サービスアカウントキーファイル名>.json
```

| 変数名 | 必須 | 説明 |
|---|---|---|
| `DISCORD_TOKEN` | ✅ | Discord Developer Portal で取得した Bot トークン |
| `SERVICE_ACCOUNT_KEY` | ✅ | Google サービスアカウントキー JSON のコンテナ内パス |
| `INSTANCE_ID` | | 複数インスタンス運用時の重複実行防止用 ID（省略時は起動ごとに UUID 自動生成） |
| `ENABLE_MESSAGE_CONTENT` | | `1` にするとメッセージ内容 intent を有効化。Privileged Intent の許可が別途必要 |

### 2. Google サービスアカウントキーの配置

Google Cloud Console でサービスアカウントを作成し、キー JSON を `app/keys/` 以下に配置する。  
対象スプレッドシートにそのサービスアカウントのメールアドレスを編集者として共有しておく必要がある。

### 3. Discord Bot の設定

Discord Developer Portal で以下を確認する。

- **Bot** タブ: `MESSAGE CONTENT INTENT` は通常不要（使う場合は `.env` で `ENABLE_MESSAGE_CONTENT=1` も設定）
- **OAuth2** タブ: 招待 URL のスコープに `bot` と `applications.commands` の両方を含める

### 4. 起動

```bash
docker compose up -d
```

`compose.yml` は `app/src` と `app/.env` をボリュームマウントしているため、ソース変更は `watchfiles` によって自動検知・再起動される。

## データ永続化

`config_store.json` はコンテナ内の `/app/` 直下に生成される。コンテナを作り直すと消えるため、必要であれば `compose.yml` にボリュームを追加する。

```yaml
volumes:
  - ./app/src:/app/src
  - ./app/.env:/app/.env:ro
  - ./app/keys:/app/keys:ro
  - ./app/config_store.json:/app/config_store.json  # 追加
```

## ログ

`compose.yml` の設定により json-file ドライバでローテーション管理される（最大 10MB × 3 世代）。

```bash
docker compose logs -f
```

## スプレッドシート設定項目

各 Discord サーバーで `/setup <スプレッドシートID>` を実行すると、スプレッドシートの設定シートを読み込む。主な設定項目：

| キー | 説明 |
|---|---|
| `EventName` | イベント名（部分一致で検索） |
| `EventID` | イベント ID（`EventName` 未設定時に使用） |
| `EventStart` / `EventEnd` | イベント開始・終了日時（API 取得できない場合に使用、ISO 8601） |
| `ChapterNo` | WL チャプター番号（通常イベントは省略） |
| `Trackings` | ランキング記録対象のユーザー名またはランク順位のリスト |
| `Focus` | 差分表示するランク順位 |
| `Runners` | ランナーのリスト（シフトシートの列数計算に使用） |
| `LogInterval` | ランキング取得間隔（分、デフォルト 60） |
| `ChangeNotice` | シフト通知を送る分（例: `55`） |
| `NextServer` | 次サーバー情報を送る分 |
