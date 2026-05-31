# MEXC 先物 デイトレード スキャナー

MEXC 永久先物市場の **デイトレード判断に必要な情報をワンクリックで取得** するシングルページサイト。

**公開 URL**: https://harukiman.github.io/mexc-volatility-tracker/

## 機能（v2）

### スキャナータブ
- **5 分足 / 15 分足 / 1 時間足 × Top 10 銘柄**: 年率換算ボラ + ATR + 24h 変動 + FR + 出来高 + OI
- **全銘柄平均ボラティリティ**: 計算対象（上位 80 銘柄）の平均
- **BTC / ETH 詳細カード**: 価格 + 24h/5m/15m/1h 方向 + FR + 次 FR 時刻 + OI + 24h 出来高
- **タグ表示**: 急騰 / 急落 / 低流動 / FR 極端 を視覚的に強調

### 急騰急落タブ
- **24h ゲイナー Top 15**: 全銘柄から上昇率順
- **24h ルーザー Top 15**: 全銘柄から下落率順
- 各行に FR / 出来高 / OI 付き

### 使い方タブ（推奨パターン 5 種）
1. **ブレイクアウトモメンタム** （順張り、 5m ボラ Top + ゲイナー Top + 出来高フィルタ）
2. **ミーンリバージョン** （逆張り、 ルーザー Top + FR 極端負）
3. **BTC 連動回避 + alt 個別** （macro neutral 時の pure alpha 探し）
4. **平均ボラとの比較で動く時間帯検出** （セッション選び）
5. **Funding rate アービ** （±0.1% 超え + hedge）

加えて、 **避けるべきパターン** と **運用 tips** を明記。

## 表示データの定義

- **年率ボラ**: 直近 20 本の log return std × √(periods/year) × 100
- **ATR**: 直近 20 本の (high - low) 平均 / 終値 × 100（％ベース）
- **FR**: funding rate（永久先物の保有コスト）。 正 = long が short に支払う
- **OI**: open interest（建玉総額 USD）= holdVol × price
- **出来高**: 24h amount24（USD ベース）

## デプロイ

GitHub Pages 自動公開:
- URL: https://harukiman.github.io/mexc-volatility-tracker/
- 単一 `index.html`。 バックエンドなし。

## ローカル動作

```bash
open index.html
# または
python3 -m http.server 8000
# → http://localhost:8000/
```

## 制限事項

- 計算対象は出来高上位 80 銘柄（全銘柄 ~400 のうち）。 API レート制限考慮
- 取得時間: 約 40〜60 秒
- 急騰急落タブのみ全銘柄ベース（ticker 一括取得で 1 リクエスト）
- ブラウザのキャッシュは無効化（`Cache-Control: no-cache`）

## データソース

- MEXC 公開 API: `https://contract.mexc.com/api/v1/contract/`
  - `/ticker`: 24h スナップショット（lastPrice、 riseFallRate、 fundingRate、 holdVol、 amount24、 nextSettleTime 等）
  - `/kline/{symbol}`: 時系列 OHLCV
- CORS 許可済、 ブラウザ直接コール可

## 監査履歴（v1 → v2 改善点）

| 観点 | v1 | v2 |
|---|---|---|
| funding rate 表示 | なし | BTC/ETH カード + 全 Top 10 表 |
| open interest 表示 | なし | 全テーブル |
| 出来高表示 | なし | 全テーブル + 低流動警告タグ |
| ATR 指標 | なし | ボラ Top 10 表 |
| 急騰急落タブ | なし | Top 15 ゲイナー + ルーザー |
| 使い方ガイド | なし | 5 パターン + 避けるべきパターン + 運用 tips |
| タグ表示 | なし | 急騰 / 急落 / 低流動 / FR 極端 |
| 次 FR 時刻 | なし | BTC/ETH カードに表示 |
| 大型銘柄カード | プレーン | 価格 + 4 時間軸変動 + FR + OI |
