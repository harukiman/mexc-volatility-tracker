# MEXC 先物 ボラティリティトラッカー

MEXC 永久先物市場の直近ボラティリティを 5 分足・15 分足・1 時間足で 1 クリック表示するシングルページサイト。

## 機能

- **ワンボタン取得**: 「データ取得」 ボタンで全てが更新
- **3 時間軸別 上位 10 銘柄**: 各時間軸での年率換算ボラティリティ Top 10
- **全銘柄平均ボラティリティ**: 計算対象（出来高上位 80 銘柄）の平均
- **BTC / ETH 大型カード**: 現在価格 + 24h / 5m / 15m / 1h の方向と変動率
- **ゼロバックエンド**: ブラウザから MEXC 公開 API を直接コール（CORS 許可確認済）

## 計算手法

- 各銘柄について、 各時間軸の klines 直近 21 本を取得
- log return の標準偏差 σ を計算
- 年率換算: σ × √(periods/year) × 100 = 年率ボラティリティ %
- periods/year: 5m = 105,120 / 15m = 35,040 / 1h = 8,760

## データソース

- MEXC 公開 API: `https://contract.mexc.com/api/v1/contract/`
- エンドポイント: `/ticker`（24h スナップショット）、 `/kline/{symbol}`（時系列）
- CORS 許可済、 ブラウザ直接コール可

## デプロイ

GitHub Pages で自動公開:
- URL: `https://harukiman.github.io/mexc-volatility-tracker/`
- 単一 `index.html` のみ。 サーバー不要。

## ローカル動作

```bash
open index.html
# または
python3 -m http.server 8000
# → http://localhost:8000/
```

## 制限事項

- 計算対象は出来高上位 80 銘柄（全銘柄 ~400 のうち）。 全件計算は API レート制限により非実用的
- 各時間軸ごとに 80 銘柄 × klines 取得 = 約 80 リクエスト
- 取得時間目安: 約 30〜60 秒
- ブラウザのキャッシュは無効化済（`Cache-Control: no-cache`）
