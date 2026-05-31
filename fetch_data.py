#!/usr/bin/env python3
"""MEXC 先物データを取得し data.json に書き出す。 GitHub Actions cron で 5 分毎に実行。"""

import json
import math
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib.request
import urllib.error

API_BASE = 'https://contract.mexc.com/api/v1/contract'
TIMEFRAMES = [
    {'interval': 'Min5',  'label': '5分足',  'minutes': 5,  'periods_per_year': 105120},
    {'interval': 'Min15', 'label': '15分足', 'minutes': 15, 'periods_per_year': 35040},
    {'interval': 'Min60', 'label': '1時間足', 'minutes': 60, 'periods_per_year': 8760},
]
TOP_N = 10
KLINE_LIMIT = 30
VOL_WINDOW = 20
MAX_SYMBOLS = 80

def fetch_json(url, retries=3, timeout=15):
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'mexc-vol-tracker/1.0'})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode('utf-8'))
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            if i == retries - 1:
                raise
            time.sleep(0.5 * (i + 1))
    return None

def fetch_klines(symbol, interval):
    minutes = next(tf['minutes'] for tf in TIMEFRAMES if tf['interval'] == interval)
    end = int(time.time())
    start = end - minutes * 60 * (KLINE_LIMIT + 5)
    url = f'{API_BASE}/kline/{symbol}?interval={interval}&start={start}&end={end}'
    try:
        j = fetch_json(url)
        if not j or not j.get('success'):
            return None
        return j['data']
    except Exception as e:
        print(f'  WARN klines {symbol} {interval}: {e}')
        return None

def calc_volatility(kline_data, periods_per_year):
    closes = kline_data.get('close', [])
    if len(closes) < VOL_WINDOW + 1:
        return None
    recent = [float(c) for c in closes[-(VOL_WINDOW + 1):]]
    rets = []
    for i in range(1, len(recent)):
        if recent[i-1] <= 0:
            continue
        rets.append(math.log(recent[i] / recent[i-1]))
    if len(rets) < VOL_WINDOW:
        return None
    mean = sum(rets) / len(rets)
    var = sum((r - mean)**2 for r in rets) / len(rets)
    std = math.sqrt(var)
    return std * math.sqrt(periods_per_year) * 100

def calc_atr_pct(kline_data):
    high = kline_data.get('high', [])
    low = kline_data.get('low', [])
    close = kline_data.get('close', [])
    if len(high) < VOL_WINDOW or len(close) == 0:
        return None
    n = min(VOL_WINDOW, len(high))
    ranges = []
    for i in range(n):
        try:
            h = float(high[-1 - i])
            l = float(low[-1 - i])
            ranges.append(h - l)
        except (ValueError, IndexError):
            continue
    if not ranges:
        return None
    avg_range = sum(ranges) / len(ranges)
    last_close = float(close[-1])
    return (avg_range / last_close) * 100 if last_close > 0 else None

def fetch_metrics_for_symbol(symbol, interval, periods_per_year):
    k = fetch_klines(symbol, interval)
    if not k:
        return None
    vol = calc_volatility(k, periods_per_year)
    atr = calc_atr_pct(k)
    if vol is None or math.isnan(vol):
        return None
    return {'symbol': symbol, 'vol': round(vol, 3), 'atr': round(atr, 3) if atr else None}

def main():
    started = time.time()
    print(f'[{datetime.utcnow().isoformat()}Z] MEXC データ取得開始')

    # 1. 全ティッカー
    j = fetch_json(f'{API_BASE}/ticker')
    if not j or not j.get('success'):
        raise RuntimeError('ticker 取得失敗')
    all_tickers = [t for t in j['data'] if t.get('symbol', '').endswith('_USDT')]
    all_tickers.sort(key=lambda t: float(t.get('amount24', 0) or 0), reverse=True)
    print(f'  全 USDT 銘柄: {len(all_tickers)}')

    ticker_map = {t['symbol']: t for t in all_tickers}
    top_symbols = [t['symbol'] for t in all_tickers[:MAX_SYMBOLS]]

    # 2. BTC/ETH の klines
    major_klines = {}
    for sym in ['BTC_USDT', 'ETH_USDT']:
        major_klines[sym] = {}
        for tf in TIMEFRAMES:
            k = fetch_klines(sym, tf['interval'])
            if k:
                # close と high と low のみ保管（軽量化）
                major_klines[sym][tf['interval']] = {
                    'close': [float(c) for c in k.get('close', [])][-30:],
                }
            time.sleep(0.1)
    print('  BTC/ETH klines 完了')

    # 3. 各 TF × 80 銘柄の vol 計算（並列）
    vol_by_tf = {}
    for tf in TIMEFRAMES:
        print(f'  {tf["label"]} 計算中 ({len(top_symbols)} 銘柄)...')
        results = []
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {
                executor.submit(fetch_metrics_for_symbol, sym, tf['interval'], tf['periods_per_year']): sym
                for sym in top_symbols
            }
            for fut in as_completed(futures):
                r = fut.result()
                if r:
                    results.append(r)
        vol_by_tf[tf['interval']] = sorted(results, key=lambda x: x['vol'], reverse=True)
        print(f'    完了 ({len(results)}/{len(top_symbols)} 計算成功)')

    # 4. 集計
    averages = {}
    top_lists = {}
    for tf in TIMEFRAMES:
        data = vol_by_tf[tf['interval']]
        if data:
            avg = sum(x['vol'] for x in data) / len(data)
            averages[tf['interval']] = round(avg, 2)
            top_lists[tf['interval']] = data[:TOP_N]
        else:
            averages[tf['interval']] = None
            top_lists[tf['interval']] = []

    # 5. マルチ時間軸合致
    top_20_sets = {tf['interval']: set(x['symbol'] for x in vol_by_tf[tf['interval']][:20])
                   for tf in TIMEFRAMES}
    if all(top_20_sets.values()):
        agreement = list(
            top_20_sets['Min5'] & top_20_sets['Min15'] & top_20_sets['Min60']
        )
        agreement_data = []
        for sym in agreement:
            v_map = {tf['interval']: next(
                (x['vol'] for x in vol_by_tf[tf['interval']] if x['symbol'] == sym), None
            ) for tf in TIMEFRAMES}
            agreement_data.append({'symbol': sym, 'vols': v_map})
        agreement_data.sort(key=lambda x: x['vols'].get('Min5', 0) or 0, reverse=True)
    else:
        agreement_data = []

    # 6. 急騰急落
    gainers = sorted(all_tickers, key=lambda t: float(t.get('riseFallRate', 0) or 0), reverse=True)[:15]
    losers = sorted(all_tickers, key=lambda t: float(t.get('riseFallRate', 0) or 0))[:15]

    # 6.5. ロング/ショート シグナル生成（複合スコアリング）
    # 各 symbol の metrics を統合
    sym_metrics = {}
    for tf_key in ['Min5', 'Min15', 'Min60']:
        for item in vol_by_tf.get(tf_key, []):
            sym = item['symbol']
            if sym not in sym_metrics:
                sym_metrics[sym] = {}
            sym_metrics[sym][f'vol_{tf_key}'] = item['vol']
            sym_metrics[sym][f'atr_{tf_key}'] = item.get('atr')

    agreement_syms = set(item['symbol'] for item in agreement_data)

    def score_long_short(sym, m, ticker):
        """LONG / SHORT スコア + 期待値 % を計算"""
        if sym not in ticker:
            return None
        t = ticker[sym]
        price = float(t.get('lastPrice', 0) or 0)
        rise24 = float(t.get('riseFallRate', 0) or 0) * 100  # %
        fr = float(t.get('fundingRate', 0) or 0) * 100  # %
        vol24 = float(t.get('amount24', 0) or 0)
        atr_1h = m.get('atr_Min60')  # %
        atr_5m = m.get('atr_Min5')
        v5 = m.get('vol_Min5', 0) or 0
        v15 = m.get('vol_Min15', 0) or 0
        v60 = m.get('vol_Min60', 0) or 0

        # 必要データ不足はスキップ
        if not atr_1h or not v5 or not v15 or not v60 or price <= 0:
            return None

        # ─── LONG スコア (0-100) ───
        long_score = 0
        long_tags = []
        if 2 <= rise24 <= 8:
            long_score += 30; long_tags.append('uptrend')
        elif 0 <= rise24 < 2:
            long_score += 15; long_tags.append('weak-up')
        elif rise24 > 15:
            long_score -= 25; long_tags.append('overext')
        if fr < -0.02:
            long_score += 20; long_tags.append('short-crowded')
        elif fr < 0.02:
            long_score += 10
        if fr > 0.1:
            long_score -= 15
        if v5 > v15 > v60:  # 加速度: 短期ほど大
            long_score += 15; long_tags.append('accel')
        if vol24 > 100e6:
            long_score += 10; long_tags.append('liq+')
        elif vol24 < 20e6:
            long_score -= 15; long_tags.append('illiq')
        if sym in agreement_syms:
            long_score += 10; long_tags.append('multi-tf')

        # ─── SHORT スコア (0-100) ───
        short_score = 0
        short_tags = []
        if -8 <= rise24 <= -2:
            short_score += 30; short_tags.append('downtrend')
        elif -2 < rise24 <= 0:
            short_score += 15; short_tags.append('weak-down')
        elif rise24 < -15:
            short_score -= 25; short_tags.append('oversold')
        if fr > 0.05:
            short_score += 20; short_tags.append('long-crowded')
        if fr > 0.1:
            short_score += 15; short_tags.append('extreme-fr')
        if fr < -0.05:
            short_score -= 10
        if rise24 > 8 and fr > 0.05:  # 過熱 long 逆張り
            short_score += 20; short_tags.append('fade-pump')
        if v5 > v15:
            short_score += 10
        if vol24 > 100e6:
            short_score += 10; short_tags.append('liq+')
        elif vol24 < 20e6:
            short_score -= 15; short_tags.append('illiq')
        if sym in agreement_syms:
            short_score += 10; short_tags.append('multi-tf')

        # スコアを 0-100 にクリップ
        long_score = max(0, min(100, long_score))
        short_score = max(0, min(100, short_score))

        # ─── 期待値 % (1h horizon) ───
        # ATR(1h) は次 1h の平均レンジ → 方向確信度で reweight
        # conviction = score / 100 で 0-1 にスケール
        # conservative = atr/2 * conv / optimistic = atr * conv
        long_conv = long_score / 100
        short_conv = short_score / 100

        long_ev = {
            'conservative': round(atr_1h * 0.3 * long_conv, 2),
            'central':      round(atr_1h * 0.6 * long_conv, 2),
            'optimistic':   round(atr_1h * 1.0 * long_conv, 2),
        }
        short_ev = {
            'conservative': round(-atr_1h * 0.3 * short_conv, 2),
            'central':      round(-atr_1h * 0.6 * short_conv, 2),
            'optimistic':   round(-atr_1h * 1.0 * short_conv, 2),
        }

        return {
            'symbol': sym,
            'long_score': long_score,
            'short_score': short_score,
            'long_ev': long_ev,
            'short_ev': short_ev,
            'long_tags': long_tags,
            'short_tags': short_tags,
            'atr_1h': round(atr_1h, 2),
            'rise24': round(rise24, 2),
            'fr': round(fr, 4),
        }

    signals = []
    for sym, m in sym_metrics.items():
        s = score_long_short(sym, m, ticker_map)
        if s is not None:
            signals.append(s)

    # 閾値 30 以上を採択（少なくとも 2-3 ポジティブ要因あり）
    long_top = sorted(
        [s for s in signals if s['long_score'] >= 30],
        key=lambda x: (x['long_score'], x['long_ev']['central']),
        reverse=True
    )[:10]
    short_top = sorted(
        [s for s in signals if s['short_score'] >= 30],
        key=lambda x: (x['short_score'], abs(x['short_ev']['central'])),
        reverse=True
    )[:10]
    print(f'  signals computed: total={len(signals)}, long(>=30)={len(long_top)}, short(>=30)={len(short_top)}')
    if signals:
        max_l = max(signals, key=lambda x: x['long_score'])
        max_s = max(signals, key=lambda x: x['short_score'])
        print(f'    max long: {max_l["symbol"]} score={max_l["long_score"]}')
        print(f'    max short: {max_s["symbol"]} score={max_s["short_score"]}')

    # 7. 軽量化したティッカー情報（symbol -> { price, change24, fr, oi, vol24, next_fr }）
    def slim_ticker(t):
        price = float(t.get('lastPrice', 0) or 0)
        return {
            'symbol': t['symbol'],
            'price': price,
            'rise24': float(t.get('riseFallRate', 0) or 0),
            'fr': float(t.get('fundingRate', 0) or 0),
            'oi': float(t.get('holdVol', 0) or 0) * price,
            'vol24': float(t.get('amount24', 0) or 0),
            'next_fr_time': int(t.get('nextSettleTime', 0) or 0),
        }

    ticker_slim_map = {sym: slim_ticker(ticker_map[sym]) for sym in
                       set([t['symbol'] for t in top_symbols if False] + top_symbols +
                           [g['symbol'] for g in gainers] +
                           [l['symbol'] for l in losers] +
                           ['BTC_USDT', 'ETH_USDT'])
                       if sym in ticker_map}

    result = {
        'timestamp': int(time.time()),
        'updated_at': datetime.utcnow().isoformat() + 'Z',
        'meta': {
            'total_symbols': len(all_tickers),
            'computed_symbols': len(top_symbols),
            'vol_window': VOL_WINDOW,
            'top_n': TOP_N,
        },
        'tickers': ticker_slim_map,
        'major_klines': major_klines,
        'top_by_tf': top_lists,
        'averages': averages,
        'agreement': agreement_data,
        'gainers': [slim_ticker(t) for t in gainers],
        'losers': [slim_ticker(t) for t in losers],
        'signals': {
            'long': long_top,
            'short': short_top,
            'methodology': '24h trend + funding rate + multi-TF vol acceleration + liquidity を統合した複合スコア (0-100)。 期待値 = ATR(1h) × 確信度係数。',
        },
        'runtime_sec': round(time.time() - started, 1),
    }

    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, separators=(',', ':'))

    elapsed = time.time() - started
    print(f'完了 ({elapsed:.1f}s) -> data.json ({len(json.dumps(result))} bytes)')

if __name__ == '__main__':
    main()
