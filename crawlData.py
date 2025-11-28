import ccxt
from datetime import datetime

SYMBOLS = [
    "BTC/USDT",
    "ETH/USDT",
    "XRP/USDT",
    "BNB/USDT",
    "SOL/USDT",
    "TRX/USDT",
    "DOGE/USDT",
    "ADA/USDT",
    "BCH/USDT",
    "LINK/USDT",
    "XLM/USDT"
]

# Khởi tạo exchange
exchange = ccxt.binance({
    'enableRateLimit': True,  # Tự động xử lý rate limit
    'options': {
        'defaultType': 'spot',
    }
})

def fetch_klines(symbol: str, interval: str = '1d', limit: int = 500):
    """
    Lấy dữ liệu klines (OHLCV) từ Binance qua thư viện ccxt và trả về list of dicts.
    Mỗi dict gồm: timestamp, open, high, low, close, volume, symbol
    Hỗ trợ các interval: 1m, 5m, 15m, 30m, 1h, 2h, 4h, 1d
    """
    # Lấy dữ liệu OHLCV
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe=interval, limit=limit)
    # Chuyển đổi sang format mong muốn
    result = []
    for candle in ohlcv:
        result.append({
            'timestamp': datetime.fromtimestamp(candle[0] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
            'open': float(candle[1]),
            'high': float(candle[2]),
            'low': float(candle[3]),
            'close': float(candle[4]),
            'volume': float(candle[5]),
            'symbol': symbol.replace('/', '')  # BTC/USDT -> BTCUSDT
        })
    return result