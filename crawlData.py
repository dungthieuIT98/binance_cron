import ccxt
import time
from datetime import datetime

# Danh sách symbols cần theo dõi
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


# Backup: Dùng các sàn khác không bị chặn
bybit = ccxt.bybit({
    'enableRateLimit': True,
    'options': {
        'defaultType': 'spot',
    }
})

okx = ccxt.okx({
    'enableRateLimit': True,
})

def fetch_klines(symbol: str, interval: str = '1d', limit: int = 500):
    """
    Lấy dữ liệu klines từ nhiều sàn (fallback nếu 1 sàn lỗi)
    """
    
    if limit > 1000:
        limit = 1000
    
    # Thử các exchange theo thứ tự ưu tiên: Binance -> Bybit -> OKX
    exchanges_to_try = []
    exchanges_to_try.extend([('bybit', bybit), ('okx', okx)])
    
    last_error = None
    
    for exchange_name, exchange in exchanges_to_try:
        if not exchange:
            continue
            
        try:
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
                    'symbol': symbol.replace('/', '')
                })
            
            return result
            
        except ccxt.BadSymbol:
            # Symbol không tồn tại trên sàn này, thử sàn khác
            last_error = f"Symbol không hợp lệ trên {exchange_name}"
            continue
        except ccxt.NetworkError as e:
            last_error = f"Lỗi mạng: {e}"
            continue
        except ccxt.ExchangeError as e:
            last_error = f"Lỗi exchange: {e}"
            # Nếu là lỗi 451 (geo-blocking), thử sàn khác
            if "451" in str(e):
                print(f"⚠️ {exchange_name.upper()} bị chặn (451), thử sàn khác...")
                continue
            continue
        except Exception as e:
            last_error = f"Lỗi không xác định: {e}"
            continue
    
    # Nếu tất cả sàn đều fail
    return []