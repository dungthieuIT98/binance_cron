import requests
from datetime import datetime

# Danh sách symbols cần theo dõi
SYMBOLS = [
    "BTC",
    "ETH",
    "BNB",
    "SOL",
    "LINK","UNI","TON","DOT","AAVE","LTC"
]

def fetch_klines(symbol: str, interval: str = '1d', limit: int = 200):
    """
    Lấy dữ liệu klines từ CryptoCompare API và trả về list of dicts.
    Mỗi dict gồm: timestamp, open, high, low, close, volume, symbol
    Hỗ trợ các interval: 1h, 4h, 1d
    """
    
    # Map interval sang endpoint và aggregate value
    if interval == '1h':
        endpoint = 'histohour'
        aggregate = 1
    elif interval == '4h':
        endpoint = 'histohour'
        aggregate = 4
    elif interval == '1d':
        endpoint = 'histoday'
        aggregate = 1
    else:
        endpoint = 'histohour'
        aggregate = 1
    
    base_url = f"https://min-api.cryptocompare.com/data/v2/{endpoint}"
    
    params = {
        'fsym': symbol,
        'tsym': 'USDT',
        'limit': limit,
        'aggregate': aggregate
    }
    
    try:
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        raw_data = data['Data']['Data']
        
        # Chuyển đổi sang format mong muốn
        result = []
        for candle in raw_data:
            result.append({
                'timestamp': datetime.fromtimestamp(candle['time']).strftime('%Y-%m-%d %H:%M:%S'),
                'open': float(candle['open']),
                'high': float(candle['high']),
                'low': float(candle['low']),
                'close': float(candle['close']),
                'volume': float(candle['volumeto']),  # Volume in USDT
                'symbol': symbol + 'USDT'
            })
        return result
        
    except requests.exceptions.Timeout:
        print(f" Timeout khi lấy dữ liệu {symbol}")
        return []
    except requests.exceptions.RequestException as e:
        print(f" Lỗi request cho {symbol}: {e}")
        return []
    except KeyError as e:
        print(f" Lỗi parse data cho {symbol}: {e}")
        return []
    except Exception as e:
        print(f" Lỗi không xác định cho {symbol}: {e}")
        return []