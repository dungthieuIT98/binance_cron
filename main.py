import pandas as pd
import requests
from datetime import datetime

# ============================================================================
# PHẦN 1: CRAWL DATA TỪ CRYPTOCOMPARE
# ============================================================================

def fetch_klines(symbol: str, interval: str = '1d', from_date: str = None, to_date: str = None, limit: int = 500):
    """
    Lấy dữ liệu klines từ Binance API và trả về list of dicts.
    Mỗi dict gồm: date, open, high, low, close, volume
    Hỗ trợ các interval: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 1h, 6h, 8h, 12h, 1d, 3d, 1w, 1M
    
    Args:
        symbol: Mã coin (VD: 'BTC', 'ETH') - sẽ tự động thêm USDT
        interval: Khung thời gian Binance ('1h', '1h', '1d', etc.)
        from_date: Ngày bắt đầu (format: 'YYYY-MM-DD' hoặc 'YYYY-MM-DD HH:MM:SS')
        to_date: Ngày kết thúc (format: 'YYYY-MM-DD' hoặc 'YYYY-MM-DD HH:MM:SS')
        limit: Số nến tối đa (mặc định 500, tối đa 1000)
    """
    
    base_url = "https://api.binance.com/api/v3/klines"
    
    # Tạo symbol pair
    symbol_pair = f"{symbol}USDT"
    
    params = {
        'symbol': symbol_pair,
        'interval': interval,
        'limit': min(limit, 1000)  # Binance limit max 1000
    }
    
    # Xử lý from_date và to_date
    if from_date:
        try:
            from_dt = datetime.strptime(from_date, '%Y-%m-%d' if len(from_date) == 10 else '%Y-%m-%d %H:%M:%S')
            params['startTime'] = int(from_dt.timestamp() * 1000)  # Binance dùng milliseconds
        except ValueError as e:
            print(f"⚠ Lỗi format from_date: {e}")
            print("   Sử dụng format: 'YYYY-MM-DD' hoặc 'YYYY-MM-DD HH:MM:SS'")
            return []
    
    if to_date:
        try:
            to_dt = datetime.strptime(to_date, '%Y-%m-%d' if len(to_date) == 10 else '%Y-%m-%d %H:%M:%S')
            params['endTime'] = int(to_dt.timestamp() * 1000)  # Binance dùng milliseconds
        except ValueError as e:
            print(f"⚠ Lỗi format to_date: {e}")
            print("   Sử dụng format: 'YYYY-MM-DD' hoặc 'YYYY-MM-DD HH:MM:SS'")
            return []
    
    try:
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        raw_data = response.json()
        
        # Chuyển đổi sang format mong muốn
        # Binance klines format: [timestamp, open, high, low, close, volume, close_time, ...]
        result = []
        for candle in raw_data:
            candle_date = datetime.fromtimestamp(candle[0] / 1000)  # Convert milliseconds to seconds
            
            result.append({
                'date': candle_date.strftime('%Y-%m-%d %H:%M:%S'),
                'open': float(candle[1]),
                'high': float(candle[2]),
                'low': float(candle[3]),
                'close': float(candle[4]),
                'volume': float(candle[5]),
            })
        
        return result
        
    except requests.exceptions.Timeout:
        print(f"⚠ Timeout khi lấy dữ liệu {symbol}")
        return []
    except requests.exceptions.RequestException as e:
        print(f"⚠ Lỗi request cho {symbol}: {e}")
        return []
    except (KeyError, IndexError) as e:
        print(f"⚠ Lỗi parse data cho {symbol}: {e}")
        return []
    except Exception as e:
        print(f"⚠ Lỗi không xác định cho {symbol}: {e}")
        return []

def update_btc_data(interval: str = '1d', from_date: str = None, to_date: str = None, limit: int = 500):
    """
    Crawl dữ liệu BTC và lưu vào file btc_1d.csv
    
    Args:
        interval: Khung thời gian ('1h', '1h', '1d', etc.)
        from_date: Ngày bắt đầu (format: 'YYYY-MM-DD')
        to_date: Ngày kết thúc (format: 'YYYY-MM-DD')
        limit: Số nến (nếu không dùng from_date/to_date)
    
    Ví dụ:
        update_btc_data(interval='1h', from_date='2025-03-01', to_date='2025-10-30', limit=2000)
        update_btc_data(interval='1d', limit=365)
    """
    print(f"📡 Đang crawl dữ liệu BTC khung {interval.upper()}...")
    
    if from_date and to_date:
        print(f"   Khoảng thời gian: {from_date} đến {to_date}")
        data = fetch_klines('BTC', interval=interval, from_date=from_date, to_date=to_date, limit=limit)
    else:
        print(f"   Lấy {limit} nến gần nhất")
        data = fetch_klines('BTC', interval=interval, limit=limit)
    
    if data:
        df = pd.DataFrame(data)
        df.to_csv('btc_1d.csv', index=False)
        print(f"✅ Đã lưu {len(df)} nến vào btc_1d.csv")
        print(f"   Từ: {df['date'].iloc[0]}")
        print(f"   Đến: {df['date'].iloc[-1]}")
        return True
    else:
        print("❌ Không thể crawl dữ liệu BTC")
        return False

# ============================================================================
# PHẦN 2: BACKTEST MACD
# ============================================================================

def backtest_macd(csv_file: str = 'btc_1d.csv'):
    """
    Thực hiện backtest chiến lược MACD
    """
    # Đọc dữ liệu
    df = pd.read_csv(csv_file)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)
    
    print(f"\n📊 Bắt đầu backtest với {len(df)} nến")
    
    # Tính MACD
    df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
    df['ema26'] = df['close'].ewm(span=26, adjust=False).mean()
    df['ema90'] = df['close'].ewm(span=90, adjust=False).mean()
    df['dif'] = df['ema12'] - df['ema26']
    df['dea'] = df['dif'].ewm(span=9, adjust=False).mean()
    df['macd'] = (df['dif'] - df['dea']) 
    
    # Tính RSI (14 periods)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))
    
    # Xác định tín hiệu
    dea_check = df['dea'] > 0
    df['buy_condition'] = (df['dif'] > df['dea']) & (df['macd'] > 0) & dea_check 
    df['buy_signal'] = df['buy_condition'] & (~df['buy_condition'].shift(1).fillna(False))
    df['sell_condition'] = (df['dif'] < df['dea']) 
    
    # Thực hiện backtest
    trades = []
    in_position = False
    buy_date = None
    buy_price = None
    
    # Bỏ qua 30 nến đầu để đảm bảo EMA26 + DEA + RSI đã ổn định
    start_index = 30
    
    print(f"   Bỏ qua {start_index} nến đầu để đảm bảo chỉ báo ổn định")
    print(f"   Backtest từ: {df.loc[start_index, 'date'].strftime('%Y-%m-%d %H:%M:%S')}")
    
    for i in range(start_index, len(df)):
        if not in_position and df.loc[i, 'buy_signal']:
            in_position = True
            buy_date = df.loc[i, 'date']
            buy_price = df.loc[i, 'close']
        
        elif in_position and df.loc[i, 'sell_condition']:
            sell_date = df.loc[i, 'date']
            sell_price = df.loc[i, 'close']
            profit_pct = ((sell_price - buy_price) / buy_price) * 100
            
            trades.append({
                'buy_date': buy_date,
                'buy_price': buy_price,
                'sell_date': sell_date,
                'sell_price': sell_price,
                'profit_pct': profit_pct
            })
            
            in_position = False
            buy_date = None
            buy_price = None
    
    # Tạo DataFrame lịch sử giao dịch
    trades_df = pd.DataFrame(trades)
    
    # In kết quả
    print("\n" + "=" * 80)
    print("LỊCH SỬ GIAO DỊCH MACD + RSI BACKTEST")
    print("=" * 80)
    
    if len(trades_df) > 0:
        # Format hiển thị
        trades_df['buy_price'] = trades_df['buy_price'].round(2)
        trades_df['sell_price'] = trades_df['sell_price'].round(2)
        trades_df['profit_pct'] = trades_df['profit_pct'].round(2)
        
        print(trades_df.to_string(index=False))
        print("\n" + "=" * 80)
        
        # Thống kê
        total_trades = len(trades_df)
        winning_trades = len(trades_df[trades_df['profit_pct'] > 0])
        win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
        avg_profit = trades_df['profit_pct'].mean()
        total_profit = trades_df['profit_pct'].sum()
        
        print(f"THỐNG KÊ:")
        print(f"  • Tổng số lệnh: {total_trades}")
        print(f"  • Số lệnh thắng: {winning_trades}")
        print(f"  • Số lệnh thua: {total_trades - winning_trades}")
        print(f"  • Tỷ lệ thắng: {win_rate:.2f}%")
        print(f"  • Lợi nhuận trung bình/lệnh: {avg_profit:.2f}%")
        print(f"  • Tổng lợi nhuận: {total_profit:.2f}%")
        print("=" * 80)
    else:
        print("Không có giao dịch nào được thực hiện.")
        print("=" * 80)

# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    # Bước 1: Update dữ liệu BTC khung H4
    success = update_btc_data(
        interval='1d',
        from_date='2025-03-15', 
        to_date='2025-10-28',
        limit=2000
    )
    
    # Bước 2: Chạy backtest nếu crawl thành công
    if success:
        backtest_macd('btc_1d.csv')
    else:
        print("\n❌ Không thể thực hiện backtest do không có dữ liệu")