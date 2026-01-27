RSI_PERIOD = 14


def calculate_ma(prices, period):
    """Tính toán MA (Simple Moving Average - Đường trung bình động đơn giản)"""
    if len(prices) < period:
        return []
    
    ma = []
    for i in range(period - 1, len(prices)):
        window = prices[i - period + 1:i + 1]
        ma.append(sum(window) / period)
    
    return ma


def calculate_volume_avg(volumes, period=20):
    """Tính toán trung bình volume trong n ngày"""
    if len(volumes) < period:
        return []
    
    vol_avg = []
    for i in range(period - 1, len(volumes)):
        window = volumes[i - period + 1:i + 1]
        vol_avg.append(sum(window) / period)
    
    return vol_avg


def calculate_ema(prices, period):
    """Tính toán EMA (Exponential Moving Average)"""
    if len(prices) < period:
        return []
    k = 2 / (period + 1)
    ema = [sum(prices[:period]) / period]
    for price in prices[period:]:
        ema.append(price * k + ema[-1] * (1 - k))
    return ema

def calculate_rsi(prices, period=RSI_PERIOD):
    """Tính toán RSI (Relative Strength Index)"""
    if len(prices) <= period:
        return []
    
    changes = [b - a for a, b in zip(prices, prices[1:])]
    gains = [max(c, 0) for c in changes]
    losses = [-min(c, 0) for c in changes]
    
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    rsi = []
    
    for gain, loss in zip(gains[period:], losses[period:]):
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        rs = avg_gain / avg_loss if avg_loss else 0
        rsi.append(100 - 100 / (1 + rs) if avg_loss else 100)
    
    return rsi

def calculate_macd(prices):
    """Tính toán MACD (Moving Average Convergence Divergence)"""
    ema12 = calculate_ema(prices, 12)
    ema26 = calculate_ema(prices, 26)
    
    if not ema12 or not ema26:
        return [], [], []
    
    macd = [a - b for a, b in zip(ema12[-len(ema26):], ema26)]
    signal = calculate_ema(macd, 9)
    hist = [a - b for a, b in zip(macd[-len(signal):], signal)] if signal else []
    return macd, signal, hist

def process_file(data,periods=(20, 50, 90), ma_volume_period=20):
    """Tính MA, EMA, RSI, MACD, Volume Average trên dữ liệu trong memory, không ghi CSV"""
    if not data or "close" not in data[0]:
        print("Dữ liệu không hợp lệ hoặc thiếu cột 'close'")
        return data  # Return data ngay cả khi không xử lý được

    closes = [float(r["close"]) for r in data]
    volumes = [float(r.get("volume", 0)) for r in data]
    ma = {p: calculate_ma(closes, p) for p in periods}
    ema = {p: calculate_ema(closes, p) for p in periods}
    rsi = calculate_rsi(closes)
    macd, sig, hist = calculate_macd(closes)
    vol_avg = calculate_volume_avg(volumes, ma_volume_period)

    n = len(data)
    for i, row in enumerate(data):
        # MA
        for p in periods:
            m = ma[p]
            row[f"ma_{p}"] = f"{m[i - (n - len(m))]:.2f}" if i >= n - len(m) else ""
        # EMA
        for p in periods:
            e = ema[p]
            row[f"ema_{p}"] = f"{e[i - (n - len(e))]:.2f}" if i >= n - len(e) else ""
        # RSI
        if i >= n - len(rsi):
            row["rsi14"] = f"{rsi[i - (n - len(rsi))]:.2f}"
        else:
            row["rsi14"] = ""
        # MACD
        if i >= n - len(macd):
            mi = i - (n - len(macd))
            row["macd"] = f"{macd[mi]:.2f}"
            if mi >= len(macd) - len(sig):
                si = mi - (len(macd) - len(sig))
                row["macd_signal"] = f"{sig[si]:.2f}"
                row["macd_histogram"] = f"{hist[si]:.2f}" if si < len(hist) else ""
            else:
                row["macd_signal"] = row["macd_histogram"] = ""
        else:
            row["macd"] = row["macd_signal"] = row["macd_histogram"] = ""
        
        # Volume Average
        if i >= n - len(vol_avg):
            vi = i - (n - len(vol_avg))
            current_vol = float(row.get("volume", 0))
            
            row["volume_ratio"] = f"{(current_vol / vol_avg[vi]):.2f}" if vol_avg[vi] > 0 else ""
        else:
            row["volume_ratio"] = ""
 
    return data

