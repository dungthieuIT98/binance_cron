RSI_PERIOD = 14

def get_trend_label(data):
    # Kiểm tra nến cuối có trend_score không
    last_candle = data[-2]
    # Lấy symbol và current score
    symbol = last_candle.get("symbol", "N/A")
    current_score = last_candle["trend_score"]
    current_rsi = last_candle.get("rsi14", "N/A")
    current_volume_ratio = last_candle.get("volume_ratio", "N/A")
    # Lấy danh sách old_scores của 6 ngày trước (từ -7 đến -2)
    old_scores = []
    old_rsis = []
    for i in range(7, 1, -1): 
        if len(data) >= i:
            candle = data[-i]
            if "trend_score" in candle and candle["trend_score"] != "":
                old_scores.append(str(candle["trend_score"]))
                rsi_value = candle.get("rsi14", None)
                rsi_value = float(rsi_value)
                rsi_value = round(rsi_value) 
                old_rsis.append(str(rsi_value))

    # Phân loại xu hướng    
    try:
        score_value = float(current_score) if isinstance(current_score, str) else current_score
    except:
        return ""
    
    if score_value >= 5:
        label = "<b>🔴 UPTREND mạnh</b>"
    elif score_value >= 3:
        label = "<b>Uptrend yếu</b>"
    elif score_value <= -5:
        label = "<b>🔴 DOWNTREND mạnh</b>"
    elif score_value <= -3:
        label = "<b>Downtrend yếu</b>"
    else:
         label = ""
    
    # check label có rỗng không
    if label:    
        # Format message với danh sách old_scores
        old_scores_str = ", ".join(old_scores) if old_scores else ""
        old_rsis_str = ", ".join(old_rsis) if old_rsis else ""
        label += f"\n ==>RSI: {old_rsis_str}\n ==> History: {old_scores_str}"

    if float(current_rsi) > 70 or float(current_rsi) < 30:
        label += f"\n<b>⚠️ Cảnh báo RSI: {current_rsi}</b>"
    if float(current_volume_ratio) > 2:
        label += f"\n<b>⚠️ Cảnh báo Volume cao: {current_volume_ratio}x</b>"

    if label:    
        return f"👉{symbol}: {label}\n"
    return ""

 
def score_trend(ema20, ema50, ema90, rsi, macd, signal):
    # EMA score
    if ema20 > ema50 > ema90:
        ema_score = 2
    elif ema90 < ema20< ema50 :
        ema_score = 1
    elif ema90> ema20 > ema50:
        ema_score = -1
    elif ema20 < ema50 < ema90:
        ema_score = -2
    else:
        ema_score = 0

    # RSI score
    if rsi > 60:
        rsi_score = 1
    elif rsi < 40:
        rsi_score = -1
    else:
        rsi_score = 0

    # MACD score
    macd_score = 0
    if macd > signal and macd > 0:
        macd_score = 2
    elif macd < signal and macd < 0:
        macd_score = -2
    elif macd > signal or macd < 0:
            macd_score = 1
    elif macd < signal or macd > 0:
            macd_score = -1
    else:
            macd_score = 0

    total = ema_score + rsi_score + macd_score  # range approx -10..+10

    return total

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

def process_file(data, symbol, periods=(20, 50, 90)):
    """Tính EMA, RSI, MACD trên dữ liệu trong memory, không ghi CSV"""
    if not data or "close" not in data[0]:
        print("Dữ liệu không hợp lệ hoặc thiếu cột 'close'")
        return data  # Return data ngay cả khi không xử lý được

    closes = [float(r["close"]) for r in data]
    ema = {p: calculate_ema(closes, p) for p in periods}
    rsi = calculate_rsi(closes)
    macd, sig, hist = calculate_macd(closes)

    n = len(data)
    for i, row in enumerate(data):
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
        if row["ema_20"] and row["ema_50"] and row["ema_90"] and row["rsi14"] and row["macd"] and row["macd_signal"]:
            row["trend_score"] = score_trend(
                float(row["ema_20"]),
                float(row["ema_50"]),
                float(row["ema_90"]),
                float(row["rsi14"]),
                float(row["macd"]),
                float(row["macd_signal"])
            )
        else:   
            row["trend_score"] = ""
    
    add_volume_ratio(data)
 
    return data

# caculate avg vol 

def add_volume_ratio(data, lookback_days=50):
    """
    Thêm cột volume_ratio = volume / trung bình volume (dựa trên 50 ngày trước đó)
    """
    for i, d in enumerate(data):
        if i < lookback_days:
            # Không đủ dữ liệu để tính trung bình
            d["volume_ratio"] = ""
            continue

        # Lấy 50 ngày trước đó
        recent_volumes = [float(data[j]["volume"]) for j in range(i - lookback_days, i) if data[j].get("volume")]
        if not recent_volumes:
            d["volume_ratio"] = ""
            continue

        avg_volume = sum(recent_volumes) / len(recent_volumes)

        if d.get("volume") and (float(d['volume']) / avg_volume) > 1:
            d["volume_ratio"] = f"{(float(d['volume']) / avg_volume):.2f}"
        else:
            d["volume_ratio"] = 0

    return data
