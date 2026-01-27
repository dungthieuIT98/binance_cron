def get_trend_label(data):
    """
    Analyze cryptocurrency data and generate trend alert message.
    
    This function examines the last completed candle to detect:
    - Strong/weak uptrends and downtrends based on trend_score
    - RSI overbought/oversold conditions (>70 or <30)
    - High volume alerts combined with RSI signals
    - Historical trend data (6 days history)
    
    Args:
        data: List of candle data with calculated indicators (trend_score, rsi14, volume_ratio)
        
    Returns:
        str: HTML-formatted alert message for Telegram, or empty string if no alert
        
    Note: Requires trend_score and volume_ratio to be calculated in data
    """
    # Check last completed candle (use -2 because -1 is incomplete)
    last_candle = data[-2]
    
    # Get symbol and current metrics
    symbol = last_candle.get("symbol", "N/A")
    current_score = last_candle["trend_score"]
    current_rsi = last_candle.get("rsi14", "N/A")
    current_volume_ratio = last_candle.get("volume_ratio", "N/A")
    
    # Collect 6-day history (from -7 to -2)
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

    # Classify trend strength
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
    
    # Add historical data if label exists
    if label:    
        old_scores_str = ", ".join(old_scores) if old_scores else ""
        old_rsis_str = ", ".join(old_rsis) if old_rsis else ""
        label += f"\n ==>RSI: {old_rsis_str}\n ==> History: {old_scores_str}"

    # RSI overbought/oversold alert
    if float(current_rsi) > 70 or float(current_rsi) < 30:
        label += f"\n<b>⚠️ Cảnh báo RSI: {current_rsi}</b>"

    # High volume alerts
    if float(current_volume_ratio) > 2 and float(current_rsi) > 60:
        label += f"\n<b>⚠️🟢Cảnh báo tăng Volume cao: {current_volume_ratio}x</b>"
    elif float(current_volume_ratio) > 2 and float(current_rsi) < 40:
        label += f"\n<b>⚠️🔴Cảnh báo Volume cao: {current_volume_ratio}x</b>"
    
    if label:    
        return f"👉{symbol}: {label}\n"
    return ""



def calculate_crypto_indicators(data):
    """
    Process cryptocurrency data to add technical indicator flags.
    
    Features for crypto:
    - Uses EMA20/EMA50 (faster trend indicators suitable for volatile crypto markets)
    - Tracks price vs EMA20 and EMA20 vs EMA50 relationships  
    - Detects both MACD uptrend and downtrend
    
    Returns:
        data: Enhanced with show_indicator, rsi_high, macd_down fields
    """
    for d in data:
        d["show_indicator"] = ""
        
        # EMA20 trend indicator (faster for crypto)
        if d.get("close") and d.get("ema_20") and float(d["close"]) > float(d["ema_20"]):
            d["show_indicator"] = "up ema_20"

        # EMA50 trend indicator
        if d.get("ema_20") and d.get("ema_50") and float(d["ema_20"]) > float(d["ema_50"]):
            d["show_indicator"] += ", up ema_50"

        # RSI indicator (all ranges tracked)
        d["rsi_high"] = d.get("rsi14")

        # MACD trend indicator (tracks both up and down)
        macd_raw = d.get("macd", 0)
        macd_signal_raw = d.get("macd_signal", 0)
        macd = float(macd_raw) if macd_raw != '' else 0
        macd_signal = float(macd_signal_raw) if macd_signal_raw != '' else 0
        
        if macd < macd_signal and macd < 0:
            d["macd_down"] = "down_trend"
        elif macd > macd_signal and macd > 0:
            d["macd_down"] = "up_trend"
        else:
            d["macd_down"] = ""
        
    return data
