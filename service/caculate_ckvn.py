def calculate_ckvn(data):
    """
    Process the data to display specific conditions:
    - Show ema50 when price > ema50
    - Show ema90 when ema50 > ema90
    - Show rsi when rsi > 50
    """
    for d in data:
        # Display ema50 when price > ema50
        if d.get("close") and d.get("ema_50") and float(d["close"]) > float(d["ema_50"]):
            d["show_indicator"] = "up ema50 " 
        else:
            d["show_indicator"] = "down ema50"
        # Display ema90 when ema50 > ema90
        if d.get("ema_50") and d.get("ema_90") and float(d["ema_50"]) > float(d["ema_90"]):
            d["show_indicator"] += ", up ema90 " 
        else:
            d["show_indicator"] += ", down ema90"

        # Display rsi when rsi > 75
        if d.get("rsi14") and float(d["rsi14"]) > 55 and float(d["rsi14"]) < 75:
            d["rsi_high"] = d["rsi14"]
        elif d.get("rsi14") and float(d["rsi14"]) > 75:
            d["rsi_high"] = d["rsi14"]
        elif d.get("rsi14") and float(d["rsi14"]) < 50 and float(d["rsi14"]) >= 30:
            d["rsi_high"] = d["rsi14"]
        elif d.get("rsi14") and float(d["rsi14"]) < 30 and float(d["rsi14"]) >= 1:
            d["rsi_high"] = d["rsi14"]
        else:
            d["rsi_high"] = d["rsi14"]

        # Display rsi when vol > 1,2
        if d.get("volume_ratio") and float(d["volume_ratio"]) > 1.2 and float(d["volume_ratio"]) < 3:
            d["vol_high"] = d["volume_ratio"]
        elif d.get("volume_ratio") and float(d["volume_ratio"]) > 3:
            d["vol_high"] = d["volume_ratio"]
        else:
            d["vol_high"] = d["volume_ratio"]

        if d.get("macd") and d.get("signal") and float(d["macd"]) < float(d["signal"]) and float(d["macd"]) < 0:
            d["macd_down"] = "down_trend"
        else:
            d["macd_down"] = ""
    return data