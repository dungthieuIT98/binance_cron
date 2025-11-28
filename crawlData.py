import csv
import pandas as pd
from datetime import datetime, timedelta, timezone
import requests
import os

API_URL = "https://api.binance.com/api/v3/klines"
SYMBOLS = [
  "BTCUSDT",
  "ETHUSDT",
  "XRPUSDT",
  "BNBUSDT",
  "SOLUSDT",
  "TRXUSDT",
  "DOGEUSDT",
  "ADAUSDT",
  "BCHUSDT",
  "LINKUSDT",
  "XLMUSDT"
]
def fetch_klines(symbol: str, interval: str, limit: int):
    """Lấy dữ liệu klines từ Binance và trả về list of dicts"""
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    response = requests.get(API_URL, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
    
    columns = [
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_vol", "num_trades",
        "taker_buy_vol", "taker_buy_quote_vol", "ignore"
    ]
    df = pd.DataFrame(data, columns=columns)
    df["symbol"] = symbol
    
    return df.to_dict('records')
