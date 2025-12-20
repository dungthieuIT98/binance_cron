import requests
from datetime import datetime, timedelta

class TokenService:
    def __init__(self):
        self.access_token = None
        self.token_expiry = None

    def get_access_token(self):
        # Check if token is expired or not set
        if self.access_token == None or not self.token_expiry or datetime.now() >= self.token_expiry:
            self.access_token = self.fetch_new_token()

        return self.access_token

    def fetch_new_token(self):
        url = "https://fc-data.ssi.com.vn/api/v2/Market/AccessToken"
        headers = {
            "Content-Type": "application/json"
        }
        payload = {
            "consumerID": "9386329312674e1c8e359084531eccb0",
            "consumerSecret": "5ea3e893cbae4ec7ac461df63c68bb58"
        }
        response = requests.post(url, json=payload, headers=headers)

        if response.status_code == 200:
            res = response.json()
            token = res.get("data", {}).get("accessToken")
            if not token:
                raise Exception(f"Không tìm thấy accessToken: {res}")
            expired_in = res["data"].get("expiredIn", 25200)
            self.token_expiry = datetime.now() + timedelta(seconds=expired_in)

            return token
        else:
            raise Exception(f"Lỗi lấy token: {response.status_code} - {response.text}")


class StockDataFetcher:
    def __init__(self):
        self.token_service = TokenService()

    def format_stock_data(self, raw_data, symbol):
        formatted_data = []
        for candle in raw_data.get('data', []):
            try:
                formatted_data.append({
                    'timestamp': datetime.strptime(candle.get('TradingDate', ''), '%d/%m/%Y').strftime('%Y-%m-%d %H:%M:%S'),
                    'open': float(candle.get('Open', 0)),
                    'high': float(candle.get('High', 0)),
                    'low': float(candle.get('Low', 0)),
                    'close': float(candle.get('Close', 0)),
                    'volume': float(candle.get('Volume', 0)),
                    'symbol': symbol
                })
            except (ValueError, KeyError) as e:
                print(f"Error formatting candle data: {e}")
        return formatted_data

    def fetch_stock_data(self, symbol, from_date, to_date, page_index=1, page_size=1000):
        url = f"https://fc-data.ssi.com.vn/api/v2/Market/DailyOhlc"
        headers = {
            'Authorization': f"Bearer {self.token_service.get_access_token()}"
        }
        params = {
            'fromDate': from_date,
            'toDate': to_date,
            'pageIndex': page_index,
            'pageSize': page_size,
            'Symbol': symbol
        }

        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            raw_data = response.json()
            return self.format_stock_data(raw_data, symbol)
        else:
            raise Exception(f"Failed to fetch stock data: {response.status_code}, {response.text}")