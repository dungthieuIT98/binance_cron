import threading
import time
from datetime import datetime, timedelta
import pandas as pd
from api.CrawlDataCK import StockDataFetcher
from config.enums import SYMBOL_CK, SLEEP_INTERVAL
from notify.notify import tele_notification
from service.calculateData import process_file
from service.calculateCkvn import calculate_stock_indicators


class DailyStockAnalyzer:
    """Handles daily stock analysis and reporting."""
    
    def __init__(self):
        self.results = {}
        self.results_lock = threading.Lock()
        self.completed_count = 0

    @staticmethod
    def calculate_date_range():
        """Calculate date range for stock data fetching (from 2020 to now)."""
        from_date_obj = datetime(2020, 1, 1)
        to_date_obj = datetime.now()
        from_date = from_date_obj.strftime('%d/%m/%Y')
        to_date = to_date_obj.strftime('%d/%m/%Y')
        return from_date, to_date

    def process_symbol(self, symbol):
        """Process a single stock symbol continuously with while loop."""
        # while True:  # Comment để chạy 1 lần, bỏ comment để loop liên tục
        try:
                print(f"\nProcessing {symbol}...")
                fetcher = StockDataFetcher()
                from_date, to_date = self.calculate_date_range()
                
                # Fetch and process stock data
                data = fetcher.fetch_stock_data(symbol, from_date, to_date, 1, 1000)
                data.reverse()  # Reverse for chronological order
                processed_data = process_file(data, (20, 50, 90), 50)
                processed_data = calculate_stock_indicators(processed_data)  # Auto saves to Excel

                # Create DataFrame for analysis
                df = pd.DataFrame(processed_data)
                # Store results
                with self.results_lock:
                    self.results[symbol] = {
                        "message": self._extract_message_from_dataframe(df),
                        "timestamp": datetime.now()
                    }
                    self.completed_count += 1
                    
                    # Send report when all symbols are processed
                    if self.completed_count == len(SYMBOL_CK):
                        self._send_aggregated_report()
                        self.completed_count = 0
                        
        except Exception as e:
            print(f"Error processing {symbol}: {e}")
            import traceback
            traceback.print_exc()
        
        # time.sleep(SLEEP_INTERVAL)  # Bỏ comment nếu dùng while True

    def _extract_message_from_dataframe(self, df):
        """Extract trend analysis message from processed dataframe."""
        if len(df) < 6:
            return "Not enough data for analysis."
            
        recent_data = df.iloc[-8:-1]
        prev_day = df.iloc[-2]
        symbol = prev_day['symbol']

        # Check various market conditions
        conditions = self._analyze_market_conditions(recent_data, prev_day)
        
        return self._generate_analysis_message(symbol, recent_data, conditions)
    
    def _analyze_market_conditions(self, recent_data, prev_day):
        """Analyze market conditions and return condition flags."""
        try:
            # Replace empty strings with 0 before converting to float
            ema_50 = recent_data['ema_50'].replace('', 0).astype(float)
            close = recent_data['close'].replace('', 0).astype(float)
            rsi_high = recent_data['rsi_high'].replace('', 0).astype(float)
            prev_rsi = float(prev_day['rsi_high']) if prev_day['rsi_high'] != '' else 0
            
            return {
                'ema_uptrend': (ema_50 < close).sum() >= 3,
                'rsi_high': (rsi_high > 50).all() and (rsi_high > 65).sum() >= 2,
                'rsi_trend_leader': (rsi_high > 65).all() and (rsi_high > 70).sum() >= 3,
                'highest': prev_rsi > 60
            }
        except Exception as e:
            print(f"⚠️ Error analyzing conditions: {e}")
            return {
                'ema_uptrend': False,
                'rsi_high': False,
                'rsi_trend_leader': False,
                'highest': False
            }
    
    def _generate_analysis_message(self, symbol, recent_data, conditions):
        """Generate analysis message based on market conditions."""
        message = ""
        rsi_values = recent_data['rsi_high'].tolist()
        
        if conditions['rsi_trend_leader']:
            message = f"👉<b>{symbol} 🟢 Strong Trend Leader.</b>"
        elif (conditions['ema_uptrend'] and conditions['rsi_high']):
            message = f"👉{symbol} Uptrend."
            
            if conditions['highest']:
                message += "🟢Highest."
        else:
            return "" 
            
        message += f"\nRSI: {rsi_values}\n"
        return message

    def _send_aggregated_report(self):
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        aggregated_message = f"<b>📈 Daily Stock Report {current_time}</b>\n"
        aggregated_message += "=" * 40 + "\n"

        # Collect all meaningful messages
        for symbol in SYMBOL_CK:
            if symbol in self.results and self.results[symbol]['message']:
                aggregated_message += f"{self.results[symbol]['message']}\n"

        # Send notification if there's meaningful content
        # if aggregated_message.count('\n') > 2:
            # tele_notification(aggregated_message)
        # else:
        #     print("\nNo significant stock data to send in the report.")

    def run_daily_analysis(self):
        """Start continuous stock analysis with threads for all symbols."""
        print("Starting continuous daily stock analysis...")
        
        # Reset counters and results
        self.completed_count = 0
        self.results = {}
        
        # Start threads for all symbols (continuous processing)
        threads = []
        for symbol in SYMBOL_CK:
            thread = threading.Thread(target=self.process_symbol, args=(symbol,), daemon=False)
            thread.start()
            threads.append(thread)
            time.sleep(1)  # Small delay between thread starts
        
        # Join threads (they run continuously)
        for thread in threads:
            thread.join()


def run_daily_stock_job():
    analyzer = DailyStockAnalyzer()
    analyzer.run_daily_analysis()


if __name__ == "__main__":
    print("Starting daily stock analysis service...")
    
    try:
        run_daily_stock_job()
    except KeyboardInterrupt:
        print("\nStopping the system...")