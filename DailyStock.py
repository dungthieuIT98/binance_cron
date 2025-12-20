import threading
import time
from datetime import datetime, timedelta
import pandas as pd
from api.CrawlDataCK import StockDataFetcher
from config.enums import SYMBOL_CK, SLEEP_INTERVAL
from notify.notify import tele_notification
from service.calculateData import process_file
from service.caculate_ckvn import calculate_ckvn


class DailyStockAnalyzer:
    """Handles daily stock analysis and reporting."""
    
    def __init__(self):
        self.results = {}
        self.results_lock = threading.Lock()
        self.completed_count = 0

    @staticmethod
    def calculate_date_range():
        """Calculate date range for stock data fetching (last 1200 days)."""
        to_date = datetime.now().strftime('%d/%m/%Y')
        from_date = (datetime.now() - timedelta(days=200)).strftime('%d/%m/%Y')
        return from_date, to_date

    def process_symbol(self, symbol):
        """Process a single stock symbol continuously with while loop."""
        while True:
            try:
                print(f"\nProcessing {symbol}...")
                fetcher = StockDataFetcher()
                from_date, to_date = self.calculate_date_range()
                
                # Fetch and process stock data
                data = fetcher.fetch_stock_data(symbol, from_date, to_date, 1, 1000)
                data.reverse()  # Reverse for chronological order
                processed_data = process_file(data, symbol)
                processed_data = calculate_ckvn(processed_data)

                # Create DataFrame with required columns
                df = pd.DataFrame(processed_data)
                columns_to_keep = [
                    "timestamp", "close", "symbol", "trend_score", "show_indicator",
                    "rsi_high", "vol_high", "ema_20", "ema_50", "ema_90"
                ]
                df = df[columns_to_keep]
                
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
            
            time.sleep(SLEEP_INTERVAL)

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
        return {
            'ema_uptrend': (recent_data['ema_50'].astype(float) < recent_data['close'].astype(float)).sum() >= 3,
            'vol_high': (recent_data['vol_high'].astype(float) > 1.2).sum() >= 4,
            'vol_super_high': (recent_data['vol_high'].astype(float) > 1.9).sum() >= 1,
            'rsi_high': (recent_data['rsi_high'].astype(float) > 50).all() and (recent_data['rsi_high'].astype(float) > 65).sum() >= 2,
            'vol_trend_leader': (recent_data['vol_high'].astype(float) > 1.5).sum() >= 4 and (recent_data['vol_high'].astype(float) > 2.5).sum() >= 1,
            'rsi_trend_leader': (recent_data['rsi_high'].astype(float) > 65).all() and (recent_data['rsi_high'].astype(float) > 70).sum() >= 3,
            'highest': float(prev_day['rsi_high']) > 60 and float(prev_day['vol_high']) > 2
        }
    
    def _generate_analysis_message(self, symbol, recent_data, conditions):
        """Generate analysis message based on market conditions."""
        message = ""
        rsi_values = recent_data['rsi_high'].tolist()
        vol_values = recent_data['vol_high'].tolist()
        
        if conditions['vol_trend_leader'] and conditions['rsi_trend_leader']:
            message = f"👉<b>{symbol} 🟢 Strong Trend Leader.</b>"
        elif (conditions['ema_uptrend'] and conditions['vol_high'] and conditions['rsi_high']):
            if conditions['vol_super_high']:
                message = f"👉<b>{symbol} Strong Uptrend.</b>"
            else:
                message = f"👉{symbol} Uptrend."
            
            if conditions['highest']:
                message += "🟢Highest."
        else:
            return "" 
            
        message += f"\nRSI: {rsi_values}\nVol: {vol_values}\n"
        return message

    def _send_aggregated_report(self):
        """Send aggregated report via Telegram notification."""
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        aggregated_message = f"<b>📈 Daily Stock Report {current_time}</b>\n"
        aggregated_message += "=" * 40 + "\n"

        # Collect all meaningful messages
        for symbol in SYMBOL_CK:
            if symbol in self.results and self.results[symbol]['message']:
                aggregated_message += f"{self.results[symbol]['message']}\n"

        # Send notification if there's meaningful content
        if aggregated_message.count('\n') > 2:
            tele_notification(aggregated_message)
        else:
            print("\nNo significant stock data to send in the report.")

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