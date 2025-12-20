from datetime import datetime
import pandas as pd
import time
import threading

from service.calculateData import process_file
from service.caculate_ckvn import calculate_ckvn
from api.crawlData import fetch_klines
from notify.notify import tele_notification
from config.enums import Symbols, SLEEP_INTERVAL

# Global variables
results = {}
results_lock = threading.Lock()
completed_count = 0

def process_symbol_data(symbol):
    """Process data for a single symbol in a continuous loop."""
    global completed_count

    while True:
        try:
            print(f"\nProcessing {symbol}...")
            
            # Fetch and process data
            raw_data = fetch_klines(symbol, '1d', 300)
            processed_data = process_file(raw_data, symbol)
            processed_data = calculate_ckvn(processed_data)

            # Create DataFrame with only required columns
            columns_to_keep = [
                "timestamp", "close", "symbol", "trend_score", 
                "show_indicator", "rsi_high", "vol_high", "macd_down"
            ]
            df = pd.DataFrame(processed_data)[columns_to_keep]

            # Update results
            with results_lock:
                results[symbol] = {
                    "message": extract_message_from_dataframe(df),
                    "timestamp": datetime.now()
                }
                completed_count += 1

                if completed_count == len(symbols):
                    send_aggregated_report()
                    completed_count = 0

        except Exception as e:
            print(f"Error processing {symbol}: {e}")

        time.sleep(SLEEP_INTERVAL)

def send_aggregated_report():
    """Send aggregated report for all processed symbols."""
    aggregated_message = f"<b>📊 Daily Report {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</b>\n"
    aggregated_message += "=" * 40 + "\n"

    for symbol in symbols:
        if symbol in results and results[symbol]['message']:
            aggregated_message += f"{results[symbol]['message']}\n"

    if aggregated_message.count('\n') > 2:
        tele_notification(aggregated_message)
    else:
        print("\nNo data to send in the aggregated report.")

def extract_message_from_dataframe(df):
    """Extract trading message from dataframe based on technical indicators."""
    if len(df) < 6:
        return "Not enough data to extract message."

    # Analyze last 3 days of data (excluding the most recent day)
    recent_data = df.iloc[-5:-2]
    prev_day = df.iloc[-2]
    symbol = prev_day['symbol']

    # Convert to float for calculations
    rsi_values = recent_data['rsi_high'].astype(float)
    vol_values = recent_data['vol_high'].astype(float)
    prev_rsi = float(prev_day['rsi_high'])
    prev_vol = float(prev_day['vol_high'])

    # Define conditions
    uptrend_rsi = (rsi_values > 65).all()
    high_volume = (vol_values > 1.5).any()
    downtrend_rsi = (rsi_values < 50).all()
    
    highest_condition = prev_rsi > 80 and prev_vol > 1.5
    lowest_condition = prev_rsi < 30 and prev_vol > 1.5

    # Generate messages
    if uptrend_rsi and high_volume:
        message = f"👉{symbol} Uptrend."
        if highest_condition:
            message += "🟢Highest."
        return message
    
    elif downtrend_rsi and high_volume:
        message = f"👉{symbol} Downtrend."
        if lowest_condition:
            message += "🔴Lowest."
        return message

    return ""


def main():
    """Main function to start the daily blockchain data processing."""
    global symbols
    
    symbols = [symbol.value for symbol in Symbols]
    print("Starting daily blockchain data fetcher...")
    print(f"Processing {len(symbols)} symbols: {', '.join(symbols)}")

    try:
        threads = []
        
        # Start a thread for each symbol
        for symbol in symbols:
            thread = threading.Thread(
                target=process_symbol_data, 
                args=(symbol,), 
                daemon=False
            )
            thread.start()
            threads.append(thread)
            time.sleep(1)  # Stagger thread starts

        # Wait for all threads to complete (they run indefinitely)
        for thread in threads:
            thread.join()

    except KeyboardInterrupt:
        print("\nStopping the system...")


if __name__ == "__main__":
    main()