"""
Crypto Trading Analysis System
Monitors cryptocurrency prices and technical indicators every 4 hours.
Sends aggregated reports via Telegram when all symbols are processed.
"""

import time
import threading
from datetime import datetime
import pytz
from api.crawlData import fetch_klines, SYMBOLS
from service.calculateData import process_file
from service.calculateCoin import get_trend_label
from notify.notify import tele_notification
from config.enums import SLEEP_INTERVAL_TRADING

# Global state
results = {}
results_lock = threading.Lock()
completed_count = 0

def job(symbol, interval_name, interval_str, limit):
    """Process a single cryptocurrency symbol continuously.
    
    Args:
        symbol: Crypto symbol (e.g., 'BTC', 'ETH')
        interval_name: Human-readable interval ('4h')
        interval_str: API interval parameter ('4h')
        limit: Number of candles to fetch (200)
    """
    global completed_count 
    
    while True:
        try:
            print(f"\n🔎 Processing {symbol}...")
                
            # Fetch latest market data and calculate indicators
            klines = fetch_klines(symbol, interval_str, limit)
            processed_data = process_file(klines, (20, 50, 90), 20)
            message = get_trend_label(processed_data)
            
            # Store results and check if all symbols completed
            with results_lock:
                results[symbol] = {
                    "message": message,
                    "timestamp": datetime.now(),
                    "interval": interval_name
                }
                completed_count += 1
                
                # Send aggregated report when all symbols are done
                if completed_count == len(SYMBOLS):
                    send_aggregated_report_once()
                    completed_count = 0  
                
        except Exception as e:
            print(f"❌ Error processing {symbol}: {e}")
            import traceback
            traceback.print_exc()
        
        # Wait before next analysis cycle
        time.sleep(SLEEP_INTERVAL_TRADING)

def send_aggregated_report_once():
    """Send consolidated Telegram report for all processed symbols."""
    print("\n📤 Sending aggregated report...")
    aggregated_message = f"<b>📊 CRYPTO REPORT {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</b>\n"
    aggregated_message += "="*40 + "\n"
    
    for symbol in SYMBOLS:
        if symbol in results:
            r = results[symbol]
            if r['message']:
                aggregated_message += f"{r['message']}"
    
    if aggregated_message.count('\n') > 2:
        tele_notification(aggregated_message)
        print("\n✅ Aggregated report sent successfully")
    else:
        print("\n⚠️ No significant data to report")

if __name__ == "__main__":
    print("🚀 Starting Crypto Monitoring System...")
    print(f"📊 Tracking {len(SYMBOLS)} symbols: {', '.join(SYMBOLS)}")
    print(f"⏰ Analysis interval: 4 hours\n")
    
    try:
        threads = []
        
        # Start monitoring thread for each symbol
        for symbol in SYMBOLS:
            t = threading.Thread(
                target=job,
                args=(symbol, "4h", "4h", 200),
                daemon=False
            )
            t.start()
            threads.append(t)
            time.sleep(1)  # Stagger thread starts

        # Keep main thread alive
        for t in threads:
            t.join()
            
    except KeyboardInterrupt:
        print("\n🛑 Stopping system...")