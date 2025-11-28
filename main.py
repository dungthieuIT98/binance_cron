import time
import threading
from datetime import datetime
from crawlData import fetch_klines, SYMBOLS
from calculateData import process_file, get_trend_label
from notify import tele_notification

SLEEP_INTERVAL = 4 * 60 * 60  # 4h = 14400 giây

results = {}
results_lock = threading.Lock()
completed_count = 0  # ĐÃ SỬA: Phải khai báo ở ngoài function

def job(symbol, interval_name, interval_str, limit):
    global completed_count  # ĐÃ SỬA: Phải khai báo global
    
    while True:
        try:
            print(f"\n🔄 Đang xử lý {symbol}...")
            klines = fetch_klines(symbol, interval_str, limit)
            processed_data = process_file(klines)
            message = get_trend_label(processed_data)
            
            with results_lock:
                results[symbol] = {
                    "message": message,
                    "timestamp": datetime.now(),
                    "interval": interval_name
                }
                completed_count += 1
                
                if completed_count == len(SYMBOLS):
                    send_aggregated_report_once()
                    completed_count = 0  # Reset
                
        except Exception as e:
            print(f"❌ Lỗi xử lý {symbol}: {e}")
            import traceback
            traceback.print_exc() 
        
        time.sleep(SLEEP_INTERVAL)

def send_aggregated_report_once():
    """Gửi báo cáo tổng hợp 1 lần (được gọi từ job)"""
    # ĐÃ SỬA: Không cần lock vì đã được gọi trong lock rồi
    aggregated_message = "📊 BÁO CÁO TỔNG HỢP\n"
    aggregated_message += f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    aggregated_message += "="*40 + "\n"
    
    for symbol in SYMBOLS:
        if symbol in results:
            r = results[symbol]
            if r['message']:
                aggregated_message += f"{r['message']}"
    
    # Gửi telegram nếu có ít nhất 1 tín hiệu
    if aggregated_message.count('\n') > 3:
        print("\n" + aggregated_message)
        tele_notification(aggregated_message)

if __name__ == "__main__":
    for symbol in SYMBOLS:
        t = threading.Thread(
            target=job,
            args=(symbol, "4h", "4h", 300),
            daemon=True
        )
        t.start()
        time.sleep(1)  # Delay nhỏ giữa các thread để tránh rate limit
    
    # Giữ main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n👋 Đang dừng hệ thống...")