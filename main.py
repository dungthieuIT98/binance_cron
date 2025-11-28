import time
import threading
from datetime import datetime
from crawlData import fetch_klines, SYMBOLS
from calculateData import process_file, get_trend_label
from notify import tele_notification

SLEEP_INTERVAL = 4 * 60 * 60  # 4h = 14400 giây

results = {}
results_lock = threading.Lock()
completed_count = 0

def job(symbol, interval_name, interval_str, limit):
    """Job chạy định kỳ để lấy và xử lý dữ liệu"""
    global completed_count
    
    while True:
        message = ""
        try:
            # 1. Lấy dữ liệu từ Binance
            klines = fetch_klines(symbol, interval_str, limit)
            # 2. Xử lý và tính toán các chỉ báo
            processed_data = process_file(klines)
            message = get_trend_label(processed_data)
            # 3. Lưu kết quả vào shared dict (thread-safe)
            with results_lock:
                results[symbol] = {
                    "message": message,
                    "timestamp": datetime.now(),
                    "interval": interval_name
                }
                completed_count += 1
                
                # Nếu đủ số lượng symbols, gửi tổng hợp
                if completed_count == len(SYMBOLS):
                    send_aggregated_report()
                    completed_count = 0  # Reset
        
        except Exception as e:
            import traceback
            traceback.print_exc()
            
            with results_lock:
                results[symbol] = {
                    "message": f"",
                    "timestamp": datetime.now(),
                    "interval": interval_name
                }
                completed_count += 1

        # Chờ trước khi chạy lần tiếp theo
        time.sleep(SLEEP_INTERVAL)

def send_aggregated_report():
    aggregated_message = "📊 BÁO CÁO TỔNG HỢP\n"
    aggregated_message += f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    aggregated_message += "="*40 + "\n"
    for symbol in SYMBOLS:
        if symbol in results:
            r = results[symbol]
            if r['message']:
                aggregated_message += f"{r['message']}"
    
    # Gửi telegram
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
    # Giữ main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n👋 Đang dừng hệ thống...")