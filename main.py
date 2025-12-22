import time
import threading
from datetime import datetime
import pytz
from api.crawlData import fetch_klines, SYMBOLS
from service.calculateData import process_file, get_trend_label
from notify.notify import tele_notification
from config.enums import SLEEP_INTERVAL_TRADING

results = {}
results_lock = threading.Lock()
completed_count = 0  # ĐÃ SỬA: Phải khai báo ở ngoài function

def job(symbol, interval_name, interval_str, limit):
    global completed_count 
    
    while True:
        try:
            print(f"\n Đang xử lý {symbol}...")
                
            # Fetch and process data
            # endtime = datetime(2025, 12, 20, 15, 0)  
            # toTs = int(endtime.timestamp())
            # klines = fetch_klines(symbol, interval_str, limit,toTs)
            klines = fetch_klines(symbol, interval_str, limit)
            
            processed_data = process_file(klines, (20, 50, 90),20)
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
                    completed_count = 0  
                
        except Exception as e:
            print(f" Lỗi xử lý {symbol}: {e}")
            import traceback
            traceback.print_exc()
        
        time.sleep(SLEEP_INTERVAL_TRADING)

def send_aggregated_report_once():
    print("\n Gửi báo cáo tổng hợp...")
    aggregated_message = f"<b>📊BÁO CÁO TỔNG HỢP NGÀY {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</b>\n"
    aggregated_message += "="*40 + "\n"
    
    for symbol in SYMBOLS:
        if symbol in results:
            r = results[symbol]
            if r['message']:
                aggregated_message += f"{r['message']}"
    
    if aggregated_message.count('\n') > 2:
        tele_notification(aggregated_message)
        print("\n Gửi báo cáo tổng hợp thành công")
    else:
        print("\n Không có dữ liệu để gửi trong báo cáo tổng hợp")

if __name__ == "__main__":
    print("Bắt đầu hệ thống theo dõi crypto...")
    
    try:
        threads = []
        for symbol in SYMBOLS:
            t = threading.Thread(
                target=job,
                args=(symbol, "4h", "4h", 200),
                daemon=False  
            )
            t.start()
            threads.append(t)
            time.sleep(1)

        # Chỉ cần join một lần, threads sẽ chạy mãi mãi
        for t in threads:
            t.join()
            
    except KeyboardInterrupt:
        print("\nĐang dừng hệ thống...")