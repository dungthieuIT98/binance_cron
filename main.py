import time
import threading
from datetime import datetime
import pytz
from crawlData import fetch_klines, SYMBOLS
from calculateData import process_file, get_trend_label
from notify import tele_notification

# SLEEP_INTERVAL = 4 * 60 * 60  # 4h = 14400 giây
SLEEP_INTERVAL =  60  # 4h = 14400 giây

results = {}
results_lock = threading.Lock()
completed_count = 0  # ĐÃ SỬA: Phải khai báo ở ngoài function

def get_next_run_time():
    """Tính toán thời điểm chạy tiếp theo (4h, 8h, 12h, 16h, 20h, 00h giờ Mỹ)"""
    us_tz = pytz.timezone('America/New_York')  # EST/EDT
    now = datetime.now(us_tz)
    
    # Các mốc giờ chạy
    run_hours = [0, 4, 8, 12, 16, 20]
    
    current_hour = now.hour
    next_hour = None
    
    # Tìm mốc giờ tiếp theo
    for h in run_hours:
        if h > current_hour:
            next_hour = h
            break
    
    # Nếu không tìm thấy (sau 20h), chạy vào 00h ngày hôm sau
    if next_hour is None:
        next_run = now.replace(hour=0, minute=0, second=0, microsecond=0)
        from datetime import timedelta
        next_run = next_run + timedelta(days=1)
    else:
        next_run = now.replace(hour=next_hour, minute=0, second=0, microsecond=0)
    
    # Tính số giây cần sleep
    sleep_seconds = (next_run - now).total_seconds()
    return sleep_seconds, next_run

def job(symbol, interval_name, interval_str, limit):
    global completed_count 
    
    while True:
        try:
            print(f"\n Đang xử lý {symbol}...")
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
                    completed_count = 0  
                
        except Exception as e:
            print(f" Lỗi xử lý {symbol}: {e}")
            import traceback
            traceback.print_exc()
        
        time.sleep(SLEEP_INTERVAL)

def send_aggregated_report_once():

    aggregated_message = f"📊BÁO CÁO TỔNG HỢP NGÀY {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
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

    print(" Bắt đầu hệ thống theo dõi crypto...")
    try:
        while True:
            sleep_seconds, next_run = get_next_run_time()
            #time.sleep(sleep_seconds + 5)

            # Khởi động thread cho mỗi symbol
            threads = []
            for symbol in SYMBOLS:
                # t = threading.Thread(
                #     target=job,
                #     args=(symbol, "4h", "4h", 300),
                #     daemon=False  
                # )
                t = threading.Thread(
                    target=job,
                    args=(symbol, "1m", "1m", 300),
                    daemon=False  
                )
                t.start()
                threads.append(t)
                time.sleep(1)

            # Chờ tất cả thread hoàn thành
            for t in threads:
                t.join()

    except KeyboardInterrupt:
        print("\n Đang dừng hệ thống...")