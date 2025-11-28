import requests

# ================= CONFIG =================
BOT_TOKEN = "8155656052:AAH4ytlcApLa_N9Zi-B_sTizSrSO0Nv24yQ"
CHAT_ID = "2062254404"

def tele_notification(results, latest_date=None):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": results}
    try:
        res = requests.post(url, data=payload, timeout=10)
        res.raise_for_status()
    except requests.RequestException as e:
        print("❌ Lỗi gửi Telegram:", e)
