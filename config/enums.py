from enum import Enum

class Symbols(Enum):
    BTC = "BTC"
    ETH = "ETH"
    BNB = "BNB"
    SOL = "SOL"

SLEEP_INTERVAL = 24 * 60 * 60  # 24 hours in seconds

SLEEP_INTERVAL_TRADING = 4 * 60 * 60  # 24 hours in seconds

SYMBOL_CK = [
    "TCB", "VPB", "MBB", "ACB", "SSI", "VND", "VCI", "HCM", "VHM", "MWG", 
    "FPT", "VNM", "MSN", "HPG", "BMP", "TCM", "VHC", "PTB", "GMD", "GAS", 
    "PVD", "DPM", "BSR", "REE", "BCM", "HVN", "CTR", "VTP", "BVH", "DHG", 
    "DPR", "PHR", "DGC", "CMG", "FOX", "ELC", "PC1", "GEG", "HDG", "POW"

]