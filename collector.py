import os
import sqlite3
import time
import requests
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(".env")
BINANCE_API_KEY = os.getenv("BINANCE_API")
BINANCE_SECRET = os.getenv("BINANCE_SECRET")

DB_FOLDER = "db"
TABLE_NAME = "kline"
SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "SOLUSDT",
    "DOGEUSDT",
    "ADAUSDT",
    "TRXUSDT",
    "LINKUSDT",
    "AVAXUSDT"
]
COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_asset_volume", "number_of_trades",
    "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
]

N_MINUTES = 1000

LOG_FILE = "collector.log"
MAX_LOG_SIZE = 1 * 1024 * 1024
BACKUP_COUNT = 10

logger = logging.getLogger("collector")
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(LOG_FILE, maxBytes=MAX_LOG_SIZE, backupCount=BACKUP_COUNT)
formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s]: %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def get_last_open_time(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(f"SELECT MAX(open_time) FROM {TABLE_NAME}")
    row = cur.fetchone()
    conn.close()
    return row[0] if row and row[0] else None

def insert_kline(db_path, kline):
    conn = sqlite3.connect(db_path)
    with conn:
        conn.execute(
            f"INSERT OR IGNORE INTO {TABLE_NAME} ({', '.join(COLUMNS)}) VALUES ({', '.join(['?']*len(COLUMNS))})",
            kline
        )
    conn.close()

def batch_fill_history(symbol, db_path, earliest_time=None):
    url = "https://api.binance.com/api/v3/klines"
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
    last_time = get_last_open_time(db_path)
    if last_time is None:
        if earliest_time is not None:
            start_ms = earliest_time
        else:
            start_ms = int(datetime(2017, 8, 17, 4, 0).timestamp()) * 1000
    else:
        start_ms = last_time + 60000
    now_ms = int(time.time()) * 1000
    while start_ms < now_ms:
        limit = min(N_MINUTES, (now_ms - start_ms) // 60000 + 1)
        params = {
            "symbol": symbol,
            "interval": "1m",
            "startTime": start_ms,
            "limit": limit
        }
        try:
            r = requests.get(url, params=params, headers=headers, timeout=10)
            r.raise_for_status()
            data = r.json()
            if not data:
                break
            for kline in data:
                insert_kline(db_path, kline)
            logger.info(f"{symbol} fill: {len(data)} candles {datetime.utcfromtimestamp(data[0][0]/1000)} - {datetime.utcfromtimestamp(data[-1][0]/1000)}")
            start_ms = data[-1][0] + 60000
            time.sleep(0.2)
        except Exception as e:
            logger.error(f"{symbol} error in batch fill: {e}")
            break

def get_last_n_open_times(db_path, n):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(f"SELECT open_time FROM {TABLE_NAME} ORDER BY open_time DESC LIMIT {n}")
    rows = cur.fetchall()
    conn.close()
    return set(r[0] for r in rows)

def fetch_binance_n_klines(symbol, n):
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": "1m", "limit": n}
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
    r = requests.get(url, params=params, headers=headers, timeout=10)
    r.raise_for_status()
    data = r.json()
    return data if data else []

def minute_loop():
    logger.info("Minute candles monitor started (batch fill + RT sync, 1000 min, auto log rotation)")
    while True:
        for symbol in SYMBOLS:
            db_path = os.path.join(DB_FOLDER, f"{symbol}.sqlite")
            try:
                batch_fill_history(symbol, db_path)
                db_minutes = get_last_n_open_times(db_path, N_MINUTES)
                klines = fetch_binance_n_klines(symbol, N_MINUTES)
                new_count = 0
                for kline in klines:
                    if kline[0] not in db_minutes:
                        insert_kline(db_path, kline)
                        new_count += 1
                        logger.info(f"{symbol} saved minute: {datetime.utcfromtimestamp(kline[0]/1000)} ({kline[0]})")
                if new_count > 0:
                    logger.info(f"{symbol} new/filled candles: {new_count} (last: {datetime.utcfromtimestamp(klines[-1][0]/1000)})")
            except Exception as e:
                logger.error(f"{symbol} error: {e}")
        now_sec = int(time.time())
        secs_until_next_minute = 60 - now_sec % 60
        time.sleep(max(2, secs_until_next_minute))

if __name__ == "__main__":
    minute_loop()