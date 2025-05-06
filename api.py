import os
import sqlite3
from fastapi import FastAPI, Query, HTTPException, Depends
from typing import Optional
import pandas as pd
from datetime import datetime
from dateutil import parser as dateparser
from fastapi.responses import Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials

DB_FOLDER = "db"
TABLE_NAME = "kline"
SYMBOLS = [
    "BTCUSDT","ETHUSDT","BNBUSDT","XRPUSDT","SOLUSDT",
    "DOGEUSDT","ADAUSDT","TRXUSDT","LINKUSDT","AVAXUSDT"
]
DEFAULT_LIMIT = 1000
USER = os.getenv("API_USER", "admin")
PASS = os.getenv("API_PASS", "admin123")

app = FastAPI()
security = HTTPBasic()

def get_db_path(symbol: str):
    f = os.path.join(DB_FOLDER, f"{symbol}.sqlite")
    if not os.path.exists(f):
        raise HTTPException(404, detail="No such database")
    return f

def to_unix_ms(val: Optional[str]):
    if not val:
        return None
    try:
        if str(val).isdigit():
            n = int(val)
            # auto-detect ms or sec
            return n if n > 10**11 else n*1000
        else:
            dt = dateparser.parse(val)
            return int(dt.timestamp()*1000)
    except Exception:
        raise HTTPException(400, detail=f"Invalid date: {val}")

def check_auth(credentials: HTTPBasicCredentials = Depends(security)):
    if credentials.username != USER or credentials.password != PASS:
        from fastapi import status
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return True

@app.get("/symbols")
def list_symbols():
    """Get list of available symbols"""
    return {"symbols": SYMBOLS}

@app.get("/available_range")
def available_range(symbol: str = Query(..., description="Coin symbol")):
    db_path = get_db_path(symbol)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(f"SELECT MIN(open_time), MAX(open_time) FROM {TABLE_NAME}")
    min_ts, max_ts = cur.fetchone()
    conn.close()
    if min_ts is None or max_ts is None:
        return {"min_open_time": None, "max_open_time": None, "min_time_human": None, "max_time_human": None}
    min_dt = datetime.utcfromtimestamp(min_ts/1000).strftime("%Y-%m-%d %H:%M:%S")
    max_dt = datetime.utcfromtimestamp(max_ts/1000).strftime("%Y-%m-%d %H:%M:%S")
    return {
        "min_open_time": min_ts,
        "max_open_time": max_ts,
        "min_time_human": min_dt + " UTC",
        "max_time_human": max_dt + " UTC"
    }

@app.get("/klines")
def get_klines(
    auth=Depends(check_auth),
    symbol: str = Query(..., description="Coin symbol (e.g. BTCUSDT)"),
    start: Optional[str] = Query(None, description="YYYY-MM-DD или timestamp (ms)"),
    end: Optional[str] = Query(None, description="YYYY-MM-DD или timestamp (ms)"),
    limit: Optional[int] = Query(DEFAULT_LIMIT)
):
    if symbol not in SYMBOLS:
        raise HTTPException(404, detail="Unknown symbol")
    db_path = get_db_path(symbol)
    start_ms = to_unix_ms(start)
    end_ms = to_unix_ms(end)
    sql = f"SELECT * FROM {TABLE_NAME}"
    params = []
    where = []
    if start_ms is not None:
        where.append("open_time >= ?")
        params.append(start_ms)
    if end_ms is not None:
        where.append("open_time <= ?")
        params.append(end_ms)
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY open_time LIMIT ?"
    params.append(limit)
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()
    return df.to_dict(orient="records")

@app.get("/bulk_export")
def bulk_export(
    auth=Depends(check_auth),
    symbol: str = Query(..., description="Coin symbol"),
    start: Optional[str] = Query(None, description="YYYY-MM-DD или timestamp(ms)"),
    end: Optional[str] = Query(None, description="YYYY-MM-DD или timestamp(ms)")
):
    if symbol not in SYMBOLS:
        raise HTTPException(404, detail="Unknown symbol")
    db_path = get_db_path(symbol)
    start_ms = to_unix_ms(start)
    end_ms = to_unix_ms(end)
    sql = f"SELECT * FROM {TABLE_NAME}"
    params = []
    where = []
    if start_ms is not None:
        where.append("open_time >= ?")
        params.append(start_ms)
    if end_ms is not None:
        where.append("open_time <= ?")
        params.append(end_ms)
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY open_time"
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()
    csv_bytes = df.to_csv(index=False).encode('utf-8')
    headers = {
        "Content-Disposition": f"attachment; filename={symbol}_klines.csv"
    }
    return Response(content=csv_bytes, media_type="text/csv", headers=headers)

@app.get("/agg")
def aggregate(
    auth=Depends(check_auth),
    symbol: str = Query(...),
    timeframe: str = Query("1m", description="1m, 1h, 1d"),
    start: Optional[str] = Query(None, description="YYYY-MM-DD или timestamp (ms)"),
    end: Optional[str] = Query(None, description="YYYY-MM-DD или timestamp (ms)")
):
    if symbol not in SYMBOLS:
        raise HTTPException(404, detail="Unknown symbol")
    db_path = get_db_path(symbol)
    start_ms = to_unix_ms(start)
    end_ms = to_unix_ms(end)
    sql = f"SELECT * FROM {TABLE_NAME}"
    params = []
    where = []
    if start_ms is not None:
        where.append("open_time >= ?")
        params.append(start_ms)
    if end_ms is not None:
        where.append("open_time <= ?")
        params.append(end_ms)
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY open_time"
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()
    if timeframe == "1h":
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
        df.set_index('open_time', inplace=True)
        agg = df.resample("1H").agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
            "close_time": "last",
            "quote_asset_volume": "sum",
            "number_of_trades": "sum",
            "taker_buy_base_asset_volume": "sum",
            "taker_buy_quote_asset_volume": "sum",
            "ignore": "last"
        }).dropna().reset_index()
        agg['open_time'] = agg['open_time'].astype("int64") // 10**6
        return agg.to_dict(orient="records")
    elif timeframe == "1d":
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
        df.set_index('open_time', inplace=True)
        agg = df.resample("1D").agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
            "close_time": "last",
            "quote_asset_volume": "sum",
            "number_of_trades": "sum",
            "taker_buy_base_asset_volume": "sum",
            "taker_buy_quote_asset_volume": "sum",
            "ignore": "last"
        }).dropna().reset_index()
        agg['open_time'] = agg['open_time'].astype("int64") // 10**6
        return agg.to_dict(orient="records")
    else:
        return df.to_dict(orient="records")