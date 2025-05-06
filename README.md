# Crypto Data Collector & API

A self-contained project for collecting, storing, and serving historical 1-minute candles (klines) for the top 10 cryptocurrencies from Binance. Includes an automatic data collector and a secure FastAPI server for data access and bulk export.

---

## Project Structure

```
crypto-data-collector/
├── db/                   # SQLite databases for each symbol (auto-created/mounted as Docker volume)
├── api.py                # FastAPI server for API access
├── collector.py          # Binance candle data collector
├── requirements.txt      # Project dependencies
├── .env                  # Your secret API credentials and auth (see below)
├── entrypoint.sh         # Runs collector and API together in Docker
├── Dockerfile            # Builds the Docker container
├── .gitignore
├── .dockerignore
```

---

## Setting up `.env`

Create a file called `.env` in the project root with the following variables:

```
# API Authentication (protects your FastAPI server)
API_USER=yourusername
API_PASS=yourverysecretpassword

# Binance API keys (for the collector)
BINANCE_API=your_binance_api_key
BINANCE_SECRET=your_binance_api_secret
```

---

## Installing and Running (Locally)

1. **Install Python dependencies:**
   ```
   pip install -r requirements.txt
   ```

2. **Start the collector:**
   ```
   python collector.py
   ```

3. **Start the API server (in a new terminal):**
   ```
   uvicorn api:app --host 0.0.0.0 --port 8080
   ```

4. **Access the API (needs authentication):**
   - Swagger UI: `http://localhost:8080/docs`
   - Use your `API_USER` & `API_PASS` to authorize

---

## Running in Docker (Recommended/Production/AWS EC2)

1. **Build the Docker image:**
   ```
   docker build -t mycryptoapi .
   ```

2. **Run the container:**
   ```
   docker run -d \
     --env-file .env \
     -v $(pwd)/db:/app/db \
     -p 8080:8080 \
     mycryptoapi
   ```
   - Maps your `db/` folder as a Docker volume (persist databases)
   - Loads environment variables from `.env`

3. **(Optional, production) Always restart on failure:**
   ```
   docker run --restart always -d ... (rest as above)
   ```

4. **On AWS EC2:**  
   Open port 8080 for your IP in the Security Group settings.

---

## API Overview

- **/symbols** — List supported symbols
- **/available_range?symbol=SYMBOL** — Show available data period for symbol
- **/klines** — Get 1-min candles in JSON (with date ranges and limit)
- **/bulk_export** — Download candles as CSV (with date ranges)
- **/agg** — On-the-fly aggregation to 1m, 1h, or 1d candles

**All endpoints (except /symbols) require HTTP Basic authentication!**

---

## Example Requests

Query 10 BTCUSDT 1-min candles with Python:

```python
import requests
from requests.auth import HTTPBasicAuth

url = "http://yourserver:8080/klines"
params = {"symbol": "BTCUSDT", "start": "2023-01-01", "end": "2023-01-02", "limit": 10}
auth = HTTPBasicAuth("youruser", "yourpassword")
resp = requests.get(url, params=params, auth=auth)
print(resp.json())
```

---

## Security Notes

- Change API_USER and API_PASS to something unique and strong!
- Use Security Groups or firewalls to restrict external access to the API.
- Never commit your `.env` or SQLite files to public git repositories!
