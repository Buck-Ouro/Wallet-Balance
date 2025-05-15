import os
import time
import json
import logging
import random
import requests
import gspread
import hmac
import hashlib
from functools import wraps
from cryptography.fernet import Fernet
from oauth2client.service_account import ServiceAccountCredentials

# Logging setup - only INFO level messages and above
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration
BASE_URL = "https://api.bybit.com"
MAX_API_RETRIES = 3
INITIAL_RETRY_DELAY = 5
BACKOFF_FACTOR = 2
RECV_WINDOW = "5000"

def decrypt(encrypted_text, key):
    """Decrypt encrypted text using Fernet symmetric encryption"""
    cipher = Fernet(key)
    return cipher.decrypt(encrypted_text.encode()).decode()

def validate_environment():
    required_vars = ['GCP_CREDENTIALS_PATH', 'ENCRYPTION_KEY', 'SHEET_ID']
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise ValueError(f"Missing env vars: {', '.join(missing)}")
    if not os.path.exists(os.getenv('GCP_CREDENTIALS_PATH')):
        raise FileNotFoundError(f"GCP credentials not found at {os.getenv('GCP_CREDENTIALS_PATH')}")

def retry_api(max_retries=MAX_API_RETRIES, initial_delay=INITIAL_RETRY_DELAY):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            last_exception = None
            while retries <= max_retries:
                try:
                    return func(*args, **kwargs)
                except (requests.exceptions.RequestException, gspread.exceptions.APIError) as e:
                    retries += 1
                    last_exception = e
                    if retries > max_retries:
                        logging.error(f"Max retries reached for {func.__name__}")
                        raise e
                    delay = initial_delay * (BACKOFF_FACTOR ** (retries - 1))
                    logging.warning(f"Retrying {func.__name__} in {delay:.1f}s after error: {e}")
                    time.sleep(delay * random.uniform(0.8, 1.2))
            raise last_exception
        return wrapper
    return decorator

@retry_api()
def fetch_spot_prices(coin_list, proxies=None):
    prices = {"USDT": 1.0, "USDC": 1.0}
    try:
        response = requests.get(
            f"{BASE_URL}/v5/market/tickers",
            params={"category": "spot"},
            proxies=proxies,
            timeout=10
        )
        response.raise_for_status()
        tickers = response.json()["result"]["list"]
        for coin in coin_list:
            if coin in prices:
                continue
            symbol = f"{coin}USDT"
            price_entry = next((item for item in tickers if item["symbol"] == symbol), None)
            if price_entry:
                prices[coin] = float(price_entry["lastPrice"])
            else:
                logging.warning(f"No price found for {coin}, defaulting to 0.0")
                prices[coin] = 0.0
    except Exception as e:
        logging.warning(f"Failed to fetch prices: {e}")
        for coin in coin_list:
            prices.setdefault(coin, 0.0)
    logging.info(f"Spot prices: {prices}")
    return prices

@retry_api()
def get_subaccount_balances(api_key, api_secret, member_id, coins, proxies=None):
    timestamp = str(int(time.time() * 1000))
    coin_param = ",".join(coins)

    params = {
        "api_key": api_key,
        "accountType": "UNIFIED",
        "coin": coin_param,
        "timestamp": timestamp,
        "recv_window": RECV_WINDOW
    }
    if member_id:
        params["memberId"] = member_id

    param_str = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
    signature = hmac.new(api_secret.encode(), param_str.encode(), hashlib.sha256).hexdigest()
    params["sign"] = signature

    response = requests.get(
        f"{BASE_URL}/v5/asset/transfer/query-account-coins-balance",
        params=params,
        proxies=proxies,
        timeout=10
    )
    response.raise_for_status()
    data = response.json()
    if data["retCode"] != 0:
        raise ValueError(f"API error: {data['retMsg']}")
    return data["result"]["balance"]

def calculate_total_value(balances, prices):
    total = 0.0
    for asset in balances:
        coin = asset["coin"]
        amount = float(asset.get("walletBalance", 0))
        price = prices.get(coin, 0.0)
        total += amount * price
    return total

@retry_api(max_retries=2, initial_delay=3)
def update_sheet(sheet, row_index, value):
    sheet.update(range_name=f"A{row_index}", values=[[f"${value:,.2f}"]])

def main():
    try:
        validate_environment()

        # Initialize clients
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            os.getenv("GCP_CREDENTIALS_PATH"),
            ["https://spreadsheets.google.com/feeds"]
        )
        sheet = gspread.authorize(creds).open_by_key(os.getenv("SHEET_ID")).worksheet(os.getenv("SHEET_NAME", "Sheet2"))
        rows = sheet.get_all_values()

        coin_cell = sheet.acell("H1").value
        coin_list = json.loads(coin_cell)
        logging.info(f"Using coins: {coin_list}")

        proxies = {
            "http": os.getenv("PROXY_HTTP"),
            "https": os.getenv("PROXY_HTTPS")
        }
        proxies = {k: v for k, v in proxies.items() if v}

        price_cache = fetch_spot_prices(coin_list, proxies)

        dec_key = os.getenv("ENCRYPTION_KEY").encode()

        for row_index, row in enumerate(rows[1:], start=2):
            try:
                api_key_encrypted = row[1]
                api_secret_encrypted = row[2]

                if not api_key_encrypted or not api_secret_encrypted:
                    logging.info(f"Skipping row {row_index} (missing API credentials)")
                    continue

                api_key = decrypt(api_key_encrypted, dec_key)
                api_secret = decrypt(api_secret_encrypted, dec_key)

                member_id = row[4].strip() if len(row) > 4 else ""
                target_type = f"subaccount ({member_id})" if member_id else "main account"

                balances = get_subaccount_balances(api_key, api_secret, member_id or None, coin_list, proxies)
                nav = calculate_total_value(balances, price_cache)

                update_sheet(sheet, row_index, nav)
                logging.info(f"Processed row {row_index} ({target_type}): ${nav:,.2f}")
                time.sleep(1)

            except Exception as e:
                logging.error("Script failed", exc_info=True)

    except Exception as e:
        logging.error(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    main()
