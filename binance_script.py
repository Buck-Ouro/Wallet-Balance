import os
import time
import logging
import requests
import gspread
import hmac
import hashlib
from functools import wraps, lru_cache
from cryptography.fernet import Fernet
from oauth2client.service_account import ServiceAccountCredentials

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Configuration
BASE_URL = "https://api.binance.com"
FUTURES_URL = "https://fapi.binance.com"

def decrypt(encrypted_text, key):
    """Decrypt encrypted text using Fernet symmetric encryption"""
    cipher = Fernet(key)
    return cipher.decrypt(encrypted_text.encode()).decode()

def validate_environment():
    """Validate all required environment variables"""
    required_vars = [
        'GCP_CREDENTIALS_PATH',
        'ENCRYPTION_KEY',
        'SHEET_ID'
    ]
    
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise ValueError(f"Missing environment variables: {', '.join(missing)}")
    
    if not os.path.exists(os.getenv('GCP_CREDENTIALS_PATH')):
        raise FileNotFoundError(
            f"Credentials file not found at {os.getenv('GCP_CREDENTIALS_PATH')}"
        )

def retry_api():
    """Retry decorator - retries once after 2 seconds on failure"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except (requests.exceptions.RequestException, 
                   gspread.exceptions.APIError) as e:
                logging.warning(f"Retry in 2s for {func.__name__}")
                time.sleep(2)
                try:
                    return func(*args, **kwargs)
                except Exception as retry_error:
                    logging.error(f"Retry failed for {func.__name__}")
                    raise retry_error
        return wrapper
    return decorator

class BinanceAPI:
    def __init__(self, api_key, api_secret, proxies=None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.proxies = proxies or {}
        self.last_call = 0

    def _rate_limit(self):
        elapsed = time.time() - self.last_call
        if elapsed < 0.1:
            time.sleep(0.1 - elapsed)
        self.last_call = time.time()

    def _create_signature(self, query_string):
        """Create HMAC SHA256 signature for Binance API"""
        return hmac.new(
            self.api_secret.encode(),
            query_string.encode(),
            hashlib.sha256
        ).hexdigest()

    def _get_headers(self):
        """Return headers with API key for Binance requests"""
        return {"X-MBX-APIKEY": self.api_key}

    @lru_cache(maxsize=32)
    def _get_price(self, asset):
        if asset == "USDT":
            return 1.0
        try:
            self._rate_limit()
            response = requests.get(
                f"{BASE_URL}/api/v3/ticker/price",
                params={"symbol": f"{asset}USDT"},
                proxies=self.proxies,
                timeout=5
            )
            response.raise_for_status()
            return float(response.json()["price"])
        except Exception:
            logging.warning(f"Price fetch failed for {asset}, using 0")
            return 0.0

    @retry_api()
    def get_spot_balances(self):
        """Fetch spot account balances from Binance"""
        timestamp = str(int(time.time() * 1000))
        query_string = f"timestamp={timestamp}"
        signature = self._create_signature(query_string)

        response = requests.get(
            f"{BASE_URL}/api/v3/account",
            params=f"{query_string}&signature={signature}",
            headers=self._get_headers(),
            proxies=self.proxies,
            timeout=10
        )
        response.raise_for_status()
        
        account_data = response.json()
        balances = {b["asset"]: float(b["free"]) for b in account_data["balances"] if float(b["free"]) > 0}
        
        # Calculate total USD value and get BTC amount
        total = 0.0
        btc_amount = balances.get("BTC", 0.0)
        
        for asset, amount in balances.items():
            total += amount * self._get_price(asset)
        return total, btc_amount

    @retry_api()
    def get_futures_equity(self):
        """Fetch futures account equity from Binance (includes unrealized PnL)"""
        timestamp = str(int(time.time() * 1000))
        query_string = f"timestamp={timestamp}"
        signature = self._create_signature(query_string)

        response = requests.get(
            f"{FUTURES_URL}/fapi/v2/account",
            params=f"{query_string}&signature={signature}",
            headers=self._get_headers(),
            proxies=self.proxies,
            timeout=10
        )
        response.raise_for_status()
        
        account_data = response.json()
        # This includes unrealized PnL (totalCrossUnPnl)
        return float(account_data["totalWalletBalance"]) + float(account_data["totalCrossUnPnl"])

@retry_api()
def update_sheet(sheet, row_index, total_value, btc_amount):
    sheet.batch_update([{
        'range': f"A{row_index}",
        'values': [[f"${total_value:,.2f}"]]
    }, {
        'range': f"E{row_index}",
        'values': [[btc_amount]]
    }])

def main():
    try:
        validate_environment()
        
        # Initialize clients
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            os.getenv("GCP_CREDENTIALS_PATH"),
            ["https://spreadsheets.google.com/feeds"]
        )
        sheet = gspread.authorize(creds).open_by_key(
            os.getenv("SHEET_ID")
        ).worksheet(os.getenv("SHEET_NAME", "Sheet1"))

        # Process rows
        dec_key = os.getenv("ENCRYPTION_KEY").encode()
        rows = sheet.get_all_values()

        for row_index, row in enumerate(rows[1:], start=2):
            spot_total = 0.0
            btc_amount = 0.0
            futures = 0.0

            try:
                api = BinanceAPI(
                    decrypt(row[1], dec_key),
                    decrypt(row[2], dec_key),
                    {"http": os.getenv("PROXY_HTTP"), "https": os.getenv("PROXY_HTTPS")}
                )

                try:
                    spot_total, btc_amount = api.get_spot_balances()
                except Exception as e:
                    logging.warning(f"Row {row_index} - Spot balance fetch failed: {e}")

                try:
                    futures = api.get_futures_equity()
                except Exception as e:
                    logging.warning(f"Row {row_index} - Futures equity fetch failed: {e}")

                total_value = spot_total + futures
                update_sheet(sheet, row_index, total_value, btc_amount)

                logging.info(
                    f"Processed row {row_index}: "
                    f"${total_value:,.2f} (Spot: ${spot_total:,.2f}, Futures: ${futures:,.2f}, BTC: {btc_amount:.8f})"
                )
                time.sleep(1)  # Row processing delay

            except Exception as e:
                logging.error(f"Row {row_index} failed to initialize or decrypt: {e}")

    except Exception as e:
        logging.error(f"Script failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
