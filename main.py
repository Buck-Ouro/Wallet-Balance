import os
import requests
import time
import hmac
import hashlib
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from cryptography.fernet import Fernet
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load environment variables
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")
PROXY_HTTP = os.getenv("PROXY_HTTP")
PROXY_HTTPS = os.getenv("PROXY_HTTPS")
GCP_CREDENTIALS_PATH = os.getenv("GCP_CREDENTIALS_PATH", "/tmp/gcp_credentials.json")

# Binance API Endpoints
SPOT_ACCOUNT_URL = "https://api.binance.com/api/v3/account"
FUTURES_ACCOUNT_URL = "https://fapi.binance.com/fapi/v2/account"
TICKER_PRICE_URL = "https://api.binance.com/api/v3/ticker/price"

# Google Sheets Configuration
SHEET_ID = "15eCPbtZr3-MVoSFloY8EfTiadMZQ1n6WZ4Pq-QqqCY4"
SHEET_NAME = "Sheet1"

def decrypt(encrypted_text, key):
    """Decrypt encrypted text using Fernet symmetric encryption"""
    cipher = Fernet(key)
    return cipher.decrypt(encrypted_text.encode()).decode()

def get_timestamp():
    """Get current timestamp in milliseconds"""
    return int(time.time() * 1000)

def create_signature(query_string, api_secret):
    """Create HMAC SHA256 signature for Binance API"""
    return hmac.new(api_secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()

def get_headers(api_key):
    """Return headers with API key for Binance requests"""
    return {"X-MBX-APIKEY": api_key}

def get_asset_prices(assets, proxies=None):
    """Fetch current prices for assets in USDT"""
    asset_prices = {}
    proxies = proxies or {}
    
    for asset in assets:
        if asset == "USDT":
            asset_prices[asset] = 1.0
            continue

        try:
            price_response = requests.get(
                f"{TICKER_PRICE_URL}?symbol={asset}USDT",
                proxies=proxies,
                timeout=15
            )
            if price_response.status_code == 200:
                asset_prices[asset] = float(price_response.json()["price"])
            else:
                logging.warning(f"Could not fetch price for {asset}, defaulting to 0")
                asset_prices[asset] = 0
        except Exception as e:
            logging.error(f"Error fetching price for {asset}: {e}")
            asset_prices[asset] = 0

    return asset_prices

def get_spot_balances(api_key, api_secret, proxies=None):
    """Fetch spot account balances from Binance"""
    proxies = proxies or {}
    timestamp = get_timestamp()
    query_string = f"timestamp={timestamp}"
    signature = create_signature(query_string, api_secret)
    
    try:
        response = requests.get(
            f"{SPOT_ACCOUNT_URL}?{query_string}&signature={signature}",
            headers=get_headers(api_key),
            proxies=proxies,
            timeout=15
        )
        response.raise_for_status()
        
        account_data = response.json()
        return {b["asset"]: float(b["free"]) for b in account_data["balances"] if float(b["free"]) > 0}
    except Exception as e:
        logging.error(f"Spot balance error: {e}")
        return {}

def get_futures_equity(api_key, api_secret, proxies=None):
    """Fetch futures account equity from Binance"""
    proxies = proxies or {}
    timestamp = get_timestamp()
    query_string = f"timestamp={timestamp}"
    signature = create_signature(query_string, api_secret)
    
    try:
        response = requests.get(
            f"{FUTURES_ACCOUNT_URL}?{query_string}&signature={signature}",
            headers=get_headers(api_key),
            proxies=proxies,
            timeout=15
        )
        response.raise_for_status()
        
        account_data = response.json()
        return float(account_data["totalWalletBalance"]) + float(account_data["totalCrossUnPnl"])
    except Exception as e:
        logging.error(f"Futures balance error: {e}")
        return 0.0

def main():
    # Validate environment variables
    if not ENCRYPTION_KEY:
        raise ValueError("ENCRYPTION_KEY environment variable is required")
    
    # Configure proxies if available
    PROXIES = {}
    if PROXY_HTTP and PROXY_HTTPS:
        PROXIES = {"http": PROXY_HTTP, "https": PROXY_HTTPS}
        logging.info("Proxy configuration enabled")
    else:
        logging.info("No proxy configuration found")

    # Authenticate Google Sheets API
    if not os.path.exists(GCP_CREDENTIALS_PATH):
        raise ValueError(f"Google Sheets credentials file not found at {GCP_CREDENTIALS_PATH}")
    
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GCP_CREDENTIALS_PATH, scope)
    client = gspread.authorize(creds)
    logging.info("Google Sheets authentication successful")

    # Access Google Sheet
    sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
    rows = sheet.get_all_values()
    dec_key = ENCRYPTION_KEY.strip().encode()

    # Process each row in the sheet
    for row_index, row in enumerate(rows[1:], start=2):  # Skip header row
        encrypted_api_key = row[1]
        encrypted_api_secret = row[2]
        
        try:
            API_KEY = decrypt(encrypted_api_key, dec_key)
            API_SECRET = decrypt(encrypted_api_secret, dec_key)
        except Exception as e:
            logging.error(f"Row {row_index} decryption failed: {e}")
            continue
        
        try:
            # Get balances
            spot_balances = get_spot_balances(API_KEY, API_SECRET, PROXIES)
            futures_equity = get_futures_equity(API_KEY, API_SECRET, PROXIES)
            
            # Calculate spot balance in USD
            if spot_balances:
                asset_prices = get_asset_prices(spot_balances.keys(), PROXIES)
                total_usd_spot = sum(spot_balances[asset] * asset_prices.get(asset, 0) for asset in spot_balances)
            else:
                total_usd_spot = 0.0
            
            # Calculate total value (spot + futures equity)
            total_value = total_usd_spot + futures_equity
            
            # Update Google Sheet
            sheet.update(f"A{row_index}", [[f"${total_value:,.2f}"]])
            logging.info(
                f"Row {row_index}: Updated successfully - "
                f"Spot: ${total_usd_spot:,.2f} + "
                f"Futures: ${futures_equity:,.2f} = "
                f"Total: ${total_value:,.2f}"
            )
            
        except Exception as e:
            logging.error(f"Row {row_index} processing failed: {e}")
        
        time.sleep(2)  # Rate limiting

if __name__ == "__main__":
    main()
