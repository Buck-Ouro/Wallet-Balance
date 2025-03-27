import os
import requests
import time
import hmac
import hashlib
import base64
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from cryptography.fernet import Fernet
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Load environment variables
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")
PROXY_HTTP = os.getenv("PROXY_HTTP")
PROXY_HTTPS = os.getenv("PROXY_HTTPS")

def decrypt(encrypted_text, key):
    cipher = Fernet(key)
    return cipher.decrypt(encrypted_text.encode()).decode()

# Authenticate Google Sheets API
credentials_path = "/tmp/gcp_credentials.json"
if not os.path.exists(credentials_path):
    raise ValueError("❌ Google Sheets credentials file not found!")

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
client = gspread.authorize(creds)

logging.info("✅ Google Sheets authentication successful!")

# Google Sheets Configuration
SHEET_ID = "15eCPbtZr3-MVoSFloY8EfTiadMZQ1n6WZ4Pq-QqqCY4"
SHEET_NAME = "Sheet1"
sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)

# Fetch encrypted API keys from Google Sheets
rows = sheet.get_all_values()

dec_key = ENCRYPTION_KEY.strip().encode()
PROXIES = {"http": PROXY_HTTP, "https": PROXY_HTTPS} if PROXY_HTTP and PROXY_HTTPS else {}

# Binance API Endpoints
ACCOUNT_INFO_URL = "https://api.binance.com/api/v3/account"
TICKER_PRICE_URL = "https://api.binance.com/api/v3/ticker/price"

for row_index, row in enumerate(rows[1:], start=2):  # Skip header row
    encrypted_api_key = row[1]
    encrypted_api_secret = row[2]
    
    try:
        API_KEY = decrypt(encrypted_api_key, dec_key)
        API_SECRET = decrypt(encrypted_api_secret, dec_key)
    except Exception as e:
        logging.error(f"❌ Row {row_index} decryption failed: {e}")
        continue
    
    timestamp = int(time.time() * 1000)
    query_string = f"timestamp={timestamp}"
    signature = hmac.new(API_SECRET.encode(), query_string.encode(), hashlib.sha256).hexdigest()
    headers = {"X-MBX-APIKEY": API_KEY}
    
    try:
        response = requests.get(f"{ACCOUNT_INFO_URL}?{query_string}&signature={signature}", headers=headers, proxies=PROXIES, timeout=15)
        response.raise_for_status()
        
        account_data = response.json()
        total_usd = 0.0
        balances = {b["asset"]: float(b["free"]) for b in account_data["balances"] if float(b["free"]) > 0}
        asset_prices = {}
        
        for asset in balances.keys():
            if asset == "USDT":
                asset_prices[asset] = 1.0
            else:
                price_response = requests.get(f"{TICKER_PRICE_URL}?symbol={asset}USDT", proxies=PROXIES, timeout=15)
                if price_response.status_code == 200:
                    asset_prices[asset] = float(price_response.json()["price"])
                else:
                    asset_prices[asset] = 0
        
        for asset, amount in balances.items():
            total_usd += amount * asset_prices.get(asset, 0)
        
        sheet.update(f"A{row_index}", [[f"${total_usd:,.2f}"]])
        logging.info(f"✅ Row {row_index}: ${total_usd:,.2f}")
    except Exception as e:
        logging.error(f"❌ Row {row_index} failed: {e}")
    
    time.sleep(2)  # Delay before processing the next row
