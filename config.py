# config.py
# General Bot Settings
BOT_NAME = "DYDX_ML_BOT"

# dYdX Network Settings
INDEXER_URL = "https://indexer.dydx.trade"  # Mainnet for data, testnet for trading
TESTNET_INDEXER_URL = "https://indexer.v4testnet.dydx.exchange"

# Market and Trading Parameters
TRADING_MARKETS = [
    "BTC-USD",
    "ETH-USD",
    "SOL-USD",
    "ADA-USD",
    "XRP-USD"
]
CANDLE_RESOLUTION = "5MINS"  # dYdX v4 standard: 1MIN, 5MINS, 15MINS, 1HOUR, etc.

# API & Logging
POLLING_INTERVAL_SECONDS = 300  # 5 minutes