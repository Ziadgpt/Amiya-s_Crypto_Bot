# execution.py
import ccxt
from config import TESTNET_INDEXER_URL
from utils.logger import setup_logger

logger = setup_logger('execution', 'execution.log')

def initialize_exchange():
    """Initialize dYdX testnet exchange."""
    try:
        exchange = ccxt.dydx({
            'apiKey': 'YOUR_TESTNET_API_KEY',
            'secret': 'YOUR_TESTNET_API_SECRET',
            'enableRateLimit': True,
            'urls': {'api': TESTNET_INDEXER_URL}
        })
        logger.info("Initialized dYdX testnet exchange")
        return exchange
    except Exception as e:
        logger.error(f"Error initializing exchange: {e}")
        return None

def place_order(market, side, size, price):
    """Place a market/limit order on dYdX testnet."""
    exchange = initialize_exchange()
    if exchange:
        try:
            order = exchange.create_order(market, 'limit', side, size, price)
            logger.info(f"Placed {side} order for {size} {market} at {price}")
            return order
        except Exception as e:
            logger.error(f"Error placing order for {market}: {e}")
            return None
    return None