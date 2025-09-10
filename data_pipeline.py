import pandas as pd
import numpy as np
import sqlite3
import requests
from datetime import datetime, timedelta
import logging
from config import INDEXER_URL, TRADING_MARKETS, CANDLE_RESOLUTION

# Setup logging
logging.basicConfig(
    filename='data_pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('data_pipeline')

def get_available_markets(indexer_url=INDEXER_URL):
    """Fetch available perpetual markets from dYdX v4 mainnet."""
    url = f"{indexer_url}/v4/perpetualMarkets"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        markets = response.json().get('markets', {})
        market_list = list(markets.keys())
        logger.info(f"Available markets from {indexer_url}: {market_list}")
        print(f"Available markets from {indexer_url}: {market_list}")
        return market_list
    except Exception as e:
        logger.error(f"Error fetching markets from {indexer_url}: {e}")
        print(f"Error fetching markets from {indexer_url}: {e}")
        return []

def fetch_data(market='BTC-USD', timeframe=CANDLE_RESOLUTION, limit=200, indexer_url=INDEXER_URL):
    """Fetch OHLCV candles from dYdX v4 mainnet via REST API."""
    timeframes = [timeframe, '1MIN']  # Fallback to 1MIN if 5MINS fails
    markets_to_try = [market, market.replace('USD', 'USDT'), 'LINK-USD', 'MATIC-USD']
    # Set date range for recent data (last 1 day)
    to_iso = datetime.utcnow().isoformat() + 'Z'
    from_iso = (datetime.utcnow() - timedelta(days=1)).isoformat() + 'Z'
    for mkt in markets_to_try:
        for tf in timeframes:
            url = f"{indexer_url}/v4/candles/perpetualMarkets/{mkt}"
            params = {'resolution': tf, 'limit': limit, 'fromIso': from_iso, 'toIso': to_iso}
            try:
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                data = response.json().get('candles', [])
                if not data:
                    logger.warning(f"No data returned for {mkt} at {url} with {tf}")
                    print(f"No data returned for {mkt} at {url} with {tf}")
                    continue
                # Convert to DataFrame
                df = pd.DataFrame([{
                    'started_at': candle['startedAt'],
                    'open': float(candle['open']),
                    'high': float(candle['high']),
                    'low': float(candle['low']),
                    'close': float(candle['close']),
                    'base_token_volume': float(candle['baseTokenVolume'])
                } for candle in data])
                df['started_at'] = pd.to_datetime(df['started_at'])
                df = df.sort_values('started_at').reset_index(drop=True)
                df['log_returns'] = np.log(df['close'] / df['close'].shift(1)).fillna(0)
                df['volatility'] = df['log_returns'].rolling(window=20).std().fillna(0)
                logger.info(f"Fetched {len(df)} candles for {mkt} at {url} with {tf}")
                print(f"Fetched {len(df)} candles for {mkt} at {url} with {tf}")
                return df
            except Exception as e:
                logger.error(f"Error fetching {mkt} at {url} with {tf}: {e}")
                print(f"Error fetching {mkt} at {url} with {tf}: {e}")
                if 'response' in locals():
                    logger.error(f"Response: {response.text}")
                    print(f"Response: {response.text}")
    logger.error(f"Failed to fetch data for {market} from mainnet")
    print(f"Failed to fetch data for {market} from mainnet")
    return pd.DataFrame()

def save_to_db(df, market='BTC-USD', db_name='crypto_data.db'):
    """Save historical data to SQLite for backtesting."""
    if df.empty:
        logger.warning(f"No data to save for {market}")
        print(f"No data to save for {market}")
        return
    try:
        conn = sqlite3.connect(db_name)
        table_name = f'{market.replace("-", "_")}_data'
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()
        logger.info(f"Saved {len(df)} rows for {market} to {db_name}")
        print(f"Saved {len(df)} rows for {market} to {db_name}")
    except Exception as e:
        logger.error(f"Error saving {market} to DB: {e}")
        print(f"Error saving {market} to DB: {e}")

def get_live_data(market='BTC-USD'):
    """Fetch latest candle for real-time trading."""
    df = fetch_data(market, limit=1)
    if not df.empty:
        latest = df.iloc[-1]
        logger.info(f"Latest {market}: Close={latest['close']:.2f}, Volatility={latest['volatility']:.4f}")
        print(f"Latest {market}: Close={latest['close']:.2f}, Volatility={latest['volatility']:.4f}")
    return df

def fetch_all_data(markets=TRADING_MARKETS):
    """Fetch and save data for all markets."""
    available_markets = get_available_markets()
    for market in markets:
        if market in available_markets or market.replace('USD', 'USDT') in available_markets:
            df = fetch_data(market)
            if not df.empty:
                save_to_db(df, market)
        else:
            logger.warning(f"Market {market} not available")
            print(f"Market {market} not available")

if __name__ == "__main__":
    # Test markets and fetch all
    available_markets = get_available_markets()
    fetch_all_data()
    hist_df = fetch_data('BTC-USD', limit=100)
    if not hist_df.empty:
        save_to_db(hist_df, 'BTC-USD')
        print("Sample BTC-USD data:\n", hist_df.tail())
        hist_df.to_csv('btc_sample.csv')
    get_live_data('BTC-USD')