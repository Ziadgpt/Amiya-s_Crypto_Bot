import pandas as pd
from utils.logger import setup_logger
import sqlite3

logger = setup_logger('bollinger_bands', 'bollinger_bands.log')

def calculate_bollinger_bands(df, window=20, num_std=2):
    """Calculate Bollinger Bands and generate signals."""
    logger.info(f"Applying Bollinger Bands to {len(df)} rows with window={window}, num_std={num_std}")
    df['ma20'] = df['close'].rolling(window=window).mean()
    df['std20'] = df['close'].rolling(window=window).std()
    df['upper_band'] = df['ma20'] + (df['std20'] * num_std)
    df['lower_band'] = df['ma20'] - (df['std20'] * num_std)
    df['bb_signal'] = 0
    df.loc[df['close'] > df['upper_band'], 'bb_signal'] = -1  # Sell
    df.loc[df['close'] < df['lower_band'], 'bb_signal'] = 1  # Buy
    return df

def backtest_bollinger_bands(market='BTC-USD', db_name='crypto_data.db'):
    """Backtest Bollinger Bands on market data."""
    try:
        conn = sqlite3.connect(db_name, isolation_level=None)
        df = pd.read_sql(f"SELECT * FROM {market.replace('-', '_')}_data", conn)
        conn.close()
        if df.empty or 'started_at' not in df.columns:
            logger.error(f"No data or missing 'started_at' for {market} in {db_name}")
            return pd.DataFrame()
        df = calculate_bollinger_bands(df)
        logger.info(f"Backtested Bollinger Bands for {market}: {df['bb_signal'].value_counts().to_dict()}")
        print(f"Bollinger Bands signals for {market}: {df['bb_signal'].value_counts().to_dict()}")
        conn = sqlite3.connect(db_name, isolation_level=None)
        df.to_sql(f"{market.replace('-', '_')}_data", conn, if_exists='replace', index=False)
        conn.close()
        logger.info(f"Saved {len(df)} rows with BB signals for {market} to {db_name}")
        return df
    except Exception as e:
        logger.error(f"Error backtesting Bollinger Bands for {market}: {e}")
        print(f"Error backtesting Bollinger Bands for {market}: {e}")
        return pd.DataFrame()