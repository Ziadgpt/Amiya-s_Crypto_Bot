import pandas as pd
import numpy as np
import sqlite3
from utils.logger import setup_logger

logger = setup_logger('formula_7', 'formula_7.log')


def calculate_formula_7(df, lookback=20):
    """
    Calculates the custom "Formula 7" indicator.
    Formula: ((Prob of high x ret of high) - (Prob of low x ret of low)) / n
    """
    logger.info(f"Calculating Formula 7 for {len(df)} rows with lookback={lookback}")

    # Calculate daily returns for direction and magnitude
    df['returns'] = df['close'].pct_change()

    # Use a rolling window to calculate the components
    df['positive_returns'] = df['returns'].apply(lambda x: x if x > 0 else 0)
    df['negative_returns'] = df['returns'].apply(lambda x: x if x < 0 else 0)

    # Calculate "Prob of high" and "Prob of low"
    df['prob_high'] = df['positive_returns'].rolling(window=lookback).apply(lambda x: (x > 0).sum() / lookback,
                                                                            raw=True)
    df['prob_low'] = df['negative_returns'].rolling(window=lookback).apply(lambda x: (x < 0).sum() / lookback, raw=True)

    # Calculate "ret of high" and "ret of low"
    df['ret_high'] = df['positive_returns'].rolling(window=lookback).apply(
        lambda x: x[x > 0].mean() if (x > 0).sum() > 0 else 0, raw=True)
    df['ret_low'] = df['negative_returns'].rolling(window=lookback).apply(
        lambda x: x[x < 0].mean() if (x < 0).sum() > 0 else 0, raw=True)

    # Calculate the final Formula 7 value
    df['f7_value'] = ((df['prob_high'] * df['ret_high']) - (
                df['prob_low'] * df['ret_low'])) / 2  # Using n=2 as a default

    # Generate signals based on the value
    df['f7_signal'] = 0
    df.loc[df['f7_value'] > 0.0001, 'f7_signal'] = 1  # Buy
    df.loc[df['f7_value'] < -0.0001, 'f7_signal'] = -1  # Sell

    return df


def backtest_formula_7(market='BTC-USD', db_name='crypto_data.db'):
    """Backtest Formula 7 on market data."""
    try:
        conn = sqlite3.connect(db_name)
        df = pd.read_sql(f"SELECT * FROM {market.replace('-', '_')}_data", conn)
        conn.close()

        if df.empty or 'started_at' not in df.columns:
            logger.error(f"No data or missing 'started_at' for {market}")
            return pd.DataFrame()

        df = calculate_formula_7(df)
        df.loc[df['f7_value'].isnull(), 'f7_value'] = 0
        df.loc[df['f7_signal'].isnull(), 'f7_signal'] = 0

        conn = sqlite3.connect(db_name)
        df.to_sql(f"{market.replace('-', '_')}_data", conn, if_exists='replace', index=False)
        conn.close()
        logger.info(f"Saved {len(df)} rows with Formula 7 signals for {market} to {db_name}")
        return df

    except Exception as e:
        logger.error(f"Error backtesting Formula 7 for {market}: {e}")
        return pd.DataFrame()