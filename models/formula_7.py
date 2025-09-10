import pandas as pd
import numpy as np
from utils.logger import setup_logger
import sqlite3

logger = setup_logger('formula_7', 'formula_7.log')

def calculate_formula_7(df, n=20):
    """Apply Formula 7 indicator: ((ph * rh) - (pl * rl)) / n"""
    logger.info(f"Applying Formula 7 to {len(df)} rows with n={n}")
    df['returns'] = df['close'].pct_change()
    formula_values = []
    for i in range(len(df)):
        if i < n-1:
            formula_values.append(0)
            continue
        window = df['returns'].iloc[i-n+1:i+1]
        pos_returns = window[window > 0]
        neg_returns = window[window < 0]
        ph = len(pos_returns) / n
        rh = pos_returns.mean() if not pos_returns.empty else 0
        pl = len(neg_returns) / n
        rl = abs(neg_returns).mean() if not neg_returns.empty else 0
        formula = ((ph * rh) - (pl * rl)) / n
        formula_values.append(formula)
    df['f7_value'] = formula_values
    df['f7_signal'] = 0
    df.loc[df['f7_value'] > 1e-8, 'f7_signal'] = 1  # Buy
    df.loc[df['f7_value'] < -1e-8, 'f7_signal'] = -1  # Sell
    return df

def backtest_formula_7(market='BTC-USD', db_name='crypto_data.db', conn=None):
    """Backtest Formula 7 on market data."""
    try:
        if conn is None:
            conn = sqlite3.connect(db_name)
            close_conn = True
        else:
            close_conn = False
        df = pd.read_sql(f"SELECT * FROM {market.replace('-', '_')}_data", conn)
        if close_conn:
            conn.close()
        if df.empty or 'started_at' not in df.columns:
            logger.error(f"No data or missing 'started_at' for {market} in {db_name}")
            return pd.DataFrame()
        df = calculate_formula_7(df)
        logger.info(f"Backtested Formula 7 for {market}: {df['f7_signal'].value_counts().to_dict()}")
        print(f"Formula 7 signals for {market}: {df['f7_signal'].value_counts().to_dict()}")
        try:
            conn = sqlite3.connect(db_name)
            df.to_sql(f"{market.replace('-', '_')}_data", conn, if_exists='replace', index=False)
            conn.close()
            logger.info(f"Saved {len(df)} rows with Formula 7 signals for {market} to {db_name}")
        except Exception as e:
            logger.error(f"Error saving Formula 7 signals for {market}: {e}")
        return df
    except Exception as e:
        logger.error(f"Error backtesting Formula 7 for {market}: {e}")
        print(f"Error backtesting Formula 7 for {market}: {e}")
        return pd.DataFrame()