# models/hmm_regime.py
import sqlite3

import pandas as pd
from hmmlearn.hmm import GaussianHMM
from utils.logger import setup_logger
logger = setup_logger('hmm_regime', 'hmm_regime.log')

def train_hmm(df, n_states=3):
    logger.info(f"Training HMM on {len(df)} rows with {n_states} states")
    model = GaussianHMM(n_components=n_states, covariance_type="full", n_iter=100)
    X = df[['log_returns', 'volatility']].values
    model.fit(X)
    regimes = model.predict(X)
    df['regime'] = regimes
    # Example: Buy for regime 0, sell for regime 2
    df['hmm_signal'] = 0
    df.loc[df['regime'] == 0, 'hmm_signal'] = 1  # Buy
    df.loc[df['regime'] == 2, 'hmm_signal'] = -1  # Sell
    logger.info(f"HMM regimes: {df['regime'].value_counts()}")
    return df, model

def backtest_hmm(market='BTC-USD', db_name='crypto_data.db'):
    conn = sqlite3.connect(db_name)
    df = pd.read_sql(f"SELECT * FROM {market.replace('-', '_')}_data", conn)
    conn.close()
    df, model = train_hmm(df)
    logger.info(f"Backtested HMM for {market}: {df['hmm_signal'].value_counts()}")
    return df