import pandas as pd
import numpy as np
from hmmlearn.hmm import GaussianHMM
from utils.logger import setup_logger
import sqlite3

logger = setup_logger('hmm_regime', 'hmm_regime.log')

def train_hmm(df, n_states=2):
    """Train HMM to detect market regimes and generate signals."""
    logger.info(f"Training HMM on {len(df)} rows with {n_states} states")
    try:
        X = df[['log_returns', 'volatility']].copy()
        X['log_returns'] = X['log_returns'] + np.random.normal(0, 1e-6, len(X))
        X['volatility'] = X['volatility'] + np.random.normal(0, 1e-6, len(X))
        X['log_returns'] = (X['log_returns'] - X['log_returns'].mean()) / (X['log_returns'].std() + 1e-8)
        X['volatility'] = (X['volatility'] - X['volatility'].mean()) / (X['volatility'].std() + 1e-8)
        X = X.values
        if len(X) < n_states * 2:
            logger.error(f"Insufficient data for HMM: {len(X)} rows")
            return df, None
        model = GaussianHMM(n_components=n_states, covariance_type="diag", n_iter=200, init_params='stmc', random_state=42)
        model.fit(X)
        regimes = model.predict(X)
        df['regime'] = regimes
        df['hmm_signal'] = 0
        df.loc[df['regime'] == 0, 'hmm_signal'] = 1  # Buy
        df.loc[df['regime'] == 1, 'hmm_signal'] = -1  # Sell
        logger.info(f"HMM regimes for {len(df)} rows: {df['regime'].value_counts().to_dict()}")
        logger.info(f"HMM signals: {df['hmm_signal'].value_counts().to_dict()}")
        print(f"HMM regimes: {df['regime'].value_counts().to_dict()}")
        print(f"HMM signals: {df['hmm_signal'].value_counts().to_dict()}")
        return df, model
    except Exception as e:
        logger.error(f"Error training HMM: {e}")
        print(f"Error training HMM: {e}")
        return df, None

def backtest_hmm(market='BTC-USD', db_name='crypto_data.db', conn=None):
    """Backtest HMM on market data."""
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
        df, model = train_hmm(df)
        if model is None:
            return df
        logger.info(f"Backtested HMM for {market}: {df['hmm_signal'].value_counts().to_dict()}")
        print(f"Backtested HMM for {market}: {df['hmm_signal'].value_counts().to_dict()}")
        conn = sqlite3.connect(db_name)
        df.to_sql(f"{market.replace('-', '_')}_data", conn, if_exists='replace', index=False)
        conn.close()
        logger.info(f"Saved {len(df)} rows with HMM signals for {market} to {db_name}")
        return df
    except Exception as e:
        logger.error(f"Error backtesting HMM for {market}: {e}")
        print(f"Error backtesting HMM for {market}: {e}")
        return pd.DataFrame()