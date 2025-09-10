# models/hmm_regime.py
import pandas as pd
from hmmlearn.hmm import GaussianHMM
from utils.logger import setup_logger

logger = setup_logger('hmm_regime', 'hmm_regime.log')

def train_hmm(df, n_states=3):
    """Train HMM to detect market regimes."""
    logger.info(f"Training HMM on {len(df)} rows with {n_states} states")
    model = GaussianHMM(n_components=n_states, covariance_type="full", n_iter=100)
    X = df[['log_returns', 'volatility']].values
    model.fit(X)
    regimes = model.predict(X)
    df['regime'] = regimes
    logger.info(f"HMM regimes: {df['regime'].value_counts()}")
    return df, model

def backtest_hmm(market='BTC-USD', db_name='crypto_data.db'):
    """Backtest HMM on market data."""
    conn = sqlite3.connect(db_name)
    df = pd.read_sql(f"SELECT * FROM {market.replace('-', '_')}_data", conn)
    conn.close()
    df, model = train_hmm(df)
    logger.info(f"Backtested HMM for {market}")
    return df