import pytest
import pandas as pd
import numpy as np
import sqlite3
from models.hmm_regime import train_hmm, backtest_hmm


def test_train_hmm():
    # Create sample DataFrame
    data = {
        'started_at': pd.date_range(start='2025-09-10', periods=100, freq='5min'),
        'close': [100 + i * 0.1 for i in range(100)],
        'open': [100 + i * 0.09 for i in range(100)],
        'high': [100 + i * 0.11 for i in range(100)],
        'low': [100 + i * 0.08 for i in range(100)],
        'base_token_volume': [1000] * 100,
        'log_returns': [0.001] * 100,
        'volatility': [0.01] * 100
    }
    df = pd.DataFrame(data)

    # Train HMM
    result, model = train_hmm(df, n_states=3)

    # Assertions
    assert 'regime' in result.columns, "regime column missing"
    assert 'hmm_signal' in result.columns, "hmm_signal column missing"
    assert result['hmm_signal'].isin([0, 1, -1]).all(), "Invalid signal values"
    assert len(result) == 100, "DataFrame length changed"
    assert model is not None, "HMM model not trained"


def test_backtest_hmm():
    # Create temporary SQLite database
    conn = sqlite3.connect(':memory:')
    data = {
        'started_at': pd.date_range(start='2025-09-10', periods=100, freq='5min'),
        'close': [100 + i * 0.1 for i in range(100)],
        'open': [100 + i * 0.09 for i in range(100)],
        'high': [100 + i * 0.11 for i in range(100)],
        'low': [100 + i * 0.08 for i in range(100)],
        'base_token_volume': [1000] * 100,
        'log_returns': [0.001] * 100,
        'volatility': [0.01] * 100
    }
    df = pd.DataFrame(data)
    df.to_sql('BTC_USD_data', conn, index=False, if_exists='replace')

    # Run backtest
    result = backtest_hmm('BTC-USD', db_name=':memory:', conn=conn)
    conn.close()

    # Assertions
    assert not result.empty, "Backtest returned empty DataFrame"
    assert 'regime' in result.columns, "regime column missing"
    assert 'hmm_signal' in result.columns, "hmm_signal column missing"
    assert result['hmm_signal'].isin([0, 1, -1]).all(), "Invalid signal values"