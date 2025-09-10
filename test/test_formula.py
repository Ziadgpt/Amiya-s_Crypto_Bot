import pytest
import pandas as pd
import numpy as np
import sqlite3
from models.formula_7 import calculate_formula_7, backtest_formula_7


def test_calculate_formula_7():
    # Create sample DataFrame with varied data to trigger signals
    data = {
        'started_at': pd.date_range(start='2025-09-10', periods=100, freq='5min'),
        'close': [100 + np.sin(i / 10) * 10 for i in range(100)],  # Sinusoidal for variation
        'open': [100 + np.sin(i / 10) * 9 for i in range(100)],
        'high': [100 + np.sin(i / 10) * 11 for i in range(100)],
        'low': [100 + np.sin(i / 10) * 8 for i in range(100)],
        'base_token_volume': [1000] * 100
    }
    df = pd.DataFrame(data)
    df['log_returns'] = np.log(df['close'] / df['close'].shift(1)).fillna(0)
    df['volatility'] = df['log_returns'].rolling(window=20).std().fillna(0)

    # Apply Formula 7
    result = calculate_formula_7(df)

    # Assertions
    assert 'f7_value' in result.columns, "f7_value column missing"
    assert 'f7_signal' in result.columns, "f7_signal column missing"
    assert result['f7_signal'].isin([0, 1, -1]).all(), "Invalid signal values"
    assert len(result) == 100, "DataFrame length changed"


def test_backtest_formula_7():
    # Create temporary SQLite database
    conn = sqlite3.connect(':memory:')
    data = {
        'started_at': pd.date_range(start='2025-09-10', periods=100, freq='5min'),
        'close': [100 + np.sin(i / 10) * 10 for i in range(100)],
        'open': [100 + np.sin(i / 10) * 9 for i in range(100)],
        'high': [100 + np.sin(i / 10) * 11 for i in range(100)],
        'low': [100 + np.sin(i / 10) * 8 for i in range(100)],
        'base_token_volume': [1000] * 100,
        'log_returns': [0.001] * 100,
        'volatility': [0.01] * 100
    }
    df = pd.DataFrame(data)
    df.to_sql('BTC_USD_data', conn, index=False, if_exists='replace')

    # Run backtest
    result = backtest_formula_7('BTC-USD', db_name=':memory:', conn=conn)
    conn.close()

    # Assertions
    assert not result.empty, "Backtest returned empty DataFrame"
    assert 'f7_value' in result.columns, "f7_value column missing"
    assert 'f7_signal' in result.columns, "f7_signal column missing"
    assert result['f7_signal'].isin([0, 1, -1]).all(), "Invalid signal values"