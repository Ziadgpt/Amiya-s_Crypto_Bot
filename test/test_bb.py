import pytest
import pandas as pd
from models.bollinger_bands import calculate_bollinger_bands, backtest_bollinger_bands


def test_calculate_bollinger_bands():
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
    result = calculate_bollinger_bands(df)
    assert 'ma20' in result.columns, "ma20 column missing"
    assert 'upper_band' in result.columns, "upper_band column missing"
    assert 'lower_band' in result.columns, "lower_band column missing"
    assert 'bb_signal' in result.columns, "bb_signal column missing"
    assert result['bb_signal'].isin([0, 1, -1]).all(), "Invalid BB signal values"


def test_backtest_bollinger_bands():
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
    result = backtest_bollinger_bands('BTC-USD', db_name=':memory:', conn=conn)
    conn.close()

    # Assertions
    assert not result.empty, "Backtest returned empty DataFrame"
    assert 'bb_signal' in result.columns, "bb_signal column missing"
    assert result['bb_signal'].isin([0, 1, -1]).all(), "Invalid BB signal values"