import pytest
import pandas as pd
import numpy as np
from models.lstm_model import prepare_lstm_data, train_lstm, predict_lstm

def test_prepare_lstm_data():
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
    X, y, scaler = prepare_lstm_data(df, lookback=60)
    assert X.shape[0] == 40, "Incorrect X shape"
    assert y.shape[0] == 40, "Incorrect y shape"
    assert X.shape[1] == 60, "Incorrect lookback"
    assert X.shape[2] == 7, "Incorrect feature count"

def test_train_lstm():
    X = np.random.random((40, 60, 7))
    y = np.random.random(40)
    model = train_lstm(X, y)
    assert model is not None, "Model training failed"

def test_predict_lstm():
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
    X, y, scaler = prepare_lstm_data(df, lookback=60)
    model = train_lstm(X, y)
    df, predicted_price = predict_lstm(model, X, scaler, df)
    assert 'lstm_signal' in df.columns, "lstm_signal column missing"
    assert df['lstm_signal'].isin([0, 1, -1]).all(), "Invalid LSTM signal values"