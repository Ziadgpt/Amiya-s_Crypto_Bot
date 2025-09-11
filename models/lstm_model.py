import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from keras.models import Sequential
from keras.layers import LSTM, Dense
from keras.callbacks import EarlyStopping
from utils.logger import setup_logger
import sqlite3
import tensorflow as tf
import os

# Disable TensorFlow threading to avoid mutex issues
os.environ['TF_NUM_INTEROP_THREADS'] = '1'
os.environ['TF_NUM_INTRAOP_THREADS'] = '1'
tf.config.threading.set_intra_op_parallelism_threads(1)
tf.config.threading.set_inter_op_parallelism_threads(1)

logger = setup_logger('lstm_model', 'lstm_model.log')


# In models/lstm_model.py
# (Add the new features to the list)
def prepare_lstm_data(df, lookback=60):
    """Prepare data for LSTM (normalize and create sequences)."""
    logger.info(f"Preparing LSTM data for {len(df)} rows with lookback={lookback}")

    # --- MODIFIED: ADD NEW FEATURES ---
    # The new features will be added here
    feature_cols = ['open', 'high', 'low', 'close', 'base_token_volume', 'log_returns', 'volatility', 'f7_value',
                    'hmm_signal', 'bb_signal']
    # ---------------------------------

    # Check if all features are in the DataFrame
    missing_cols = [col for col in feature_cols if col not in df.columns]
    if missing_cols:
        logger.error(f"Missing columns for LSTM: {missing_cols}")
        raise ValueError(f"Missing columns for LSTM: {missing_cols}")

    scaler = MinMaxScaler()
    scaled_data = scaler.fit_transform(df[feature_cols])

    X, y = [], []
    for i in range(lookback, len(scaled_data)):
        X.append(scaled_data[i - lookback:i])
        y.append(scaled_data[i, 3])  # Predict 'close'

    X, y = np.array(X), np.array(y)
    if X.shape[0] == 0:
        logger.error("No sequences created for LSTM training")
        return X, y, scaler

    X = X.reshape((X.shape[0], X.shape[1], X.shape[2]))
    return X, y, scaler
def train_lstm(X, y, epochs=50, batch_size=32):
    """Train LSTM model."""
    logger.info(f"Training LSTM on {X.shape[0]} samples")
    model = Sequential()
    model.add(LSTM(50, return_sequences=True, input_shape=(X.shape[1], X.shape[2])))
    model.add(LSTM(50, return_sequences=False))
    model.add(Dense(25))
    model.add(Dense(1))
    model.compile(optimizer='adam', loss='mean_squared_error')
    early_stop = EarlyStopping(monitor='loss', patience=5, restore_best_weights=True)
    model.fit(X, y, epochs=epochs, batch_size=batch_size, verbose=0, callbacks=[early_stop])
    return model

def predict_lstm(model, X, scaler, df, lookback=60):
    """Predict future prices and generate signals."""
    logger.info(f"Predicting with LSTM for {len(X)} sequences")
    if X.shape[0] < lookback:
        logger.error("Insufficient sequences for prediction")
        return df, None
    X_scaled = X[-lookback:].reshape(1, lookback, X.shape[2])
    predicted = model.predict(X_scaled, verbose=0)
    predicted_price = scaler.inverse_transform(np.concatenate([np.zeros((1, X.shape[2]-1)), predicted], axis=1))[:, -1][0]
    current_close = df['close'].iloc[-1]
    lstm_signal = 0
    lstm_signal = 1 if predicted_price > current_close * 1.001 else -1 if predicted_price < current_close * 0.999 else 0
    df.loc[df.index[-1], 'lstm_signal'] = lstm_signal
    logger.info(f"LSTM prediction for {df['started_at'].iloc[-1]}: {predicted_price:.2f}, Signal: {lstm_signal}")
    print(f"LSTM prediction for {df['started_at'].iloc[-1]}: {predicted_price:.2f}, Signal: {lstm_signal}")
    return df, predicted_price

def backtest_lstm(market='BTC-USD', db_name='crypto_data.db'):
    """Backtest LSTM on market data."""
    try:
        conn = sqlite3.connect(db_name, isolation_level=None)
        df = pd.read_sql(f"SELECT * FROM {market.replace('-', '_')}_data", conn)
        conn.close()
        if df.empty or 'started_at' not in df.columns:
            logger.error(f"No data or missing 'started_at' for {market} in {db_name}")
            return pd.DataFrame()
        if len(df) < 60:
            logger.error(f"Insufficient data rows ({len(df)}) for LSTM training for {market}")
            return df
        feature_cols = ['open', 'high', 'low', 'close', 'base_token_volume', 'log_returns', 'volatility']
        X, y, scaler = prepare_lstm_data(df, lookback=60)
        if len(X) == 0:
            logger.error(f"Insufficient sequences for LSTM training for {market}")
            return df
        model = train_lstm(X, y)
        df, predicted_price = predict_lstm(model, X, scaler, df)
        logger.info(f"Backtested LSTM for {market}: Predicted price {predicted_price:.2f}")
        print(f"Backtested LSTM for {market}: Predicted price {predicted_price:.2f}")
        conn = sqlite3.connect(db_name, isolation_level=None)
        df.to_sql(f"{market.replace('-', '_')}_data", conn, if_exists='replace', index=False)
        conn.close()
        logger.info(f"Saved {len(df)} rows with LSTM signals for {market} to {db_name}")
        return df
    except Exception as e:
        logger.error(f"Error backtesting LSTM for {market}: {e}")
        print(f"Error backtesting LSTM for {market}: {e}")
        return pd.DataFrame()