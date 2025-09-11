import streamlit as st
import pandas as pd
import sqlite3
from data_pipeline import fetch_data
from config import TRADING_MARKETS
from utils.logger import setup_logger
from models.lstm_model import backtest_lstm
from models.bollinger_bands import backtest_bollinger_bands

logger = setup_logger('dashboard', 'dashboard.log')

st.title("Crypto Bot Dashboard")
market = st.selectbox("Select Market", TRADING_MARKETS)

df = pd.DataFrame()

try:
    conn = sqlite3.connect('crypto_data.db', isolation_level=None)
    table_name = market.replace('-', '_') + '_data'
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()
    if df.empty or 'started_at' not in df.columns:
        logger.warning(f"No data or missing 'started_at' in SQLite for {market}")
        st.warning(f"No usable data in SQLite for {market}. Falling back to live API fetch...")
        try:
            raw_response = fetch_data(market, limit=50, return_raw=True)
            logger.info(f"Raw API response for {market}: {raw_response}")
            df = fetch_data(market, limit=50)
            if df.empty or 'started_at' not in df.columns:
                logger.error(f"Live fetch for {market} failed. Columns: {df.columns.tolist() if not df.empty else 'Empty'}")
                st.error(f"Failed to fetch valid live data for {market}.")
                st.subheader("Debug Info: Live Fetch DataFrame")
                st.write("Columns available:", df.columns.tolist() if not df.empty else "DataFrame is empty")
                st.dataframe(df.head())
            else:
                logger.info(f"Fetched {len(df)} live rows for {market}")
                st.success(f"Fetched {len(df)} live rows for {market}")
                conn = sqlite3.connect('crypto_data.db', isolation_level=None)
                df.to_sql(table_name, conn, if_exists='replace', index=False)
                conn.close()
        except Exception as e:
            logger.error(f"Error fetching live data for {market}: {e}")
            st.error(f"Error fetching live data: {e}")
            df = pd.DataFrame()
    else:
        logger.info(f"Loaded {len(df)} rows from SQLite for {market}")
        st.success(f"Loaded {len(df)} rows from SQLite for {market}")
except Exception as e:
    logger.error(f"SQLite error for {market}: {e}")
    st.error(f"Database error: {e}")
    df = pd.DataFrame()

if not df.empty and 'started_at' in df.columns:
    df['started_at'] = pd.to_datetime(df['started_at'])
    df_for_plotting = df.set_index('started_at')
    st.subheader("Market Data")
    st.metric("Latest Close", f"${df['close'].iloc[-1]:.2f}")
    st.metric("Volatility", f"{df['volatility'].iloc[-1]:.4f}")
    st.line_chart(df_for_plotting[['open', 'close']], height=300, use_container_width=True, color=["#FF0000", "#00FF00"])
    st.line_chart(df_for_plotting[['volatility']], height=200, use_container_width=True, color="#0000FF")
    try:
        conn = sqlite3.connect('crypto_data.db', isolation_level=None)
        available_columns = pd.read_sql(f"PRAGMA table_info({table_name})", conn)['name'].tolist()
        signal_columns = [col for col in ['f7_signal', 'regime', 'hmm_signal', 'lstm_signal', 'bb_signal'] if col in available_columns]
        if signal_columns:
            df_signals = pd.read_sql(f"SELECT started_at, {', '.join(signal_columns)} FROM {table_name}", conn)
            conn.close()
            if not df_signals.empty:
                st.subheader("Trading Signals")
                if 'f7_signal' in signal_columns:
                    st.write("Formula 7 Signals", df_signals['f7_signal'].value_counts())
                if 'regime' in signal_columns and 'hmm_signal' in signal_columns:
                    st.write("HMM Regimes & Signals", df_signals[['regime', 'hmm_signal']].value_counts())
                if 'lstm_signal' in signal_columns:
                    st.write("LSTM Signals", df_signals['lstm_signal'].value_counts())
                if 'bb_signal' in signal_columns:
                    st.write("Bollinger Bands Signals", df_signals['bb_signal'].value_counts())
                # Fixed signal chart
                signal_plot_columns = [col for col in ['f7_signal', 'hmm_signal', 'lstm_signal', 'bb_signal'] if col in df_signals.columns]
                if signal_plot_columns:
                    df_signals['started_at'] = pd.to_datetime(df_signals['started_at'])
                    st.line_chart(df_signals.set_index('started_at')[signal_plot_columns], height=200, use_container_width=True, color=["#FFA500", "#800080", "#008000", "#0000FF"])
                st.write("Sample Data with Signals", df_signals.tail())
            else:
                logger.warning(f"No signals data for {market}")
                st.warning(f"No signals data for {market}")
        else:
            logger.warning(f"No signal columns in {table_name}")
            st.warning(f"No signal columns available for {market}")
    except Exception as e:
        logger.warning(f"Could not load signals for {market}: {e}")
        st.warning(f"Could not load signals: {e}")
else:
    logger.error(f"Dashboard render failed: No valid data with 'started_at' for {market}")
    st.error(f"No valid data could be loaded for {market} to display charts.")
    if not df.empty:
        st.subheader("Debug Info: Final DataFrame State")
        st.write("Columns available:", df.columns.tolist())
        st.dataframe(df)