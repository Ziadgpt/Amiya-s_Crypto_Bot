import streamlit as st
import pandas as pd
import sqlite3
from data_pipeline import fetch_data
from config import TRADING_MARKETS
from utils.logger import setup_logger

# Setup logger
logger = setup_logger('dashboard', 'dashboard.log')

st.title("Crypto Bot Dashboard")
market = st.selectbox("Select Market", TRADING_MARKETS)

# Try fetching live data
try:
    raw_response = fetch_data(market, limit=50, return_raw=True)
    logger.info(f"Raw API response for {market}: {raw_response}")
    df = fetch_data(market, limit=50)
    if df.empty or 'started_at' not in df.columns:
        logger.warning(f"No live data or missing 'started_at' for {market}")
        st.warning(f"No live data for {market}, trying SQLite database...")
        # Fallback to SQLite
        try:
            conn = sqlite3.connect('crypto_data.db')
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({market.replace('-', '_')}_data)")
            table_info = cursor.fetchall()
            logger.info(f"Table info for {market}: {table_info}")
            st.write(f"Table info for {market}: {table_info}")
            df = pd.read_sql(f"SELECT * FROM {market.replace('-', '_')}_data", conn)
            conn.close()
            if df.empty or 'started_at' not in df.columns:
                logger.error(f"No data or missing 'started_at' in SQLite for {market}")
                st.error(f"No data available for {market} in database")
            else:
                logger.info(f"Loaded {len(df)} rows from SQLite for {market}")
                st.success(f"Loaded {len(df)} rows from SQLite for {market}")
        except Exception as e:
            logger.error(f"SQLite error for {market}: {e}")
            st.error(f"Database error: {e}")
            df = pd.DataFrame()
    else:
        logger.info(f"Fetched {len(df)} live rows for {market}")
        st.success(f"Fetched {len(df)} live rows for {market}")
except Exception as e:
    logger.error(f"Error fetching live data for {market}: {e}")
    st.error(f"Error fetching live data: {e}")
    df = pd.DataFrame()

# Display data if available
if not df.empty and 'started_at' in df.columns:
    st.metric("Latest Close", f"${df['close'].iloc[-1]:.2f}")
    st.metric("Volatility", f"{df['volatility'].iloc[-1]:.4f}")
    st.line_chart(df[['open', 'close']].set_index('started_at'), height=300, use_container_width=True)
    st.line_chart(df[['volatility']].set_index('started_at'), height=200, use_container_width=True)

    # Load and display model signals
    try:
        conn = sqlite3.connect('crypto_data.db')
        df_signals = pd.read_sql(
            f"SELECT started_at, f7_signal, regime, hmm_signal FROM {market.replace('-', '_')}_data", conn)
        conn.close()
        if not df_signals.empty:
            st.write("Formula 7 Signals", df_signals['f7_signal'].value_counts())
            st.write("HMM Regimes & Signals", df_signals[['regime', 'hmm_signal']].value_counts())
            st.write("Sample Data with Signals", df_signals.tail())
        else:
            logger.warning(f"No signals data for {market}")
            st.warning(f"No signals data for {market}")
    except Exception as e:
        logger.warning(f"Could not load signals for {market}: {e}")
        st.warning(f"Could not load signals: {e}")
else:
    logger.error(f"No valid data for {market}")
    st.error(f"No valid data for {market}")