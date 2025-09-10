# dashboard.py
import streamlit as st
import pandas as pd
import sqlite3
from data_pipeline import fetch_data

st.title("Crypto Bot Dashboard")
market = st.selectbox("Select Market", ['BTC-USD', 'ETH-USD', 'SOL-USD', 'ADA-USD', 'XRP-USD'])
df = fetch_data(market, limit=50)
if not df.empty:
    st.line_chart(df[['open', 'close']].set_index('started_at'))
    st.write("Sample Data", df.tail())