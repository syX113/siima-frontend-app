import streamlit as st
from pymongo import MongoClient
import pandas as pd
import altair as alt
from datetime import datetime, timedelta
import pytz
import os

def fetch_data():
    mongo_connection_string = os.getenv("MONGO_CONNECTION_STRING")
    client = MongoClient(mongo_connection_string)
    db = client['cosmos-db-siima-telemetry']
    collection = db['cosmos-collection-siima-telemetry']
    # Specify the projection to only include the fields you want
    projection = {'DeviceMessageTimestamp': 1, 'Battery1Charge': 1, 'Energy1Consumption': 1}
    data = list(collection.find(projection=projection).sort('DeviceMessageTimestamp', 1))
    return pd.DataFrame(data)

def filter_data_by_timestamp(df, start_ts, end_ts):
    df = df.set_index('DeviceMessageTimestamp')
    df.index = pd.to_datetime(df.index, utc=True).tz_convert('Europe/Zurich')
    return df.loc[start_ts:end_ts].reset_index()

def calculate_energy_account(df):
    df['Energy Consumption (kWh)'] = pd.to_numeric(df['Battery1Charge'], errors='coerce')
    df['Energy Production (kWh)'] = pd.to_numeric(df['Energy1Consumption'], errors='coerce')
    df['Energy Account (kWh)'] = (
        df['Energy Consumption (kWh)'].shift(-1) - df['Energy Production (kWh)'].shift(-1) -
        df['Energy Consumption (kWh)'] + df['Energy Production (kWh)']
    ).fillna(0)
    return df

def get_yesterday_hourly_last_data(df):
    if not pd.api.types.is_datetime64_any_dtype(df.index):
        df['DeviceMessageTimestamp'] = pd.to_datetime(df['DeviceMessageTimestamp'])
        df.set_index('DeviceMessageTimestamp', inplace=True)
    
    df_hourly_last = df.groupby(df.index.floor('h')).last().reset_index()
    return df_hourly_last


def get_time_range_for_yesterday(timezone='Europe/Zurich'):
    tz = pytz.timezone(timezone)
    today = datetime.now(tz).date()
    start_yesterday = datetime.combine(today - timedelta(days=1), datetime.min.time(), tzinfo=tz)
    end_yesterday = datetime.combine(today, datetime.min.time(), tzinfo=tz) - timedelta(seconds=1)
    return start_yesterday, end_yesterday

def process_data():
    df = fetch_data()
    df['DeviceMessageTimestamp'] = pd.to_datetime(df['DeviceMessageTimestamp'], utc=False).dt.tz_localize('Europe/Zurich')
    df = calculate_energy_account(df)
    return df

# Main app logic
st.title('Siima - Swiss Energy Account')

css="""
<style>
    [data-testid="stForm"] {
        background: LightBlue;
    }
</style>
"""
st.write(css, unsafe_allow_html=True)

# Process data
df_processed = process_data()

# Get the range for yesterday
start_yesterday, end_yesterday = get_time_range_for_yesterday()

# Filter data for the last hour and yesterday
df_last_hour = filter_data_by_timestamp(df_processed, datetime.now(pytz.timezone('Europe/Zurich')) - timedelta(hours=1), datetime.now(pytz.timezone('Europe/Zurich')))
df_yesterday_hourly = filter_data_by_timestamp(df_processed, start_yesterday, end_yesterday)

# Get the last entry for each hour of yesterday
df_yesterday_hourly_last = get_yesterday_hourly_last_data(df_yesterday_hourly)

# Plotting the last hour data
if not df_last_hour.empty:
    last_hour_chart = alt.Chart(df_last_hour).mark_line(color='#42c0b1').encode(
        x=alt.X('DeviceMessageTimestamp:T', title='Minute'),
        y=alt.Y('Energy Account (kWh):Q', title='Energy Account (kWh)'),
        tooltip=['DeviceMessageTimestamp:T', 'Energy Account (kWh):Q']  
    ).properties(
        width=800,
        height=400,
        background='#161b24',
        title='Energy Movements - Last Hour'
    ).interactive()
    st.altair_chart(last_hour_chart, use_container_width=True)

# Plotting the yesterday's hourly data
if not df_yesterday_hourly_last.empty:
    yesterday_chart = alt.Chart(df_yesterday_hourly_last).mark_line(color='#42c0b1').encode(
        x=alt.X('DeviceMessageTimestamp:T', title='Hour'),
        y=alt.Y('Energy Account (kWh):Q', title='Energy Account (kWh)'),
        tooltip=['DeviceMessageTimestamp:T', 'Energy Account (kWh):Q']
    ).properties(
        width=800,
        height=400,
        background='#161b24',
        title='Energy Movements - Yesterday'
    ).interactive()
    st.altair_chart(yesterday_chart, use_container_width=True)
