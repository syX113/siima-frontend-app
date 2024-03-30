import streamlit as st
from pymongo import MongoClient
import os
from datetime import datetime, timedelta
import pandas as pd
import altair as alt

# Function to set up MongoDB connection and fetch data
def fetch_data():
    # Load the connection string from an environment variable
    mongo_connection_string = os.getenv("MONGO_CONNECTION_STRING")

    # Connect to the MongoDB client
    client = MongoClient(mongo_connection_string)

    # Connect to your database - replace with your actual database name
    db = client['cosmos-db-siima-telemetry']

    # Specify the collection - replace with your actual collection name
    collection = db['cosmos-collection-siima-telemetry']

    # Fetch and order data by 'DeviceMessageTimestamp'
    data = list(collection.find().sort('DeviceMessageTimestamp', 1))

    # Convert the data to a pandas DataFrame for easier manipulation
    df = pd.DataFrame(data)

    # Convert 'DeviceMessageTimestamp' to datetime objects for filtering and indexing
    df['DeviceMessageTimestamp'] = pd.to_datetime(df['DeviceMessageTimestamp'])
    df.set_index('DeviceMessageTimestamp', inplace=True)
    
    return df

# Function to plot line chart with Altair, with dynamic Y-axis scaling
def plot_altair_chart(dataframe, title):
    # Calculating dynamic Y-axis domains
    battery_charge_domain = [dataframe['Battery1Charge'].min(), dataframe['Battery1Charge'].max()]
    energy_consumption_domain = [dataframe['Energy1Consumption'].min(), dataframe['Energy1Consumption'].max()]
    
    # Base chart
    base = alt.Chart(dataframe.reset_index()).encode(
        x=alt.X('DeviceMessageTimestamp:T', title='Time')
    ).properties(
        title=title,
        width=700,
        height=400
    )
    
    # Line for Battery1Charge
    line_battery_charge = base.mark_line(color='blue').encode(
        y=alt.Y('Battery1Charge:Q', axis=alt.Axis(title='Battery Charge'), scale=alt.Scale(domain=battery_charge_domain)),
        tooltip=['DeviceMessageTimestamp:T', 'Battery1Charge']
    )
    
    # Line for Energy1Consumption
    line_energy_consumption = base.mark_line(color='red').encode(
        y=alt.Y('Energy1Consumption:Q', axis=alt.Axis(title='Energy Consumption'), scale=alt.Scale(domain=energy_consumption_domain)),
        tooltip=['DeviceMessageTimestamp:T', 'Energy1Consumption']
    )
    
    # Combine the two lines with independent Y-axes
    chart = alt.layer(line_battery_charge, line_energy_consumption).resolve_scale(
        y='independent'
    ).interactive()
    
    st.altair_chart(chart, use_container_width=True)

# Auto-refresh handling
if 'last_refresh' not in st.session_state or (datetime.now() - st.session_state.last_refresh) > timedelta(minutes=1):
    st.session_state.last_refresh = datetime.now()
    st.experimental_rerun()

# Fetching and preparing data
df = fetch_data()

# Filter to numeric columns for resampling, preserving timestamp index
numeric_cols = df.select_dtypes(include=['number'])

# Filtering data for the plots with resampling for numeric data
now = datetime.now()
df_hour = numeric_cols.last('1H').resample('1T').mean()  # 1 minute granularity for the last hour, fixed for numeric data
df_day = numeric_cols[numeric_cols.index >= now - timedelta(days=1)]
df_month = numeric_cols[numeric_cols.index >= now - timedelta(days=30)]  # Approximation
df_year = numeric_cols[numeric_cols.index >= now - timedelta(days=365)]  # Approximation

# Merging non-numeric data if necessary, skipped here for brevity

# Plotting
st.title('Siima - Energy Account Overview')

plot_altair_chart(df_hour, 'Last Hour (1 Minute Granularity)')
plot_altair_chart(df_day, 'Last Day')
plot_altair_chart(df_month, 'Last Month')
plot_altair_chart(df_year, 'Last Year')