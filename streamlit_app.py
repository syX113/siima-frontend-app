import streamlit as st
import streamlit_authenticator as stauth
import pandas as pd
import altair as alt
import os
from datetime import datetime, timedelta
from pymongo import MongoClient

def authenticate():
    authenticator = stauth.Authenticate(
        st.secrets["credentials"].to_dict(),
        st.secrets['cookie']['name'],
        st.secrets['cookie']['key'],
        st.secrets['cookie']['expiry_days']
    )
    return authenticator, st.secrets["user_collection_map"]["mapping"]

def calculate_energy_balance(df):
    # Calculate net consumption/production for each row
    df['Net_Consumption_Production_kW'] = df['SmartMeter_Consumption_B1_kW'] - df['SmartMeter_Production_E1_kW']
    
    # Calculate the difference in net consumption/production between consecutive rows
    df['Delta_Net_kW'] = df['Net_Consumption_Production_kW'].diff().fillna(0)
    
    # Reverse the sign of the delta to match the original calculation logic
    df['Delta_Net_kW'] *= -1
    
    # Cumulatively sum these deltas to get the energy account balance
    df['Energy_Account_Balance_kW'] = df['Delta_Net_kW'].cumsum()
    
    return df

def fetch_data(client, collection_name):
    db = client['cosmos-db-siima-telemetry']
    collection = db[collection_name]
    projection = {
        '_id': 0, 'DeviceMessageTimestamp': 1, 
        'SmartMeter_Consumption_B1_kW': 1, 'SmartMeter_Production_E1_kW': 1,
        'Current_Total_Input_W': 1, 'Current_Total_Output_W': 1
    }
    df = pd.DataFrame(list(collection.find(projection=projection).sort('DeviceMessageTimestamp', 1)))
    
    # Convert 'DeviceMessageTimestamp' to datetime
    df['DeviceMessageTimestamp'] = pd.to_datetime(df['DeviceMessageTimestamp'], unit='ms')
    
    # Localize timestamps to "Europe/Zurich" without converting from UTC
    df['DeviceMessageTimestamp'] = df['DeviceMessageTimestamp'].dt.tz_localize('Europe/Zurich', ambiguous='raise')
    df = df[['DeviceMessageTimestamp', 'SmartMeter_Consumption_B1_kW', 'SmartMeter_Production_E1_kW', 'Current_Total_Input_W', 'Current_Total_Output_W']]
    
    # Convert 'Current_Total_Input_W' and 'Current_Total_Output_W' from W to kW
    df['Current_Total_Input_W'] = df['Current_Total_Input_W'] / 1000
    df['Current_Total_Output_W'] = df['Current_Total_Output_W'] / 1000

    # Rename columns to reflect that they are now in kW
    df.rename(columns={'Current_Total_Input_W': 'Current_Total_Input_kW', 'Current_Total_Output_W': 'Current_Total_Output_kW'}, inplace=True)
    # Return only selected columns
    df = df[['DeviceMessageTimestamp', 'SmartMeter_Consumption_B1_kW', 'SmartMeter_Production_E1_kW', 'Current_Total_Input_kW', 'Current_Total_Output_kW']]

    return df

def cut_df_to_timeframe(df, timeframe):
    now = df['DeviceMessageTimestamp'].max()
    if timeframe == '1h':
        cutoff_time = now - timedelta(minutes=60)
    if timeframe == '12h':
        cutoff_time = now - timedelta(hours=12)
    elif timeframe == '1 day':
        cutoff_time = now - timedelta(days=1)
    elif timeframe == '1 week':
        cutoff_time = now - timedelta(weeks=1)
    elif timeframe == '1 month':
        cutoff_time = now - pd.DateOffset(months=1)
    elif timeframe == '1 year':
        cutoff_time = now - pd.DateOffset(years=11)
    else:
        return df
    return df[df['DeviceMessageTimestamp'] > cutoff_time]

# Assuming 1 data point per minute
timeframe_to_datapoints = {
    '12h': 720,  # 12 hours * 60 minutes/hour
    '1 day': 1440,  # 24 hours * 60 minutes/hour
    '1 week': 10080,  # 7 days * 24 hours/day * 60 minutes/hour
    '1 month': 43200,  # Approx. 30 days * 24 hours/day * 60 minutes/hour
    '1 year': 518400  # Approx. 365 days * 24 hours/day * 60 minutes/hour   
}

# Authenticate user
st.set_page_config(page_title='Siima | Swiss Energy Account', page_icon=':zap:', layout="centered", initial_sidebar_state="auto", menu_items=None)
authenticator, user_collection_map = authenticate()
_, authentication_status, username = authenticator.login(fields={'Form name':'Siima Login', 'Username':'Username', 'Password':'Password', 'Login':'Login'})

# After user is logged in
if authentication_status:
    
    st.write(f'Logged in as: *{st.session_state["name"]}*')
    st.title('Siima | Swiss Energy Account :zap:')
    collection_name = user_collection_map.get(username)

    client = MongoClient(os.getenv("MONGO_CONNECTION_STRING"))
    df = fetch_data(client, collection_name)
    df = calculate_energy_balance(df)
    
    # Add dropdown for timeframe selection
    timeframe_options = ['12h', '1 day', '1 week', '1 month', '1 year']
    selected_timeframe = st.selectbox("Select desired timeframe to visualize past Energy Account Balance & Movements:", options=timeframe_options, index=1)  # Defaults to '1day'
    df_filtered = cut_df_to_timeframe(df, selected_timeframe) if not df.empty else df

    # Get the first 'DeviceMessageTimestamp'
    first_timestamp = df['DeviceMessageTimestamp'].iloc[0]
    # Format the first 'DeviceMessageTimestamp' for display
    first_timestamp_formatted = first_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    # Assuming 'first_timestamp' is your datetime object
    date_str = first_timestamp.strftime('%d. %B %Y')
    # Removing leading zero from day and adding ordinal suffix
    formatted_date = first_timestamp.strftime("%-d. %B %Y")
    time_str = first_timestamp.strftime('%H:%M') # Round to minute

    if not df_filtered.empty:
        
        last_balance_value = df_filtered['Energy_Account_Balance_kW'].iloc[-1]
    
        # Setup KPI row
        col1, col2, col3 = st.columns([4, 4, 4])
        
        with col1:
            st.metric(label="**Energy Account Balance:**", value=f"{last_balance_value:.2f} kW")

        # Determine the index for the previous balance value based on the selected timeframe
        not_enough_data = False  # Flag to indicate if there are not enough data points
        datapoints_back = timeframe_to_datapoints[selected_timeframe]
        # Check if the DataFrame has enough data points
        if len(df_filtered) >= datapoints_back:
            previous_balance_value = df_filtered['Energy_Account_Balance_kW'].iloc[-datapoints_back]
            delta = last_balance_value - previous_balance_value
        else:
            # Not enough data points
            not_enough_data = True
                
        with col2:
            if not not_enough_data:
                # Display the metric with delta if there are enough data points
                st.metric(label=f"**Energy Account Balance {selected_timeframe} Ago:**", value=f"{previous_balance_value:.2f} kW", delta=f"{delta:.2f} kW to Now")
            else:
                # Display a message indicating not enough data points
                st.metric(label=f"**Energy Account Balance {selected_timeframe} Ago:**", value="N/A")
                
        # Show timestamp if first datapoint
        with col3:
            st.markdown(f"""
            <div style="margin-top: -0px;">
                <span style="font-size: 0.85em; font-weight: bold;">Smart Meter Data Received from:</span><br>
            </div>
            {formatted_date}<br>
            {time_str}
            """, unsafe_allow_html=True)

        # Simple horizontal devider line
        st.markdown('<hr style="border-top-color: #ffffff; border-top-width: 1px;"/>', unsafe_allow_html=True)

        # Not needed!
        # Determine the appropriate timeUnit for the selected timeframe
        if selected_timeframe in ['12h', '1 day']:
            time_unit = 'hours'
        elif selected_timeframe in ['1 week', '1 month']:
            time_unit = 'date'
        elif selected_timeframe in ['1 year', 'total']:
            time_unit = 'month'
    
        # Chart for the Energy Account Balance
        balance_chart = alt.Chart(df_filtered).mark_line(color='#42c0b1').encode(
            x=alt.X('DeviceMessageTimestamp:T', title='Time'    ),
            y=alt.Y('Energy_Account_Balance_kW:Q', title='Energy Account Balance (kW)'),
            tooltip=[alt.Tooltip('DeviceMessageTimestamp:T', title='Time'), alt.Tooltip('Energy_Account_Balance_kW:Q', title='Account Balance (kW)', format='.2f')]
        ).properties(
            width=800,
            height=400,
            title='Energy Account Balance (kW)'
        ).interactive()

        st.altair_chart(balance_chart, use_container_width=True)

        st.markdown('<hr style="border-top-color: #ffffff; border-top-width: 1px;"/>', unsafe_allow_html=True)

        # Chart for "Feed In" and "From Grid"
        base = alt.Chart(df_filtered).properties(
            width=800,
            height=400,
            title='Energy Movement (kW)'
        )

        # Create the chart for 'Current_Total_Input_W' without custom Y-axis color
        input_line = base.mark_line().encode(
            x=alt.X('DeviceMessageTimestamp:T', title='Time'),
            y=alt.Y('Current_Total_Input_kW:Q', title='From Grid & Feed In (kW)'),
            color=alt.value('#FF9B9B'),  # Direct color value
            tooltip=[alt.Tooltip('DeviceMessageTimestamp:T', title='Time'), alt.Tooltip('Current_Total_Input_W:Q', title='From Grid (kW)', format='.2f')]
        )

        # Create the chart for 'Current_Total_Output_W' without custom Y-axis color
        output_line = base.mark_line().encode(
            x=alt.X('DeviceMessageTimestamp:T', title='Time'),
            y=alt.Y('Current_Total_Output_kW:Q', title='From Grid & Feed In (kW)'),
            color=alt.value('#8FD694'),  # Direct color value
            tooltip=[alt.Tooltip('DeviceMessageTimestamp:T', title='Time'), alt.Tooltip('Current_Total_Output_W:Q', title='Feed In (kW)', format='.2f')]
        )

        # Combine the charts
        chart = alt.layer(input_line, output_line).resolve_scale(y='shared').properties().interactive()

        # Add a custom legend
        legend_entries = ['From Grid (Consumption)', 'Feed In (Production)']
        colors = ['#FF9B9B', '#8FD694']

        # Display the chart
        st.altair_chart(chart, use_container_width=True)
        legend_html = "".join([f"<span style='color:{color}; margin-right: 15px;'>● {legend}</span>" for legend, color in zip(legend_entries, colors)])
        st.markdown(f"<div style='text-align: right;'>{legend_html}</div>", unsafe_allow_html=True)

    st.markdown('<hr style="border-top-color: #ffffff; border-top-width: 1px;"/>', unsafe_allow_html=True)

    # Render logout button
    authenticator.logout()

# Login failed or no credentials entered if user is not authenticated
elif st.session_state["authentication_status"] is False:
    st.error('Username/password is incorrect')

elif st.session_state["authentication_status"] is None:
    st.warning('Please enter your username and password')