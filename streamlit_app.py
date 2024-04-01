import streamlit as st
import streamlit_authenticator as stauth
import pandas as pd
import altair as alt
import os
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
    data = list(collection.find(projection=projection).sort('DeviceMessageTimestamp', 1))

    df = pd.DataFrame(data)
    # Convert 'DeviceMessageTimestamp' to datetime
    df['DeviceMessageTimestamp'] = pd.to_datetime(df['DeviceMessageTimestamp'], unit='ms')
    # Localize timestamps to "Europe/Zurich" without converting from UTC
    df['DeviceMessageTimestamp'] = df['DeviceMessageTimestamp'].dt.tz_localize('Europe/Zurich', ambiguous='raise')
    df = df[['DeviceMessageTimestamp', 'SmartMeter_Consumption_B1_kW', 'SmartMeter_Production_E1_kW', 'Current_Total_Input_W', 'Current_Total_Output_W']]

    return df

# Authenticate user
st.set_page_config(page_title='Siima | Swiss Energy Account', page_icon=':zap:', layout="centered", initial_sidebar_state="auto", menu_items=None)
authenticator, user_collection_map = authenticate()
_, authentication_status, username = authenticator.login(fields={'Form name':'Siima Login', 'Username':'Username', 'Password':'Password', 'Login':'Login'})

# Check if user is authenticated
if authentication_status:

    st.write(f'Welcome *{st.session_state["name"]}*')
    st.title('Siima | Swiss Energy Account')
    collection_name = user_collection_map.get(username, "default_collection_name")

    client = MongoClient(os.getenv("MONGO_CONNECTION_STRING"))
    df = fetch_data(client, collection_name)

    df = calculate_energy_balance(df)

    if not df.empty:

        # Get the first 'DeviceMessageTimestamp'
        first_timestamp = df['DeviceMessageTimestamp'].iloc[0]

        # Format the first 'DeviceMessageTimestamp' for display
        first_timestamp_formatted = first_timestamp.strftime('%Y-%m-%d %H:%M:%S')

        # Get the last value of 'Energy_Account_Balance_kW'
        last_balance_value = df['Energy_Account_Balance_kW'].iloc[-1]

        # Optionally, show change from the previous point
        if len(df) > 720:
            previous_balance_value = df['Energy_Account_Balance_kW'].iloc[-720]
            delta = last_balance_value - previous_balance_value
        else:
            previous_balance_value = 0.0
            delta = last_balance_value - previous_balance_value

        # Assuming 'first_timestamp' is your datetime object
        date_str = first_timestamp.strftime('%d. %B %Y')

        # Removing leading zero from day and adding ordinal suffix
        formatted_date = first_timestamp.strftime("%-d. %B %Y")
        time_str = first_timestamp.strftime('%H:%M') # Round to minute

        # Setup KPI row
        col1, col2, col3 = st.columns([4, 4, 4])

        # Current energy balance
        with col1:
            st.metric(label="**Energy Account Balance Now:**", value=f"{last_balance_value:.2f} kW")

        # 6 hour delta and change
        with col2:
            if len(df) > 360:
                st.metric(label="**Energy Account Balance 12h Ago:**", value=f"{previous_balance_value:.2f} kW", delta=f"{delta:.2f} kW to Now")

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
        st.markdown('<hr style="border-top-color: #ffffff; border-top-width: 10px;"/>', unsafe_allow_html=True)

        # Chart for the Energy Account Balance
        balance_chart = alt.Chart(df).mark_line(color='#42c0b1').encode(
            x=alt.X('DeviceMessageTimestamp:T', title='Time'),
            y=alt.Y('Energy_Account_Balance_kW:Q', title='Energy Account Balance (kW)'),
            tooltip=[alt.Tooltip('DeviceMessageTimestamp:T', title='Time'), alt.Tooltip('Energy_Account_Balance_kW:Q', title='Account Balance (kW)', format='.2f')]
        ).properties(
            width=800,
            height=400,
            title='Energy Account Balance (kW)'
        ).interactive()

        st.altair_chart(balance_chart, use_container_width=True)

        # Simple horizontal devider line
        st.markdown('<hr style="border-top-color: #ffffff; border-top-width: 10px;"/>', unsafe_allow_html=True)

        # Chart for "Feed In" and "From Grid"
        base = alt.Chart(df).properties(
            width=800,
            height=400,
            title='Energy Movement (W)'
        )

        # Create the chart for 'Current_Total_Input_W' without custom Y-axis color
        input_line = base.mark_line().encode(
            x=alt.X('DeviceMessageTimestamp:T', title='Time'),
            y=alt.Y('Current_Total_Input_W:Q', title='From Grid & Feed In (W)'),
            color=alt.value('#FF9B9B'),  # Direct color value
            tooltip=[alt.Tooltip('DeviceMessageTimestamp:T', title='Time'), alt.Tooltip('Current_Total_Input_W:Q', title='From Grid (W)', format='.2f')]
        )

        # Create the chart for 'Current_Total_Output_W' without custom Y-axis color
        output_line = base.mark_line().encode(
            x=alt.X('DeviceMessageTimestamp:T', title='Time'),
            y=alt.Y('Current_Total_Output_W:Q', title='From Grid & Feed In (W)'),
            color=alt.value('#8FD694'),  # Direct color value
            tooltip=[alt.Tooltip('DeviceMessageTimestamp:T', title='Time'), alt.Tooltip('Current_Total_Output_W:Q', title='Feed In (W)', format='.2f')]
        )

        # Combine the charts
        chart = alt.layer(input_line, output_line).resolve_scale(y='shared').properties().interactive()

        # Add a custom legend
        legend_entries = ['From Grid (Consumption)', 'Feed In (Production)']
        colors = ['#FF9B9B', '#8FD694']

        # Display the chart
        st.altair_chart(chart, use_container_width=True)
        legend_html = "".join([f"<span style='color:{color}; margin-right: 20px;'>● {legend}</span>" for legend, color in zip(legend_entries, colors)])
        st.markdown(f"<div style='text-align: left;'>{legend_html}</div>", unsafe_allow_html=True)

    st.markdown('<hr style="border-top-color: #ffffff; border-top-width: 10px;"/>', unsafe_allow_html=True)

    # Render logout button
    authenticator.logout()

# Login failed or no credentials entered if user is not authenticated
elif st.session_state["authentication_status"] is False:
    st.error('Username/password is incorrect')

elif st.session_state["authentication_status"] is None:
    st.warning('Please enter your username and password')