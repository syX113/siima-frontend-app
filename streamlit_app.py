import streamlit as st
import streamlit_authenticator as stauth
from pymongo import MongoClient
import pandas as pd
import altair as alt
import os
import yaml
from yaml.loader import SafeLoader

def authenticate():
    
    with open('config.yaml') as file:
        config = yaml.load(file, Loader=SafeLoader)

    authenticator = stauth.Authenticate(
        st.secrets["credentials"].to_dict(),
        st.secrets['cookie']['name'],
        st.secrets['cookie']['key'],
        st.secrets['cookie']['expiry_days']
    )
    user_collection_map = config.get('user_collection_map', {})
    return authenticator, config, user_collection_map


def calculate_energy_balance(df):

    # Initialize the 'Energy_Account_Balance_kW' column
    df['Energy_Account_Balance_kW'] = 0.0

    # Perform the rolling calculation
    for i in range(1, len(df)):
        previous_balance = df.iloc[i - 1]['Energy_Account_Balance_kW']
        previous_net = df.iloc[i - 1]['SmartMeter_Consumption_B1_kW'] - df.iloc[i - 1]['SmartMeter_Production_E1_kW']
        current_net = df['SmartMeter_Consumption_B1_kW'].iloc[i] - df['SmartMeter_Production_E1_kW'].iloc[i]
        df.at[i, 'Energy_Account_Balance_kW'] = previous_balance + (previous_net - current_net)
    
    return df

def fetch_data(collection_name):

    mongo_connection_string = os.getenv("MONGO_CONNECTION_STRING")

    client = MongoClient(mongo_connection_string)

    db = client['cosmos-db-siima-telemetry']

    collection = db[collection_name]
    projection = {  '_id': 0, # Exclude
                    'DeviceMessageTimestamp': 1, 
                    'SmartMeter_Consumption_B1_kW': 1, 
                    'SmartMeter_Production_E1_kW': 1,
                    'Current_Total_Input_W': 1, 
                    'Current_Total_Output_W': 1, 
                    'Current_Phase1_Input_W': 1, 
                    'Current_Phase2_Input_W': 1, 
                    'Current_Phase3_Input_W': 1, 
                    'Current_Phase1_Output_W': 1, 
                    'Current_Phase2_Output_W': 1, 
                    'Current_Phase3_Output_W': 1
                }
    
    data = list(collection.find(projection=projection).sort('DeviceMessageTimestamp', 1))

    df = pd.DataFrame(data)
    # Convert 'DeviceMessageTimestamp' to datetime
    df['DeviceMessageTimestamp'] = pd.to_datetime(df['DeviceMessageTimestamp'], unit='ms')

    # Localize timestamps to "Europe/Zurich" without converting from UTC
    df['DeviceMessageTimestamp'] = df['DeviceMessageTimestamp'].dt.tz_localize('Europe/Zurich', ambiguous='raise')

    df = df[['DeviceMessageTimestamp', 'SmartMeter_Consumption_B1_kW', 'SmartMeter_Production_E1_kW', 'Current_Total_Input_W', 'Current_Total_Output_W']]

    
    
    return df

### Main Logic ###

# Authenticate user
authenticator, config, user_collection_map = authenticate()
authenticator.login(fields={'Form name':'Login', 'Username':'Username', 'Password':'Password', 'Login':'Login'})


# User is authenticated
if st.session_state["authentication_status"]:

    # Directly dump hashed password in application
    with open('config.yaml', 'w') as file:
        yaml.dump(config, file, default_flow_style=False)

    username = st.session_state.get("username")

    st.write(f'Welcome *{st.session_state["name"]}*')
    st.title('Siima | Swiss Energy Account')

    collection_name = user_collection_map.get(username, "default_collection_name")

    df = fetch_data(collection_name)
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
        date_str = first_timestamp.strftime('%d. %B %Y')  # Basic formatting

        # Removing leading zero from day and adding ordinal suffix
        formatted_date = first_timestamp.strftime("%-d. %B %Y")
        time_str = first_timestamp.strftime('%H:%M')  # Rounds to the nearest minute
        # Use st.columns to display KPIs side by side
        col1, col2, col3 = st.columns([4, 4, 4])

        # Current energy balance
        with col1:
            st.metric(label="**Energy Account Balance Now:**", value=f"{last_balance_value:.2f} kW")

        # 6 hour delta and change
        with col2:
            if len(df) > 360:
                st.metric(label="**Energy Account Balance 12h ago:**", value=f"{previous_balance_value:.2f} kW", delta=f"{delta:.2f} kW to Now")

        with col3:
            # Attempting to nudge the title up with a negative margin
            st.markdown(f"""
            <div style="margin-top: -0px;">
                <span style="font-size: 0.85em; font-weight: bold;">Smart Meter Data Received from:</span><br>
            </div>
            {formatted_date}<br>
            {time_str}
            """, unsafe_allow_html=True)

        st.markdown('<hr style="border-top-color: #ffffff; border-top-width: 2px;"/>', unsafe_allow_html=True)

        # Chart for the Energy Account Balance
        balance_chart = alt.Chart(df).mark_line(color='#42c0b1').encode(
            x=alt.X('DeviceMessageTimestamp:T', title='Time'),
            y=alt.Y('Energy_Account_Balance_kW:Q', title='Energy Account Balance (kW)'),
            tooltip=['DeviceMessageTimestamp:T', 'Energy_Account_Balance_kW:Q']
        ).properties(
            width=800,
            height=400,
            title='Energy Account Balance (kW)'
        ).interactive()

        # Display the chart in Streamlit
        st.altair_chart(balance_chart, use_container_width=True)

        st.markdown('<hr style="border-top-color: #ffffff; border-top-width: 2px;"/>', unsafe_allow_html=True)

        # Define the base chart with common properties
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
        legend_entries = ['From Grid', 'Feed In']
        colors = ['#FF9B9B', '#8FD694']

        # Display the chart
        st.altair_chart(chart, use_container_width=True)
        legend_html = "".join([f"<span style='color:{color}; margin-right: 20px;'>● {legend}</span>" for legend, color in zip(legend_entries, colors)])
        st.markdown(f"<div style='text-align: left;'>{legend_html}</div>", unsafe_allow_html=True)

    st.markdown('<hr style="border-top-color: #ffffff; border-top-width: 5px;"/>', unsafe_allow_html=True)

    # Render logout button
    authenticator.logout()

# Login failed or no credentials entered
elif st.session_state["authentication_status"] is False:
    st.error('Username/password is incorrect')

elif st.session_state["authentication_status"] is None:
    st.warning('Please enter your username and password')