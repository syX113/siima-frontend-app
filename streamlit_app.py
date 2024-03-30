import streamlit as st
from pymongo import MongoClient
import os

# Load the connection string from an environment variable
mongo_connection_string = os.getenv("MONGO_CONNECTION_STRING")

# Connect to the MongoDB client
client = MongoClient(mongo_connection_string)
db = client['cosmos-db-siima-telemetry']
collection = db['cosmos-collection-siima-telemetry']

# Fetch data from the collection
data = list(collection.find().limit(10))

# Display the data in a table
st.title('Energy Account - Overview')
st.table(data)