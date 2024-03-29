import streamlit as st
from pymongo import MongoClient
import os

# Load the connection string from an environment variable
mongo_connection_string = os.getenv("MONGO_CONNECTION_STRING")

# Connect to the MongoDB client
client = MongoClient(mongo_connection_string)

# Connect to your database - replace 'your_database_name' with your actual database name
db = client['cosmos-db-siima-telemetry']

# Specify the collection - replace 'your_collection_name' with your actual collection name
collection = db['cosmos-collection-siima-telemetry']

# Fetch data from the collection
# This example fetches all documents from the specified collection, you can modify the query as needed
data = list(collection.find())

# Display the data in a table
st.title('MongoDB Data Display')
st.table(data)