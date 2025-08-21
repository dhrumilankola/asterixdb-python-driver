# app.py
import streamlit as st

# Must be the first Streamlit command!
st.set_page_config(
    page_title="Yelp Data Explorer - PyAsterix Demo",
    page_icon="🍽️",
    layout="wide"
)

from utils.styles import load_styles
from utils.db import connect_to_asterixdb
from modules import dashboard, business_explorer, query_lab, crud_operations

# Load custom CSS styles
load_styles()

# Connect to AsterixDB
conn = connect_to_asterixdb()
if conn is None:
    st.error("Failed to connect to AsterixDB. Please check your connection settings.")
    st.stop()
    

# Sidebar static branding and info
st.sidebar.markdown("<div class='main-header'>Yelp Data Explorer</div>", unsafe_allow_html=True)
st.sidebar.markdown("### Powered by PyAsterix")
st.sidebar.markdown("---")

# Custom navigation radio (only these options will show)
page = st.sidebar.radio(
    "Navigation",
    ["Dashboard", "Business Explorer", "CRUD Operations", "Query Lab"],
    index=0  # Default to Dashboard
)

# Route to the selected page, passing the DB connection
if page == "Dashboard":
    dashboard.run(conn)
elif page == "Business Explorer":
    business_explorer.run(conn)
elif page == "Query Lab":
    query_lab.run(conn)
elif page == "CRUD Operations":
    crud_operations.run(conn)
    
st.sidebar.markdown("---")

st.sidebar.markdown("**Dataverse:** YelpDataverse")
st.sidebar.markdown("**Datasets:**")
st.sidebar.markdown("- Businesses")
st.sidebar.markdown("- Reviews")
st.sidebar.markdown("- Tips")
st.sidebar.markdown("- Users")
st.sidebar.markdown("---")
st.sidebar.success("✅ Connected to AsterixDB")
st.sidebar.markdown("<div class='footer'>PyAsterix Demo App © 2025</div>", unsafe_allow_html=True)

