# modules/business_explorer.py
import streamlit as st
import pandas as pd
import folium
from folium.plugins import MarkerCluster
from streamlit_folium import folium_static
from utils.db import execute_query_sync

def run(conn):
    st.markdown("<h2>Business Explorer</h2>", unsafe_allow_html=True)
    st.markdown("Search and explore businesses in the Yelp dataset.")

    # Search and filter options
    col1, col2 = st.columns([3, 1])
    with col1:
        search_term = st.text_input("Search businesses by name", "")
    with col2:
        min_rating = st.selectbox("Minimum Rating", [1, 2, 3, 4, 5], index=0)

    # Get distinct categories
    categories = execute_query_sync(conn, """
        SELECT DISTINCT c AS category
        FROM YelpDataverse.Businesses b
        UNNEST b.categories c
        ORDER BY c;
    """)
    options = categories['category'].tolist() if not categories.empty and 'category' in categories.columns else []
    selected_categories = st.multiselect("Filter by categories", options=options)

    # Build query (explicitly selecting fields)
    query = """
    SELECT b.business_id, b.name, b.address, b.city, b.state, 
           b.stars, b.review_count, b.categories, b.latitude, b.longitude 
    FROM YelpDataverse.Businesses b
    WHERE 1=1
    """
    if search_term:
        query += f" AND CONTAINS(b.name, '{search_term}')"
    if min_rating > 1:
        query += f" AND b.stars >= {min_rating}"
    if selected_categories:
        cats = "', '".join(selected_categories)
        query += f" AND (SOME c IN b.categories SATISFIES c IN ['{cats}'])"
    query += " LIMIT 50;"

    with st.spinner("Searching businesses..."):
        try:
            businesses = execute_query_sync(conn, query)
            if not businesses.empty:
                st.success(f"Found {len(businesses)} businesses.")

                # Display business map
                st.markdown("### Business Locations")
                if ('latitude' in businesses.columns and 'longitude' in businesses.columns and
                    not businesses['latitude'].isnull().all() and not businesses['longitude'].isnull().all()):
                    m = folium.Map(location=[businesses['latitude'].mean(), businesses['longitude'].mean()], zoom_start=12)
                    marker_cluster = MarkerCluster().add_to(m)
                    for _, row in businesses.iterrows():
                        if pd.notnull(row['latitude']) and pd.notnull(row['longitude']):
                            popup_text = f"<strong>{row['name']}</strong><br>Rating: {row['stars']}⭐<br>Reviews: {row['review_count']}"
                            folium.Marker(
                                location=[row['latitude'], row['longitude']],
                                popup=popup_text,
                                icon=folium.Icon(color='red' if row['stars'] >= 4 else 'blue')
                            ).add_to(marker_cluster)
                    folium_static(m)
                else:
                    st.info("Location data not available for mapping.")

                # Display business list
                st.markdown("### Business List")
                for _, row in businesses.iterrows():
                    st.markdown(f"""
                    **{row['name']}**  
                    Rating: {'⭐' * int(row['stars'])}  
                    Categories: {', '.join(row['categories']) if isinstance(row['categories'], list) else row['categories']}  
                    Reviews: {row['review_count']}  
                    ID: {row['business_id']}
                    """)
            else:
                st.info("No businesses found matching your criteria.")
        except Exception as e:
            st.error(f"Error searching businesses: {e}")
