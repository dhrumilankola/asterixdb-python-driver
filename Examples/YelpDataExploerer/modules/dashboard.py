# pages/dashboard.py
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from utils.db import execute_query_sync
from utils.analysis import analyze_sentiment

def run(conn):
    st.markdown("<div class='main-header'>Yelp Data Dashboard</div>", unsafe_allow_html=True)
    st.markdown("This dashboard provides an overview of the Yelp dataset using PyAsterix.")

    @st.cache_data
    def get_dataset_stats():
        stats = {
            "businesses_count": execute_query_sync(conn, "SELECT VALUE COUNT(*) FROM YelpDataverse.Businesses;").iloc[0, 0],
            "reviews_count": execute_query_sync(conn, "SELECT VALUE COUNT(*) FROM YelpDataverse.Reviews;").iloc[0, 0],
            "users_count": execute_query_sync(conn, "SELECT VALUE COUNT(*) FROM YelpDataverse.Users;").iloc[0, 0],
            "tips_count": execute_query_sync(conn, "SELECT VALUE COUNT(*) FROM YelpDataverse.Tips;").iloc[0, 0],
            "avg_stars": execute_query_sync(conn, "SELECT VALUE AVG(b.stars) FROM YelpDataverse.Businesses b;").iloc[0, 0],
            # In pages/dashboard.py (within get_dataset_stats)
            "top_categories": execute_query_sync(conn, """
                SELECT trim(s) AS category, COUNT(*) AS count
                FROM YelpDataverse.Businesses b
                UNNEST split(b.categories, ',') AS s
                GROUP BY trim(s)
                ORDER BY COUNT(*) DESC
                LIMIT 10;
            """)
        }
        return stats

    with st.spinner("Loading dashboard data..."):
        try:
            stats = get_dataset_stats()

            # Debug output (uncomment if needed)
            # st.write("DEBUG: Top Categories Data:", stats['top_categories'])

            # Key metrics
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.markdown(f"""
                    <div class='card'>
                        <div class='metric-label'>Businesses</div>
                        <div class='metric-value'>{stats['businesses_count']:,}</div>
                    </div>
                """, unsafe_allow_html=True)

            with col2:
                st.markdown(f"""
                    <div class='card'>
                        <div class='metric-label'>Reviews</div>
                        <div class='metric-value'>{stats['reviews_count']:,}</div>
                    </div>
                """, unsafe_allow_html=True)

            with col3:
                st.markdown(f"""
                    <div class='card'>
                        <div class='metric-label'>Users</div>
                        <div class='metric-value'>{stats['users_count']:,}</div>
                    </div>
                """, unsafe_allow_html=True)

            with col4:
                st.markdown(f"""
                    <div class='card'>
                        <div class='metric-label'>Average Stars</div>
                        <div class='metric-value'>{stats['avg_stars']:.2f} ⭐</div>
                    </div>
                """, unsafe_allow_html=True)

            # Top Business Categories
            st.markdown("<div class='sub-header'>Top Business Categories</div>", unsafe_allow_html=True)
            if not stats['top_categories'].empty:
                fig = px.bar(
                    stats['top_categories'],
                    x='category',
                    y='count',
                    color='count',
                    color_continuous_scale='Reds',
                    title="Distribution of Top Business Categories"
                )
                fig.update_layout(xaxis_title="Category", yaxis_title="Number of Businesses")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No category data available. Please verify that your dataset contains non-empty 'categories' fields.")

            # (Additional dashboard components such as rating distribution and recent reviews can be added here.)
        except Exception as e:
            st.error(f"Error loading dashboard data: {str(e)}")
