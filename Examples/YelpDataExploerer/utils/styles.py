# utils/styles.py
import streamlit as st

def load_styles():
    st.markdown("""
    <style>
        /* Main Headers */
        .main-header {
            font-size: 2.5rem;
            font-weight: bold;
            color: #FF5A5F;
            margin-bottom: 1rem;
        }
        .sub-header {
            font-size: 1.8rem;
            font-weight: bold;
            color: #484848;
            margin-top: 2rem;
            margin-bottom: 1rem;
        }

        /* Card Styling */
        .card {
            padding: 1.5rem;
            border-radius: 0.5rem;
            background-color: white;
            box-shadow: 0 4px 10px rgba(0, 0, 0, 0.1);
            text-align: center;
            width: 100%;
            height: 120px;
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
        }

        /* Metric Styling */
        .metric-label {
            font-size: 1rem;
            font-weight: bold;
            color: #666;
            margin-bottom: 0.3rem;
        }
        .metric-value {
            font-size: 1.8rem;
            font-weight: bold;
            color: #FF5A5F;
        }

        /* Fixing Alignment Issues */
        .stMarkdown {
            width: 100%;
        }
    </style>
    """, unsafe_allow_html=True)
