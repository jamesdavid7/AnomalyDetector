import streamlit as st

# 🔄 Redirect immediately to your preferred page
DEFAULT_PAGE = "Dashboard"   # <-- replace with your filename inside /pages

st.set_page_config(layout="wide")

st.markdown(f"""
    <meta http-equiv="refresh" content="0; url=/{DEFAULT_PAGE}">
""", unsafe_allow_html=True)
