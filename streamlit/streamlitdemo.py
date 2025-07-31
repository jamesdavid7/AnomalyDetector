import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import os

st.set_page_config(layout="wide")
st.title("🧠 POS Anomaly Dashboard")

API_BASE = os.getenv("API_URL", "http://flask_api:5000")
METRICS_ENDPOINT = f"{API_BASE}/metrics"
DOWNLOAD_ENDPOINT = f"{API_BASE}/download"

# @st.cache_data(ttl=60)
def fetch_metrics():
    try:
        response = requests.get(METRICS_ENDPOINT)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        st.error(f"Error fetching metrics: {e}")
    return []

# Initialize session state
if "page" not in st.session_state:
    st.session_state.page = "list"
if "selected_metric" not in st.session_state:
    st.session_state.selected_metric = None
if "download_ready" not in st.session_state:
    st.session_state.download_ready = False
if "download_content" not in st.session_state:
    st.session_state.download_content = None

# Load metrics
metrics = fetch_metrics()
metrics = sorted(metrics, key=lambda x: x["created_at"], reverse=True)

# PAGE: METRIC LIST
if st.session_state.page == "list":
    st.subheader("📂 Anomaly Files")

    cols = st.columns([3, 2, 1])
    cols[0].markdown("**File Name**")
    cols[1].markdown("**Created At**")
    cols[2].markdown("**View Details**")

    for metric in metrics:
        file_name = metric["file_name"]
        created_at = metric["created_at"]
        metric_id = metric["metric_id"]

        col1, col2, col3 = st.columns([3, 2, 1])
        col1.write(file_name)
        col2.write(created_at)

        if col3.button("🔍", key=f"view_{metric_id}"):
            st.session_state.selected_metric = metric_id
            st.session_state.page = "details"
            st.session_state.download_ready = False
            st.session_state.download_content = None
            st.rerun()

# PAGE: DETAILS
elif st.session_state.page == "details":
    selected_id = st.session_state.selected_metric
    selected_metric = next((m for m in metrics if m["metric_id"] == selected_id), None)

    if selected_metric:
        top_col1, top_col2 = st.columns([1, 9])
        with top_col1:
            if st.button("🔙 Back to list", key="top_back"):
                st.session_state.page = "list"
                st.rerun()
        with top_col2:
            st.subheader(f"📊 Detailed View for `{selected_metric['file_name']}`")

        anomaly_data = selected_metric.get("metric_data", [])
        df = pd.DataFrame(anomaly_data)
        df = df[df["anomaly_type"].notnull()]

        if not df.empty:
            # Step 1: Click to trigger fetch
            if not st.session_state.download_ready:
                if st.button("📥 Fetch CSV for Download"):
                    try:
                        res = requests.get(f"{DOWNLOAD_ENDPOINT}/{selected_metric['file_name']}")
                        if res.status_code == 200:
                            st.session_state.download_content = res.content
                            st.session_state.download_ready = True
                            st.rerun()
                        else:
                            st.error("Failed to fetch CSV.")
                    except Exception as e:
                        st.error(f"Download error: {e}")

            # Step 2: Show download button if ready
            elif st.session_state.download_ready and st.session_state.download_content:
                st.download_button(
                    label="⬇️ Save CSV File",
                    data=st.session_state.download_content,
                    file_name=selected_metric['file_name'],
                    mime="text/csv"
                )

            # Charts
            left, right = st.columns(2)
            with left:
                st.markdown("**Anomaly Distribution (Bar)**")
                bar = px.bar(df, x="anomaly_type", y="count", title="Anomaly Counts by Type", text="count")
                st.plotly_chart(bar, use_container_width=True)
            with right:
                st.markdown("**Anomaly Distribution (Pie)**")
                pie = px.pie(df, names="anomaly_type", values="count", title="Anomaly Type Share")
                st.plotly_chart(pie, use_container_width=True)
        else:
            st.info("No valid anomaly data to display.")

        st.markdown("---")
        if st.button("🔙 Back to list", key="bottom_back"):
            st.session_state.page = "list"
            st.rerun()
