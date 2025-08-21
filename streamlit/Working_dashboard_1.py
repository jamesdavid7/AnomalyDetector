import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import os
from datetime import datetime, UTC

st.set_page_config(layout="wide")
st.title("🧠 POS Anomaly Dashboard")

# ---------------- API Endpoints ----------------
API_BASE = os.getenv("API_URL", "http://flask_api:5000")
TRANSACTIONS_ENDPOINT = f"{API_BASE}/transactions"   # new endpoint to fetch recent txns
METRICS_ENDPOINT = f"{API_BASE}/metrics"             # anomaly metrics (historical)
DOWNLOAD_ENDPOINT = f"{API_BASE}/download"           # anomaly csv download


# ===================================================
# Helpers
# ===================================================
def fetch_transactions(limit: int = 10, start_key=None):
    """Fetch transactions with DynamoDB pagination."""
    try:
        url = f"{TRANSACTIONS_ENDPOINT}?limit={limit}"
        if start_key:
            url += f"&start_key={start_key}"
        res = requests.get(url)
        if res.status_code == 200:
            data = res.json()
            df = pd.DataFrame(data.get("transactions", []))
            return df, data.get("last_evaluated_key")
    except Exception as e:
        st.error(f"Error fetching transactions: {e}")
    return pd.DataFrame(), None


def fetch_metrics():
    """Fetch saved anomaly metrics (files)."""
    try:
        res = requests.get(METRICS_ENDPOINT)
        if res.status_code == 200:
            return res.json()
    except Exception as e:
        st.error(f"Error fetching metrics: {e}")
    return []


# ---------------- Sidebar Navigation ----------------
page = st.sidebar.radio("📑 Select View", ["📡 Real-Time Dashboard", "📂 Anomaly Files"])


# ===================================================
# PAGE 1: REAL-TIME DASHBOARD (with pagination)
# ===================================================
if page == "📡 Real-Time Dashboard":
    st.subheader("📡 Live Transactions & Metrics")

    # --- Pagination state ---
    if "last_key" not in st.session_state:
        st.session_state.last_key = None
    if "page_stack" not in st.session_state:
        st.session_state.page_stack = []
    if "page_num" not in st.session_state:
        st.session_state.page_num = 1

    # --- Auto refresh ---
    refresh_rate = st.sidebar.slider("Auto-refresh (seconds)", 5, 60, 15)
    st_autorefresh = st.sidebar.checkbox("🔄 Auto Refresh", value=True)

    # --- Fetch transactions with pagination ---
    df, next_key = fetch_transactions(limit=10, start_key=st.session_state.last_key)

    if df.empty:
        st.warning("No transactions available yet.")
    else:
        # KPIs
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Transactions", len(df))
        col2.metric("Anomalies", int(df["is_anomaly"].sum()) if "is_anomaly" in df else 0)
        col3.metric("Avg Amount", round(df["amount"].mean(), 2))

        st.markdown(f"### 📝 Transactions (Page {st.session_state.page_num})")
        st.dataframe(
            df[["transaction_id", "store_name", "amount", "currency",
                "transaction_status", "timestamp_initiated"]],
            use_container_width=True
        )

        # Pagination controls
        nav1, nav2, nav3 = st.columns([1, 6, 1])
        with nav1:
            if st.button("⬅️ Previous") and st.session_state.page_stack:
                st.session_state.last_key = st.session_state.page_stack.pop()
                st.session_state.page_num -= 1
                st.rerun()
        with nav3:
            if next_key:
                if st.button("Next ➡️"):
                    st.session_state.page_stack.append(st.session_state.last_key)
                    st.session_state.last_key = next_key
                    st.session_state.page_num += 1
                    st.rerun()

        # Plot: Transactions Over Time
        if "timestamp_initiated" in df.columns:
            df["timestamp_initiated"] = pd.to_datetime(
                df["timestamp_initiated"], unit="s", errors="coerce"
            )
            fig_time = px.line(
                df.sort_values("timestamp_initiated"),
                x="timestamp_initiated", y="amount",
                title="Transaction Amounts Over Time"
            )
            st.plotly_chart(fig_time, use_container_width=True)

        # Plot: Anomaly Distribution
        if "anomaly_type" in df.columns:
            anomaly_df = df.groupby("anomaly_type").size().reset_index(name="count")
            if not anomaly_df.empty:
                fig_anom = px.bar(anomaly_df, x="anomaly_type", y="count",
                                  title="Anomaly Distribution")
                st.plotly_chart(fig_anom, use_container_width=True)

    if st_autorefresh:
        st.query_params["refresh"] = str(datetime.now(UTC).timestamp())




# ===================================================
# PAGE 2: ANOMALY FILES (Historical)
# ===================================================
elif page == "📂 Anomaly Files":
    st.subheader("📂 Anomaly Files")

    metrics = fetch_metrics()
    metrics = sorted(metrics, key=lambda x: x["created_at"], reverse=True)

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
            with st.expander(f"📊 Detailed View for `{file_name}`", expanded=True):
                anomaly_data = metric.get("metric_data", [])
                df = pd.DataFrame(anomaly_data)
                df = df[df["anomaly_type"].notnull()]

                if not df.empty:
                    left, right = st.columns(2)
                    with left:
                        st.markdown("**Anomaly Distribution (Bar)**")
                        bar = px.bar(df, x="anomaly_type", y="count",
                                     title="Anomaly Counts by Type", text="count")
                        st.plotly_chart(bar, use_container_width=True)
                    with right:
                        st.markdown("**Anomaly Distribution (Pie)**")
                        pie = px.pie(df, names="anomaly_type", values="count",
                                     title="Anomaly Type Share")
                        st.plotly_chart(pie, use_container_width=True)

                    # Download button
                    try:
                        res = requests.get(f"{DOWNLOAD_ENDPOINT}/{file_name}")
                        if res.status_code == 200:
                            st.download_button(
                                label="⬇️ Save CSV File",
                                data=res.content,
                                file_name=file_name,
                                mime="text/csv"
                            )
                    except Exception as e:
                        st.error(f"Download error: {e}")
                else:
                    st.info("No valid anomaly data to display.")
