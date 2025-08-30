import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import os
import uuid

from datetime import datetime, timedelta, timezone

st.set_page_config(layout="wide")
st.title("🧠 POS Anomaly Dashboard")

# ---------------- API Endpoints ----------------
API_BASE = os.getenv("API_URL", "http://flask_api:5000")
TRANSACTIONS_ENDPOINT = f"{API_BASE}/anomaly_transaction/list"   # new endpoint to fetch recent txns
METRICS_ENDPOINT = f"{API_BASE}/metrics"             # anomaly metrics (historical)
DOWNLOAD_ENDPOINT = f"{API_BASE}/download"           # anomaly csv download


# ===================================================
# Helpers
# ===================================================
def fetch_transactions(limit: int = 10, start_key=None, sort_order: str = "desc"):
    """Fetch transactions from Flask API with DynamoDB pagination."""
    try:
        url = f"{TRANSACTIONS_ENDPOINT}?limit={limit}&sort_order={sort_order}"
        if start_key:
            # ✅ Ensure next_token is URL-safe
            url += f"&last_evaluated_key={quote(start_key)}"

        res = requests.get(url)
        if res.status_code == 200:
            data = res.json()

            # ✅ Build DataFrame from items
            df = pd.DataFrame(data.get("items", []))

            # ✅ Normalize timestamps if present
            if not df.empty and "timestamp_initiated" in df.columns:
                # Ensure numeric first to avoid FutureWarning
                df["timestamp_initiated"] = pd.to_datetime(
                    pd.to_numeric(df["timestamp_initiated"], errors="coerce"),
                    unit="ms",
                    utc=True
                )

                df["timestamp_initiated_utc"] = df["timestamp_initiated"].dt.strftime(
                    "%Y-%m-%d %H:%M:%S UTC"
                )
            # ✅ Return DataFrame and next_token string
            return df, data.get("next_token")

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

# ---------------- Store Metadata ----------------
@st.cache_data
def load_store_metadata():
    path = os.path.join("resource", "metadata", "store.csv")
    if os.path.exists(path):
        return pd.read_csv(path)
    else:
        st.error(f"Store metadata file not found at {path}")
        return pd.DataFrame(columns=["store_id", "store_name", "store_lat", "store_lon"])

store_meta = load_store_metadata()

# ---------------- Sidebar Navigation ----------------
page = st.sidebar.radio("📑 Select View", ["📡 Real-Time Dashboard", "📂 Anomaly Files"])


# ===================================================
# PAGE 1: REAL-TIME DASHBOARD (with pagination + alerts + counters)
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
        col3.metric("Avg Amount", round(df["transaction_amount"].mean(), 2))  # ✅ fixed key

        # ================= LIVE ALERTS =================
        st.markdown("### 🚨 Live Alerts")
        alerts = df[df.get("is_anomaly", 0) == 1].sort_values(
            "timestamp_initiated", ascending=False
        )
        if alerts.empty:
            st.info("No live alerts detected.")
        else:
            for _, row in alerts.head(5).iterrows():
                ts = row["timestamp_initiated"]
                if pd.notna(ts):  # ✅ Only format valid timestamps
                    ts = ts.strftime("%I:%M %p")
                else:
                    ts = "Unknown Time"
                st.write(
                    f"**{ts} — ALERT:** {row.get('anomaly_type', 'Unknown')} "
                    f"at {row.get('store_name', 'N/A')}"
                )

        # ---------------- Real-Time Counters ----------------
        st.markdown("### 📊 Real-Time Counters")

        # Compute KPIs
        # Get current UTC time
        current_time = datetime.now(timezone.utc)

        # Compute KPIs
        high_value = df[
            (df.get("transaction_amount", 0) > 2000) &
            (df["timestamp_initiated"] > current_time - timedelta(minutes=15))
            ] if "transaction_amount" in df.columns else pd.DataFrame()

        refunds_last_10 = df[
            (df.get("transaction_status") == "REFUND") &
            (df["timestamp_initiated"] > current_time - timedelta(minutes=10))
            ] if "transaction_status" in df.columns else pd.DataFrame()

        outside_hours = (
            df[df["timestamp_initiated"].dt.hour.between(0, 6)]
            if "timestamp_initiated" in df.columns
            else pd.DataFrame()
        )

        manual_ratio = 0
        if "entry_mode" in df.columns:
            recent = df[df["timestamp_initiated"] > current_time - timedelta(hours=1)]
            if not recent.empty:
                manual_ratio = (
                    recent["entry_mode"].eq("MANUAL").mean()
                )
                manual_ratio = round(manual_ratio * 100, 1) if not pd.isna(manual_ratio) else 0

        # Display as metric cards
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("High-value txns (15m)", len(high_value), "⚠ Above avg" if len(high_value) > 5 else "")
        col2.metric("Refunds (10m)", len(refunds_last_10))
        col3.metric("Outside store hours", len(outside_hours))
        col4.metric("Manual vs Chip/Tap (1h)", f"{manual_ratio}% manual", "⚠ High" if manual_ratio > 5 else "")

        # ================= TRANSACTIONS TABLE =================
        st.markdown("<a name='transactions'></a>", unsafe_allow_html=True)  # anchor at top of table
        st.markdown(f"### 📝 Transactions (Page {st.session_state.page_num})")

        # Columns to display (always exist)
        display_cols = [
            "transaction_id",
            "store_name",
            "transaction_amount",
            "currency",
            "transaction_status",
            "timestamp_initiated_utc",
            "is_anomaly"
        ]

        # Add anomaly_type if present in df
        if "anomaly_type" in df.columns:
            display_cols.append("anomaly_type")

        # Map column names to shorter display names
        col_display_names = {
            "transaction_id": "ID",
            "store_name": "Store",
            "transaction_amount": "Amount",
            "currency": "Cur",
            "transaction_status": "Status",
            "timestamp_initiated_utc": "Initiated (UTC)",
            "is_anomaly": "Anomaly?",
            "anomaly_type": "Type"
        }

        # Subset dataframe
        display_df = df[display_cols]

        # ----- Header row -----
        header_cols = st.columns(len(display_cols) + 1)
        for i, col in enumerate(display_cols):
            header_cols[i].markdown(f"**{col_display_names.get(col, col)}**")
        header_cols[-1].markdown("**Action**")

        # ----- Data rows with inline button -----
        for i, row in display_df.iterrows():
            cols = st.columns(len(display_cols) + 1)
            for j, col_name in enumerate(display_cols):
                value = row[col_name]
                # anomaly highlighting
                if col_name == "is_anomaly" and row["is_anomaly"]:
                    cols[j].markdown(f"<div style='background-color:#ffcccc'>{value}</div>", unsafe_allow_html=True)
                else:
                    cols[j].write(value)
            # Simple icon button for details
            if cols[-1].button("🔍", key=f"view_{row['transaction_id']}"):
                st.session_state.selected_txn = df.iloc[i].to_dict()
                st.session_state.scroll_to_details = True
                st.rerun()

        # ================= Pagination controls =================
        # (stick directly under the table regardless of details)
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

        # ================= Popup (2-column key/value layout) =================
        if "selected_txn" in st.session_state and st.session_state.selected_txn:
            st.markdown("<a name='details'></a>", unsafe_allow_html=True)  # anchor only if visible
            st.markdown("## 🔍 Transaction Details")
            selected = st.session_state.selected_txn

            # Render as key-value pairs
            for k, v in selected.items():
                c1, c2 = st.columns([1, 3])
                c1.markdown(f"**{col_display_names.get(k, k)}**")
                c2.write(v)

            if st.button("❌ Close"):
                st.session_state.selected_txn = None
                st.session_state.scroll_to_table = True
                st.rerun()

            # auto scroll if flag set
            if st.session_state.get("scroll_to_details", False):
                js = """
                <script>
                var el = window.parent.document.querySelector("a[name='details']");
                if(el){ el.scrollIntoView({behavior: 'smooth'}); }
                </script>
                """
                st.components.v1.html(js, height=0)
                st.session_state.scroll_to_details = False

        # auto scroll back to table after closing
        if st.session_state.get("scroll_to_table", False):
            js = """
            <script>
            var el = window.parent.document.querySelector("a[name='transactions']");
            if(el){ el.scrollIntoView({behavior: 'smooth'}); }
            </script>
            """
            st.components.v1.html(js, height=0)
            st.session_state.scroll_to_table = False

        # Plot: Anomaly Distribution
        if "anomaly_type" in df.columns:
            anomaly_df = df.groupby("anomaly_type").size().reset_index(name="count")
            if not anomaly_df.empty:
                fig_anom = px.bar(anomaly_df, x="anomaly_type", y="count",
                                  title="Anomaly Distribution")
                st.plotly_chart(fig_anom, use_container_width=True)

    # ===================================================
    # 📆 SHORT-TERM METRICS PANEL (Today / Last 24 Hours)
    # ===================================================
    st.markdown("## 📆 Short-Term Metrics (Today / Last 24h)")

    if not df.empty:
        # Filter today
        today = datetime.now(timezone.utc).date()
        df_today = df[df["timestamp_initiated"].dt.date == today]

        # Refund-to-sale ratio by store
        if "transaction_status" in df_today.columns:
            refund_ratio = (
                df_today.groupby("store_name")["transaction_status"]
                .apply(lambda x: (x == "REFUND").mean())
                .reset_index(name="refund_ratio")
            )
            fig_refund = px.bar(refund_ratio, x="store_name", y="refund_ratio",
                                title="Refund-to-Sale Ratio by Store (Today)")
            st.plotly_chart(fig_refund, use_container_width=True)

        # Top SKUs sold in high-value anomalies today
        if {"sku", "transaction_amount", "is_anomaly"}.issubset(df_today.columns):
            sku_df = df_today[(df_today["is_anomaly"] == 1) & (df_today["transaction_amount"] > 2000)]
            if not sku_df.empty:
                top_skus = sku_df["sku"].value_counts().reset_index()
                top_skus.columns = ["sku", "count"]
                fig_sku = px.bar(top_skus.head(10), x="sku", y="count",
                                 title="Top SKUs in High-Value Anomalies (Today)")
                st.plotly_chart(fig_sku, use_container_width=True)

        # Hour-by-hour anomalies
        if "is_anomaly" in df_today.columns:
            df_today["hour"] = df_today["timestamp_initiated"].dt.hour
            hourly = df_today[df_today["is_anomaly"] == 1].groupby("hour").size().reset_index(name="count")
            fig_hour = px.line(hourly, x="hour", y="count",
                               title="Anomalies per Hour (Today)")
            st.plotly_chart(fig_hour, use_container_width=True)

        # Leaderboard of cashiers with most anomalies
        if {"cashier_id", "is_anomaly"}.issubset(df_today.columns):
            cashier_board = (
                df_today[df_today["is_anomaly"] == 1]
                .groupby("cashier_id")
                .size()
                .reset_index(name="anomalies")
                .sort_values("anomalies", ascending=False)
            )
            st.markdown("### 🏆 Cashier Anomaly Leaderboard (Today)")
            st.dataframe(cashier_board.head(10), use_container_width=True)

    # ===================================================
    # 📈 HISTORICAL TRENDS PANEL (Last 30 / 90 Days)
    # ===================================================
    st.markdown("## 📈 Historical Trends (30–90 Days)")

    if not df.empty:
        df_hist = df.copy()
        df_hist["date"] = df_hist["timestamp_initiated"].dt.date

        # Total anomalies per day (by type)
        if {"date", "is_anomaly", "anomaly_type"}.issubset(df_hist.columns):
            daily_anoms = (
                df_hist[df_hist["is_anomaly"] == 1]
                .groupby(["date", "anomaly_type"])
                .size()
                .reset_index(name="count")
            )
            fig_daily = px.line(daily_anoms, x="date", y="count",
                                color="anomaly_type",
                                title="Total Anomalies per Day (by Type)")
            st.plotly_chart(fig_daily, use_container_width=True)

        # Heatmap: Time-of-day vs anomaly frequency
        df_hist["hour"] = df_hist["timestamp_initiated"].dt.hour
        heatmap_data = (
            df_hist[df_hist["is_anomaly"] == 1]
            .groupby(["hour", "date"])
            .size()
            .reset_index(name="count")
        )
        if not heatmap_data.empty:
            fig_heat = px.density_heatmap(heatmap_data, x="hour", y="date", z="count",
                                          title="Anomaly Frequency Heatmap (Time of Day vs Date)")
            st.plotly_chart(fig_heat, use_container_width=True)

        # Top 5 stores by anomaly rate
        if {"store_name", "is_anomaly"}.issubset(df_hist.columns):
            store_stats = (
                df_hist.groupby("store_name")
                .agg(transactions=("transaction_id", "count"),
                     anomalies=("is_anomaly", "sum"))
                .reset_index()
            )
            store_stats["rate_per_1000"] = store_stats["anomalies"] / store_stats["transactions"] * 1000
            top_stores = store_stats.sort_values("rate_per_1000", ascending=False).head(5)
            fig_store = px.bar(top_stores, x="store_name", y="rate_per_1000",
                               title="Top 5 Stores by Anomaly Rate (per 1,000 txns)")
            st.plotly_chart(fig_store, use_container_width=True)

        # Top 5 products most targeted in fraud
        if {"sku", "is_anomaly"}.issubset(df_hist.columns):
            fraud_skus = (
                df_hist[df_hist["is_anomaly"] == 1]["sku"].value_counts().reset_index()
            )
            fraud_skus.columns = ["sku", "count"]
            fig_sku_fraud = px.bar(fraud_skus.head(5), x="sku", y="count",
                                   title="Top 5 Products Targeted in Fraud")
            st.plotly_chart(fig_sku_fraud, use_container_width=True)

        # Chargeback losses vs prevented fraud (dummy calc)
        if {"transaction_amount", "is_anomaly"}.issubset(df_hist.columns):
            chargeback_losses = df_hist[df_hist["is_anomaly"] == 1]["transaction_amount"].sum()
            prevented_fraud = df_hist[df_hist["is_anomaly"] == 0]["transaction_amount"].sum() * 0.01  # assume 1% prevented
            roi_df = pd.DataFrame({
                "Category": ["Chargeback Losses", "Prevented Fraud"],
                "Amount": [chargeback_losses, prevented_fraud]
            })
            fig_roi = px.bar(roi_df, x="Category", y="Amount",
                             title="Chargeback Losses vs Prevented Fraud")
            st.plotly_chart(fig_roi, use_container_width=True)

    # Auto-refresh trick
    if st_autorefresh:
        st.query_params["refresh"] = str(datetime.now(timezone.utc).timestamp())


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

