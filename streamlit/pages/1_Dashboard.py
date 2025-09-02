import traceback
from urllib.parse import quote

import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import streamlit.components.v1 as components
import queue
import threading
import socketio
from streamlit_autorefresh import st_autorefresh
import os

from datetime import datetime, timezone

# Initialize session state variable if it doesn't exist

st.set_page_config(layout="wide")
st.title("🧠 POS Anomaly Dashboard")

# ---------------- API Endpoints ----------------
API_BASE = os.getenv("API_URL", "http://flask_api:5000")
TRANSACTIONS_ENDPOINT = f"{API_BASE}/anomaly_transaction/list"   # new endpoint to fetch recent txns
SETTLEMENT_TRANSACTIONS_ENDPOINT = f"{API_BASE}/batch_anomaly_transaction/list"   # new endpoint to fetch recent txns
REALTIME_COUNTERS_ENDPOINT = f"{API_BASE}/anomaly_transaction/real_time_counters"
METRICS_ENDPOINT = f"{API_BASE}/metrics"             # anomaly metrics (historical)
DOWNLOAD_ENDPOINT = f"{API_BASE}/download"           # anomaly csv download
# ===================================================
# Helpers
# ===================================================
def fetch_transactions(limit: int = 10, start_key=None, sort_order: str = "desc"):
    """Fetch transactions from Flask API with DynamoDB pagination."""
    try:
        url = f"{TRANSACTIONS_ENDPOINT}?limit={limit}&sort_order={sort_order}"
        # url = f"{TRANSACTIONS_ENDPOINT_ALL}"
        if start_key:
            # ✅ Ensure next_token is URL-safe
            url += f"&last_evaluated_key={quote(start_key)}"

        res = requests.get(url)
        if res.status_code == 200:
            data = res.json()

            # ✅ Build DataFrame from items
            df = pd.DataFrame(data.get("items", []))
            # df = pd.DataFrame(data)
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

def fetch_settlement_transactions(limit: int = 10, start_key=None, sort_order: str = "desc"):
    """Fetch fetch_settlement_transactions from Flask API with DynamoDB pagination."""
    try:
        url = f"{SETTLEMENT_TRANSACTIONS_ENDPOINT}?limit={limit}&sort_order={sort_order}"
        if start_key:
            # ✅ Ensure next_token is URL-safe
            url += f"&last_evaluated_key={quote(start_key)}"

        res = requests.get(url)
        if res.status_code == 200:
            data = res.json()

            # ✅ Build DataFrame from items
            settlement_df = pd.DataFrame(data.get("items", []))
            # df = pd.DataFrame(data)
            # ✅ Normalize timestamps if present
            if not settlement_df.empty and "timestamp_initiated" in settlement_df.columns:
                # Ensure numeric first to avoid FutureWarning
                settlement_df["timestamp_initiated"] =  pd.to_datetime(
                    pd.to_numeric(settlement_df["timestamp_initiated"], errors="coerce"),
                    unit="ms",
                    utc=True
                )

                settlement_df["timestamp_initiated_utc"] = settlement_df["timestamp_initiated"].dt.strftime(
                    "%Y-%m-%d %H:%M:%S UTC"
                )

            # ✅ Return DataFrame and next_token string
            return settlement_df, data.get("next_token")

    except Exception as e:
        print(traceback.format_exc())
        st.error(f"Error fetching settlement transactions: {e}")

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
page = st.sidebar.radio("📑 Select View", ["📡 Real-Time Dashboard"])
# ---------------- Sidebar Navigation ----------------
st.sidebar.markdown("## 🔗 Navigation")
st.sidebar.markdown("""
- [📡 Live Transactions & Metrics](#live-transactions--metrics)
- [📡 Real-Time Counters](#real-time-counters)
- [📝 Transactions](#transactions)
- [📄 Settlement Anomaly Transactions](#settlement-transactions)
- [📊 Settlement Metrics](#settlement-metrics)
""", unsafe_allow_html=True)


# ===================================================
# PAGE 1: REAL-TIME DASHBOARD (with pagination + alerts + counters)
# ===================================================
if page == "📡 Real-Time Dashboard":
    st.markdown("<a name='live-transactions--metrics'></a>", unsafe_allow_html=True)
    st.subheader("📡 Live Transactions & Metrics")

    # --- Pagination state ---
    if "last_key" not in st.session_state:
        st.session_state.last_key = None
    if "page_stack" not in st.session_state:
        st.session_state.page_stack = []
    if "page_num" not in st.session_state:
        st.session_state.page_num = 1

    # -------------------------
    # Iframe for audio
    # -------------------------
    if "iframe_rendered" not in st.session_state:
        st.session_state.iframe_rendered = False

    iframe_html = """
       <iframe id="audioFrame" srcdoc='
         <html>
         <body style="display:flex; flex-direction:column; align-items:center; justify-content:center; height:100%;">
           <button id="unlockBtn">🔊 Enable Audio</button>
           <audio id="beep" preload="auto">
             <source src="https://actions.google.com/sounds/v1/alarms/beep_short.ogg" type="audio/ogg">
           </audio>
           <script>
             const btn = document.getElementById("unlockBtn");
             const audio = document.getElementById("beep");

             btn.addEventListener("click", () => {
               audio.play().then(() => {
                 window.audioReady = true;
                 alert("✅ Audio unlocked! You can now hear alerts.");
               }).catch(e => alert("Audio blocked: " + e));
             });

             function playAnomaly(message){
               if(window.audioReady){
                 audio.currentTime = 0;
                 audio.play().catch(e=>console.log("Beep error:", e));
                 if(Notification.permission === "granted"){
                   new Notification("🚨 Anomaly Detected", { body: message });
                 } else if(Notification.permission !== "denied"){
                   Notification.requestPermission().then(p => { 
                     if(p==="granted") new Notification("🚨 Anomaly Detected",{ body: message }); 
                   });
                 }
               }
             }

             window.addEventListener("message", (event) => {
               if(event.data.type === "anomaly"){
                 playAnomaly(event.data.message);
               }
             });
           </script>
         </body>
         </html>
       ' style="width:100%; height:150px; border:none;"></iframe>
       """

    if not st.session_state.iframe_rendered:
        components.html(iframe_html, height=150)
        st.session_state.iframe_rendered = True

    # -------------------------
    # Auto-refresh
    # -------------------------
    st_autorefresh(interval=15_000, key="anomaly_refresh")

    # -------------------------
    # State
    # -------------------------
    if "event_queue" not in st.session_state:
        st.session_state.event_queue = queue.Queue()
    if "anomalies_received" not in st.session_state:
        st.session_state.anomalies_received = []
    if "notified_txns" not in st.session_state:
        st.session_state.notified_txns = set()
    if "socket_started" not in st.session_state:
        st.session_state.socket_started = False

    event_queue = st.session_state.event_queue
    notification_div = st.empty()

    # -------------------------
    # SocketIO client
    # -------------------------
    if not st.session_state.socket_started:
        sio = socketio.Client(logger=False, engineio_logger=False)


        @sio.event
        def connect():
            event_queue.put({"log": "✅ Connected to backend socket"})


        @sio.on("anomaly_detected")
        def on_anomaly(data):
            event_queue.put({"anomaly": data})

        def socket_thread():
            try:
                sio.connect(API_BASE)
                sio.wait()
            except Exception as e:
                event_queue.put({"log": f"❌ SocketIO connection failed: {e}"})


        threading.Thread(target=socket_thread, daemon=True).start()
        st.session_state.socket_started = True


    # -------------------------
    # Process events
    # -------------------------
    def process_events():
        while not event_queue.empty():
            item = event_queue.get_nowait()
            if "log" in item:
                notification_div.markdown(f"<script>console.log({repr(item['log'])});</script>", unsafe_allow_html=True)
            if "anomaly" in item:
                txn = item["anomaly"]
                txn_id = txn.get("transaction_id", "unknown")
                if txn_id not in st.session_state.notified_txns:
                    txn["received_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    st.session_state.anomalies_received.append(txn)
                    st.session_state.notified_txns.add(txn_id)

                    message = f"Txn {txn_id} flagged! Customer: {txn.get('customer_name', '')} Amount: {txn.get('transaction_amount', '')}"
                    notification_div.markdown(f"""
                    <script>
                      const iframe = document.querySelector("iframe#audioFrame");
                      function sendToIframe(){{
                        if(iframe && iframe.contentWindow){{
                          iframe.contentWindow.postMessage({{type:"anomaly", message:{message!r}}}, "*");
                        }} else {{
                          setTimeout(sendToIframe, 200);
                        }}
                      }}
                      sendToIframe();
                    </script>
                    """, unsafe_allow_html=True)


    process_events()

    # -------------------------
    # Display anomalies
    # -------------------------
    if st.session_state.anomalies_received:
        st.markdown("""
        <style>
            .alert-box {
                padding: 8px 12px;
                margin-bottom: 10px;
                border-radius: 8px;
                background: #ffecec;
                border: 1px solid #ffb3b3;
            }
            .alert-box div {
                display: flex;
                align-items: center;
                gap: 10px;
            }
            .alert-box img {
                flex-shrink: 0;
            }
        </style>
        """, unsafe_allow_html=True)

        # -------------------------
        # Display anomalies
        # -------------------------
        if st.session_state.anomalies_received:
            st.markdown("""
            <style>
                .alert-box {
                    padding: 8px 12px;
                    margin-bottom: 10px;
                    border-radius: 8px;
                    background: #ffecec;
                    border: 1px solid #ffb3b3;
                }
                .alert-box div {
                    display: flex;
                    align-items: center;
                    gap: 10px;
                }
                .alert-box img {
                    flex-shrink: 0;
                }
            </style>
            """, unsafe_allow_html=True)

        # Build one big HTML block instead of multiple isolated ones
        html_content = """
          <div style="display:flex; justify-content:flex-end; margin-bottom:10px;">
              <button onclick="stopAllAlarms()"
                      style="padding:6px 10px; border:none; border-radius:8px; background:#d9534f; color:white; cursor:pointer;">
                  🔇
              </button>
          </div>
          """

        # Add anomalies with inline <audio>
        for txn in reversed(st.session_state.anomalies_received[-10:]):
            txn_id = txn.get("transaction_id", "")
            txn_amount = txn.get("amount", "")
            customer = txn.get('customer_name', '')
            received_at = txn.get('received_at', '')

            html_content += f"""
              <div class="alert-box">
                  <div class="alert-header">
                      <img src="https://cdn-icons-png.flaticon.com/512/564/564619.png" width="32" />
                      <strong>Txn ID:</strong> {txn_id}
                      <strong>Amount:</strong> {txn_amount}
                      <strong>Customer:</strong> {customer}
                      <strong>DateTime:</strong> <em>{received_at}</em>
                  </div>

                  <!-- Inline audio -->
                  <audio id="alarmSound_{txn_id}" autoplay>
                      <source src="https://actions.google.com/sounds/v1/alarms/spaceship_alarm.ogg" type="audio/ogg">
                  </audio>
              </div>
              """

        # Add script + styles at the end
        html_content += """
          <script>
          function stopAllAlarms() {
              var audios = document.querySelectorAll("audio");
              audios.forEach(audio => {
                  audio.pause();
                  audio.currentTime = 0;
              });
          }
          </script>

          <style>
          .alert-box {
              background-color: #ffecec;
              border: 1px solid #f5c2c2;
              border-radius: 10px;
              padding: 10px;
              margin: 8px 0;
          }
          .alert-header {
              display: flex;
              align-items: center;
              gap: 12px;
              flex-wrap: wrap;
          }
          </style>
          """

        # Render everything inside a single iframe
        components.html(html_content, height=200, scrolling=True)

    # # --- Auto refresh ---
    # refresh_rate = st.sidebar.slider("Auto-refresh (seconds)", 5, 60, 15)
    # st_autorefresh = st.sidebar.checkbox("🔄 Auto Refresh", value=True)

    # --- Fetch transactions with pagination ---
    df, next_key = fetch_transactions(limit=10, start_key=st.session_state.last_key)

    if df.empty:
        st.warning("No transactions available yet.")
    else:
        # KPIs
        # col1, col2, col3 = st.columns(3)
        # col1.metric("Total Transactions", len(df))
        # col2.metric("Anomalies", int(df["is_anomaly"].sum()) if "is_anomaly" in df else 0)
        # col3.metric("Avg Amount", round(df["transaction_amount"].mean(), 2))  # ✅ fixed key

        # ================= LIVE ALERTS =================
        # st.markdown("### 🚨 Live Alerts")
        # alerts = df[df.get("is_anomaly", 0) == 1].sort_values(
        #     "timestamp_initiated", ascending=False
        # )
        # if alerts.empty:
        #     st.info("No live alerts detected.")
        # else:
        #     for _, row in alerts.head(5).iterrows():
        #         ts = row["timestamp_initiated"]
        #         if pd.notna(ts):  # ✅ Only format valid timestamps
        #             ts = ts.strftime("%I:%M %p")
        #         else:
        #             ts = "Unknown Time"
        #         st.write(
        #             f"**{ts} — ALERT:** {row.get('anomaly_type', 'Unknown')} "
        #             f"at {row.get('store_name', 'N/A')}"
        #         )

        # ---------------- Real-Time Counters ----------------
        st.markdown("<a name='real-time-counters'></a>", unsafe_allow_html=True)
        st.markdown("### 📊 Real-Time Counters")

        try:
            response = requests.get(REALTIME_COUNTERS_ENDPOINT)  # adjust host/port if needed
            if response.status_code == 200:
                stats = response.json()
            else:
                st.error(f"Failed to fetch stats: {response.status_code}")
                stats = {}
        except Exception as e:
            st.error(f"Error fetching stats: {e}")
            stats = {}

        # Extract values (fallback to 0 if missing)
        high_value = stats.get("high_value_count", 0)
        refunds_last_10 = stats.get("refunds_last_10_count", 0)
        outside_hours = stats.get("outside_hours_count", 0)
        manual_ratio = stats.get("manual_entry_ratio", 0)

        # Display as metric cards
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("High-value txns (15m)", high_value, "⚠ Above avg" if high_value > 5 else "")
        col2.metric("Refunds (10m)", refunds_last_10)
        col3.metric("Outside store hours", outside_hours)
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
    # 📆 Batch METRICS PANEL (Today / Last 24 Hours)
    # ===================================================
    # Paginated transactions
    st.markdown("<a name='settlement-transactions'></a>", unsafe_allow_html=True)
    st.header("📄 Settlement Anomaly Transactions")
    if "s_last_key" not in st.session_state:
        st.session_state.s_last_key = None
    if "s_page_stack" not in st.session_state:
        st.session_state.s_page_stack = []
    if "s_page_num" not in st.session_state:
        st.session_state.s_page_num = 1
    # --- Fetch transactions with pagination ---
    settlement_df, s_next_key = fetch_settlement_transactions(limit=10, start_key=st.session_state.s_last_key)

    if df.empty:
        st.warning("No settlement transactions available yet.")
    else:
        st.markdown("<a name='settlement_transactions'></a>", unsafe_allow_html=True)  # anchor at top of table
        st.markdown(f"### 📝 Settlement Transactions (Page {st.session_state.s_page_num})")

        # Columns to display (always exist)
        s_display_cols = [
            "transaction_id",
            "store_name",
            "transaction_amount",
            "currency",
            "transaction_status",
            "timestamp_initiated_utc",
            "is_anomaly"
        ]


        # Add anomaly_type if present in df
        if "anomaly_type" in settlement_df.columns:
            s_display_cols.append("anomaly_type")

        # Map column names to shorter display names
        s_col_display_names = {
            "transaction_id": "ID",
            "store_name": "Store",
            "transaction_amount": "Amount",
            "currency": "Cur",
            "transaction_status": "Status",
            "timestamp_initiated_utc": "Initiated (UTC)",
            "is_anomaly": "Anomaly?",
            "anomaly_type": "Type"
        }

        # If timestamps look like "1.6e18" → nanoseconds
        settlement_df["timestamp_initiated_utc"] = pd.to_datetime(
            settlement_df["timestamp_initiated_utc"], unit="ns", errors="coerce", utc=True
        )

        # If timestamps look like "1.6e9" → seconds
        settlement_df["timestamp_initiated_utc"] = pd.to_datetime(
            settlement_df["timestamp_initiated_utc"], unit="s", errors="coerce", utc=True
        )

        # Subset dataframe
        s_display_df = settlement_df[s_display_cols]

        # ----- Header row -----
        s_header_cols = st.columns(len(s_display_cols) + 1)
        for i, col in enumerate(s_display_cols):
            s_header_cols[i].markdown(f"**{s_col_display_names.get(col, col)}**")
        s_header_cols[-1].markdown("**Action**")

        # ----- Data rows with inline button -----
        for i, row in s_display_df.iterrows():
            cols = st.columns(len(s_display_cols) + 1)
            for j, col_name in enumerate(s_display_cols):
                value = row[col_name]
                # anomaly highlighting
                if col_name == "is_anomaly" and row["is_anomaly"]:
                    cols[j].markdown(f"<div style='background-color:#ffcccc'>{value}</div>", unsafe_allow_html=True)
                else:
                    cols[j].write(value)
            # Simple icon button for details
            if cols[-1].button("🔍", key=f"view_{row['transaction_id']}"):
                st.session_state.s_selected_txn = settlement_df.iloc[i].to_dict()
                st.session_state.s_scroll_to_details = True
                st.rerun()

        # ================= Pagination controls =================
        # (stick directly under the table regardless of details)
        s_nav1, s_nav2, s_nav3 = st.columns([1, 6, 1])
        with s_nav1:
            if st.button("⬅️ Previous", key="s_prev") and st.session_state.s_page_stack:
                st.session_state.s_last_key = st.session_state.s_page_stack.pop()
                st.session_state.s_page_num -= 1
                st.rerun()
        with s_nav3:
            if s_next_key:
                if st.button("Next ➡️", key="s_nxt"):
                    st.session_state.s_page_stack.append(st.session_state.s_last_key)
                    st.session_state.s_last_key = s_next_key
                    st.session_state.s_page_num += 1
                    st.rerun()

        # ================= Popup (2-column key/value layout) =================
        if "s_selected_txn" in st.session_state and st.session_state.s_selected_txn:
            st.markdown("<a name='settlement_details'></a>", unsafe_allow_html=True)  # anchor only if visible
            st.markdown("## 🔍 Settlement Transaction Details")
            s_selected = st.session_state.s_selected_txn

            # Render as key-value pairs
            for k, v in s_selected.items():
                c1, c2 = st.columns([1, 3])
                c1.markdown(f"**{s_col_display_names.get(k, k)}**")
                c2.write(v)

            if st.button("❌ Close", key="s_close"):
                st.session_state.s_selected_txn = None
                st.session_state.s_scroll_to_table = True
                st.rerun()

            # auto scroll if flag set
            if st.session_state.get("s_scroll_to_details", False):
                js = """
                    <script>
                    var el = window.parent.document.querySelector("a[name='settlement_details']");
                    if(el){ el.scrollIntoView({behavior: 'smooth'}); }
                    </script>
                    """
                st.components.v1.html(js, height=0)
                st.session_state.s_scroll_to_details = False

        # auto scroll back to table after closing
        if st.session_state.get("s_scroll_to_table", False):
            js = """
                <script>
                var el = window.parent.document.querySelector("a[name='settlement_transactions']");
                if(el){ el.scrollIntoView({behavior: 'smooth'}); }
                </script>
                """
            st.components.v1.html(js, height=0)
            st.session_state.s_scroll_to_table = False

    # Metrics
    st.markdown("<a name='settlement-metrics'></a>", unsafe_allow_html=True)
    st.header("📊 Settlement Metrics")
    metrics = requests.get(f"{API_BASE}/batch_anomaly_transactions/metrics").json()

    # --- Helper function for color-coded emojis ---
    def anomaly_icon(value):
        if value > 10:
            return "🔴"
        elif value > 5:
            return "🟠"
        elif value > 0:
            return "🟢"
        return "✅"  # No anomalies


    # --- KPI Section ---
    st.subheader("📌 Key Metrics")
    col1, col2, col3 = st.columns(3)

    last_24h = sum(metrics.get("last_24h", {}).values())
    last_week = sum(metrics.get("last_week", {}).values())
    last_month = sum(metrics.get("last_month", {}).values())

    with col1:
        st.metric("Last 24h", last_24h, help=f"Status: {anomaly_icon(last_24h)}")
    with col2:
        st.metric("Last Week", last_week, help=f"Status: {anomaly_icon(last_week)}")
    with col3:
        st.metric("Last Month", last_month, help=f"Status: {anomaly_icon(last_month)}")

    # --- Breakdown with filter ---
    st.subheader("🔎 Settlements Anomaly Breakdown")

    timeframe = st.radio("Select timeframe", ["last_24h", "last_week", "last_month"], index=2)
    selected_data = metrics.get(timeframe, {})

    if not selected_data:  # Handle no data
        st.info(f"No anomalies detected in {timeframe.replace('_', ' ')} ✅")
    else:
        df_breakdown = pd.DataFrame(list(selected_data.items()), columns=["Anomaly Type", "Count"])

        # Add percentage contribution
        df_breakdown["% Contribution"] = (
                df_breakdown["Count"] / df_breakdown["Count"].sum() * 100
        ).round(1)

        # Pie chart
        fig = px.pie(
            df_breakdown,
            names="Anomaly Type",
            values="Count",
            title=f"Anomaly Distribution - {timeframe.replace('_', ' ').title()}",
            hole=0.3
        )

        # Show chart + table
        colA, colB = st.columns(2)
        with colA:
            st.plotly_chart(fig, use_container_width=True)
        with colB:
            st.dataframe(df_breakdown, use_container_width=True)
    #
    # # ===================================================
    # # 📆 SHORT-TERM METRICS PANEL (Today / Last 24 Hours)
    # # ===================================================
    # st.markdown("## 📆 Short-Term Metrics (Today / Last 24h)")
    #
    # if not df.empty:
    #     # Filter today
    #     today = datetime.now(timezone.utc).date()
    #     df_today = df[df["timestamp_initiated"].dt.date == today]
    #
    #     # Refund-to-sale ratio by store
    #     if "transaction_status" in df_today.columns:
    #         refund_ratio = (
    #             df_today.groupby("store_name")["transaction_status"]
    #             .apply(lambda x: (x == "REFUND").mean())
    #             .reset_index(name="refund_ratio")
    #         )
    #         fig_refund = px.bar(refund_ratio, x="store_name", y="refund_ratio",
    #                             title="Refund-to-Sale Ratio by Store (Today)")
    #         st.plotly_chart(fig_refund, use_container_width=True)
    #
    #     # Top SKUs sold in high-value anomalies today
    #     if {"sku", "transaction_amount", "is_anomaly"}.issubset(df_today.columns):
    #         sku_df = df_today[(df_today["is_anomaly"] == 1) & (df_today["transaction_amount"] > 2000)]
    #         if not sku_df.empty:
    #             top_skus = sku_df["sku"].value_counts().reset_index()
    #             top_skus.columns = ["sku", "count"]
    #             fig_sku = px.bar(top_skus.head(10), x="sku", y="count",
    #                              title="Top SKUs in High-Value Anomalies (Today)")
    #             st.plotly_chart(fig_sku, use_container_width=True)
    #
    #     # Hour-by-hour anomalies
    #     if "is_anomaly" in df_today.columns:
    #         df_today["hour"] = df_today["timestamp_initiated"].dt.hour
    #         hourly = df_today[df_today["is_anomaly"] == 1].groupby("hour").size().reset_index(name="count")
    #         fig_hour = px.line(hourly, x="hour", y="count",
    #                            title="Anomalies per Hour (Today)")
    #         st.plotly_chart(fig_hour, use_container_width=True)
    #
    #     # Leaderboard of cashiers with most anomalies
    #     if {"cashier_id", "is_anomaly"}.issubset(df_today.columns):
    #         cashier_board = (
    #             df_today[df_today["is_anomaly"] == 1]
    #             .groupby("cashier_id")
    #             .size()
    #             .reset_index(name="anomalies")
    #             .sort_values("anomalies", ascending=False)
    #         )
    #         st.markdown("### 🏆 Cashier Anomaly Leaderboard (Today)")
    #         st.dataframe(cashier_board.head(10), use_container_width=True)
    #
    # # ===================================================
    # # 📈 HISTORICAL TRENDS PANEL (Last 30 / 90 Days)
    # # ===================================================
    # st.markdown("## 📈 Historical Trends (30–90 Days)")
    #
    # if not df.empty:
    #     df_hist = df.copy()
    #     df_hist["date"] = df_hist["timestamp_initiated"].dt.date
    #
    #     # Total anomalies per day (by type)
    #     if {"date", "is_anomaly", "anomaly_type"}.issubset(df_hist.columns):
    #         daily_anoms = (
    #             df_hist[df_hist["is_anomaly"] == 1]
    #             .groupby(["date", "anomaly_type"])
    #             .size()
    #             .reset_index(name="count")
    #         )
    #         fig_daily = px.line(daily_anoms, x="date", y="count",
    #                             color="anomaly_type",
    #                             title="Total Anomalies per Day (by Type)")
    #         st.plotly_chart(fig_daily, use_container_width=True)
    #
    #     # Heatmap: Time-of-day vs anomaly frequency
    #     df_hist["hour"] = df_hist["timestamp_initiated"].dt.hour
    #     heatmap_data = (
    #         df_hist[df_hist["is_anomaly"] == 1]
    #         .groupby(["hour", "date"])
    #         .size()
    #         .reset_index(name="count")
    #     )
    #     if not heatmap_data.empty:
    #         fig_heat = px.density_heatmap(heatmap_data, x="hour", y="date", z="count",
    #                                       title="Anomaly Frequency Heatmap (Time of Day vs Date)")
    #         st.plotly_chart(fig_heat, use_container_width=True)
    #
    #     # Top 5 stores by anomaly rate
    #     if {"store_name", "is_anomaly"}.issubset(df_hist.columns):
    #         store_stats = (
    #             df_hist.groupby("store_name")
    #             .agg(transactions=("transaction_id", "count"),
    #                  anomalies=("is_anomaly", "sum"))
    #             .reset_index()
    #         )
    #         store_stats["rate_per_1000"] = store_stats["anomalies"] / store_stats["transactions"] * 1000
    #         top_stores = store_stats.sort_values("rate_per_1000", ascending=False).head(5)
    #         fig_store = px.bar(top_stores, x="store_name", y="rate_per_1000",
    #                            title="Top 5 Stores by Anomaly Rate (per 1,000 txns)")
    #         st.plotly_chart(fig_store, use_container_width=True)
    #
    #     # Top 5 products most targeted in fraud
    #     if {"sku", "is_anomaly"}.issubset(df_hist.columns):
    #         fraud_skus = (
    #             df_hist[df_hist["is_anomaly"] == 1]["sku"].value_counts().reset_index()
    #         )
    #         fraud_skus.columns = ["sku", "count"]
    #         fig_sku_fraud = px.bar(fraud_skus.head(5), x="sku", y="count",
    #                                title="Top 5 Products Targeted in Fraud")
    #         st.plotly_chart(fig_sku_fraud, use_container_width=True)
    #
    #     # Chargeback losses vs prevented fraud (dummy calc)
    #     if {"transaction_amount", "is_anomaly"}.issubset(df_hist.columns):
    #         chargeback_losses = df_hist[df_hist["is_anomaly"] == 1]["transaction_amount"].sum()
    #         prevented_fraud = df_hist[df_hist["is_anomaly"] == 0]["transaction_amount"].sum() * 0.01  # assume 1% prevented
    #         roi_df = pd.DataFrame({
    #             "Category": ["Chargeback Losses", "Prevented Fraud"],
    #             "Amount": [chargeback_losses, prevented_fraud]
    #         })
    #         fig_roi = px.bar(roi_df, x="Category", y="Amount",
    #                          title="Chargeback Losses vs Prevented Fraud")
    #         st.plotly_chart(fig_roi, use_container_width=True)

    # Auto-refresh trick
    if st_autorefresh:
        st.query_params["refresh"] = str(datetime.now(timezone.utc).timestamp())