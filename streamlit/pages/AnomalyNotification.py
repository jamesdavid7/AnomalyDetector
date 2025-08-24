import streamlit as st
import socketio
import threading
import queue
import os
from datetime import datetime
from streamlit_autorefresh import st_autorefresh

# -------------------------
# Configuration
# -------------------------
API_URL = os.environ.get("API_URL", "http://api:5000")

st.set_page_config(page_title="🚨 Real-time Anomaly Alerts", layout="wide")

st.markdown(
    "<h1 style='margin-bottom:8px;'>🚨 Real-time Anomaly Alerts</h1>",
    unsafe_allow_html=True,
)
# -------------------------
# Auto-refresh
# -------------------------
st_autorefresh(interval=10_000, key="anomaly_refresh")

# -------------------------
# Persisted state
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
            sio.connect(API_URL)
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
            notification_div.markdown(
                f"<script>console.log({repr(item['log'])});</script>",
                unsafe_allow_html=True
            )

        if "anomaly" in item:
            txn = item["anomaly"]
            txn_id = txn.get("transaction_id", "unknown")
            if txn_id not in st.session_state.notified_txns:
                txn["received_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                st.session_state.anomalies_received.append(txn)
                st.session_state.notified_txns.add(txn_id)

                # JS: beep & notification
                message = f"Txn {txn_id} flagged! Customer: {txn.get('customer_name','')}, Amount: {txn.get('amount','')}"
                notification_div.markdown(f"""
                <script>
                  if (window.audioReady) {{ try {{ window.playBeep(3); }} catch(e) {{ console.log(e); }} }}
                  function showNotification() {{
                    new Notification("🚨 Anomaly Detected", {{
                      body: {message!r},
                      icon: "https://cdn-icons-png.flaticon.com/512/564/564619.png"
                    }});
                  }}
                  if ("Notification" in window) {{
                    if (Notification.permission === "granted") {{
                      showNotification();
                    }} else if (Notification.permission !== "denied") {{
                      Notification.requestPermission().then(p => {{ if (p === "granted") showNotification(); }});
                    }}
                  }}
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
            align-items: center;  /* vertically align image + text */
            gap: 10px;            /* spacing between image and text */
        }
        .alert-box img {
            flex-shrink: 0;       /* image stays fixed size */
        }
    </style>
    """, unsafe_allow_html=True)

    for txn in reversed(st.session_state.anomalies_received[-10:]):
        st.markdown(f"""
        <div class="alert-box">
            <div>
                <img src="https://cdn-icons-png.flaticon.com/512/564/564619.png" width="32" />
                <div>
                    <strong>Txn ID:</strong> {txn.get('transaction_id','')} &nbsp;&nbsp;
                    <strong>Customer:</strong> {txn.get('customer_name','')} &nbsp;&nbsp;
                    <strong>Amount:</strong> {txn.get('amount','')} &nbsp;&nbsp;
                    <em>{txn.get('received_at','')}</em>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

