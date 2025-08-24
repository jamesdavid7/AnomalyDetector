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

st.set_page_config(page_title="🚨 Real-time Anomaly Alerts")

# 🚨 Sticky header with blinking siren + enable button
# 🚨 Sticky header with blinking siren + enable button
st.markdown("""
    <style>
        @keyframes blink {
            0%   { opacity: 1; }
            50%  { opacity: 0; }
            100% { opacity: 1; }
        }
        .sticky-header {
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            z-index: 9999; /* ensure it stays on top */
            background: white;
            padding: 10px 15px;
            border-bottom: 3px solid #ff4d4d;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }
        .sticky-header img {
            width: 40px;
            margin-right: 10px;
            animation: blink 1s infinite;
        }
        .sticky-header h1 {
            margin: 0;
            font-size: 1.5rem;
            display: flex;
            align-items: center;
        }
        .enable-btn {
            background-color: #ff4d4d;
            color: white;
            padding: 8px 15px;
            border: none;
            border-radius: 6px;
            cursor: pointer;
            font-weight: bold;
        }
        /* ✅ Correct Streamlit container */
        .block-container {
            padding-top: 90px !important;
        }
        .alert-box {
            padding: 10px;
            margin-bottom: 8px;
            border-left: 6px solid red;
            background-color: #ffe6e6;
            display: flex;
            align-items: center;
            border-radius: 8px;
        }
        .alert-box img {
            margin-right: 10px;
        }
    </style>

    <div class="sticky-header">
        <h1>
            <img src="https://cdn-icons-png.flaticon.com/512/564/564619.png">
            Real-time Anomaly Alerts
        </h1>
        <button class="enable-btn" onclick="enableAlerts()">🔊 Enable Alerts</button>
    </div>
""", unsafe_allow_html=True)



# -------------------------
# Auto-refresh every 10 sec
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
        print("✅ Connected to backend socket")
        event_queue.put({"log": "✅ Connected to backend socket"})

    @sio.on("anomaly_detected")
    def on_anomaly(data):
        event_queue.put({"anomaly": data})

    def socket_thread():
        try:
            sio.connect(API_URL)
            sio.wait()
        except Exception as e:
            print(f"Failed to connect to backend SocketIO: {e}")
            event_queue.put({"log": f"❌ SocketIO connection failed: {e}"})

    threading.Thread(target=socket_thread, daemon=True).start()
    st.session_state.socket_started = True

# -------------------------
# Inject global JS ONCE for audio unlock
# -------------------------
if "audio_injected" not in st.session_state:
    st.session_state.audio_injected = True
    st.markdown("""
    <script>
    // global audio element
    var anomalyAudio = new Audio('https://actions.google.com/sounds/v1/alarms/alarm_clock.ogg');
    anomalyAudio.loop = true;

    function enableAlerts() {
        anomalyAudio.play().then(() => {
            anomalyAudio.pause();
            anomalyAudio.currentTime = 0;
            alert("🔊 Alerts enabled! Sound will play on anomalies.");
        }).catch(err => console.log("Unlock failed:", err));
    }
    </script>
    """, unsafe_allow_html=True)

# -------------------------
# Process events
# -------------------------
def process_events():
    while not event_queue.empty():
        item = event_queue.get_nowait()
        print(f"📥 Processing item: {item}")

        if "log" in item:
            notification_div.markdown(f"""
            <script>console.log("✅ Notification fired: {item['log']}");</script>
            """, unsafe_allow_html=True)

        if "anomaly" in item:
            txn = item["anomaly"]
            txn_id = txn.get("transaction_id", "unknown")
            if txn_id not in st.session_state.notified_txns:
                txn["received_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                st.session_state.anomalies_received.append(txn)
                st.session_state.notified_txns.add(txn_id)

                message = f"Txn {txn_id} flagged! Customer: {txn['customer_name']}, Amount: {txn['amount']}"
                notification_div.markdown(f"""
                <script>
                if (typeof anomalyAudio !== "undefined") {{
                    anomalyAudio.currentTime = 0;
                    anomalyAudio.play().catch(err => console.log("Play failed:", err));
                }}

                function showNotification() {{
                    new Notification("🚨 Anomaly Detected", {{
                        body: "{message}",
                        icon: "https://cdn-icons-png.flaticon.com/512/564/564619.png"
                    }});
                }}

                if (window.Notification) {{
                    if (Notification.permission === "granted") {{
                        showNotification();
                    }} else if (Notification.permission !== "denied") {{
                        Notification.requestPermission().then(function(permission) {{
                            if (permission === "granted") showNotification();
                        }});
                    }}
                }}
                </script>
                """, unsafe_allow_html=True)
                print(f"✅ Notification fired for txn: {txn_id}")

process_events()

# -------------------------
# Display anomalies
# -------------------------
if st.session_state.anomalies_received:
    for txn in reversed(st.session_state.anomalies_received[-10:]):
        st.markdown(f"""
        <div class="alert-box">
            <img src="https://cdn-icons-png.flaticon.com/512/564/564619.png" width="32" />
            <div>
                <strong>Txn ID:</strong> {txn['transaction_id']} &nbsp;&nbsp;
                <strong>Customer:</strong> {txn['customer_name']} &nbsp;&nbsp;
                <strong>Amount:</strong> {txn['amount']} &nbsp;&nbsp;
                <em>{txn['received_at']}</em>
            </div>
        </div>
        """, unsafe_allow_html=True)
