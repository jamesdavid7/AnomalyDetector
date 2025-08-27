import streamlit as st
import queue
import threading
from datetime import datetime
import socketio
from streamlit_autorefresh import st_autorefresh
import streamlit.components.v1 as components
import os

# -------------------------
# Config
# -------------------------
API_URL = os.environ.get("API_URL", "http://api:5000")
st.set_page_config(page_title="🚨 Real-time Anomaly Alerts", layout="wide")
st.markdown("<h1>🚨 Real-time Anomaly Alerts</h1>", unsafe_allow_html=True)

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
st_autorefresh(interval=10_000, key="anomaly_refresh")

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
            notification_div.markdown(f"<script>console.log({repr(item['log'])});</script>", unsafe_allow_html=True)
        if "anomaly" in item:
            txn = item["anomaly"]
            txn_id = txn.get("transaction_id", "unknown")
            if txn_id not in st.session_state.notified_txns:
                txn["received_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                st.session_state.anomalies_received.append(txn)
                st.session_state.notified_txns.add(txn_id)

                message = f"Txn {txn_id} flagged! Customer: {txn.get('customer_name', '')}, Amount: {txn.get('amount', '')}"
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
        customer = txn.get('customer_name', '')
        received_at = txn.get('received_at', '')

        html_content += f"""
          <div class="alert-box">
              <div class="alert-header">
                  <img src="https://cdn-icons-png.flaticon.com/512/564/564619.png" width="32" />
                  <strong>Txn ID:</strong> {txn_id}
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
    components.html(html_content, height=600, scrolling=True)




