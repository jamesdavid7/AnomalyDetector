import streamlit as st
import pandas as pd
import plotly.express as px

# Load analyzed CSV data
file_path = "output/transactions_with_final_analysis.csv"
df = pd.read_csv(file_path)

# Rename columns for consistency if needed
df.rename(columns={
    "transactionId": "Transaction ID",
    "transactionType": "Type",
    "transactionDate": "Timestamp",
    "merchantName": "Merchant",
    "storeName": "Store",
    "deviceName": "POS ID",
    "location": "City",
    "totalAmount": "Amount",
    "cardHolderName": "Cashier",
    "cardType": "Card_Type",
    "currencyCode": "Currency",
    "transactionStatus": "POS_Status",
    "failureReason": "Bank_Status"
}, inplace=True)

# Page title
st.title("📊 POS Anomaly Insights Dashboard")

# Sidebar filters
card_filter = st.sidebar.multiselect("Filter by Card Type", options=df["Card_Type"].unique(), default=df["Card_Type"].unique())
anomaly_filter = st.sidebar.selectbox("Anomaly Filter", ["All", "Only Anomalies", "Only Normal"])

# Filtered dataset
filtered_df = df[df["Card_Type"].isin(card_filter)]
if anomaly_filter == "Only Anomalies":
    filtered_df = filtered_df[filtered_df["Anomaly"] == True]
elif anomaly_filter == "Only Normal":
    filtered_df = filtered_df[filtered_df["Anomaly"] == False]

# Main table
st.subheader("📋 Transaction Records")
st.dataframe(filtered_df, use_container_width=True)

# Anomaly distribution
st.subheader("🔍 Anomaly Classification Summary")
if "Classification" in df.columns:
    class_counts = df[df["Anomaly"] == True]["Classification"].value_counts().reset_index()
    class_counts.columns = ["Anomaly Type", "Count"]
    fig = px.bar(class_counts, x="Anomaly Type", y="Count", color="Anomaly Type", title="Anomalies by Classification")
    st.plotly_chart(fig, use_container_width=True)

# Card usage pie chart
st.subheader("💳 Card Type Distribution")
fig2 = px.pie(df, names="Card_Type", title="Card Type Usage Share")
st.plotly_chart(fig2, use_container_width=True)

# Anomaly Score Distribution
if "Anomaly_Score" in df.columns:
    st.subheader("📈 Anomaly Score Distribution")
    fig3 = px.histogram(df, x="Anomaly_Score", nbins=20, title="Distribution of Anomaly Scores", color="Anomaly")
    st.plotly_chart(fig3, use_container_width=True)

# Reason explorer
st.subheader("📌 Explanation & Suggested Actions")
if "Explanation" in filtered_df.columns and "Suggested_Action" in filtered_df.columns:
    for idx, row in filtered_df.iterrows():
        if row["Anomaly"]:
            st.markdown(f"**🆔 Transaction {row['Transaction ID']}**")
            st.markdown(f"- 📊 Anomaly Score: {row.get('Anomaly_Score', 'N/A')}")
            st.markdown(f"- 🧠 Explanation: {row['Explanation']}")
            st.markdown(f"- ✅ Suggested Action: {row['Suggested_Action']}\n")
