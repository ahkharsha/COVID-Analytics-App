import boto3
import json
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# 1. Page Configuration
st.set_page_config(page_title="Live COVID Stream", page_icon="🦠", layout="wide")

# 2. Auto-Refresh (Clicks refresh every 5 seconds)
st_autorefresh(interval=5000, key="data_refresh")

s3 = boto3.client('s3', region_name='ap-south-1')
BUCKET_NAME = 'covid-pipeline-data-week12' 

@st.cache_data(ttl=5)
def load_live_data():
    try:
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix='live_data/')
        all_records = []
        if 'Contents' in response:
            for item in response['Contents']:
                obj = s3.get_object(Bucket=BUCKET_NAME, Key=item['Key'])
                data = json.loads(obj['Body'].read().decode('utf-8'))
                all_records.append(data)
                
        if all_records:
            return pd.DataFrame(all_records)
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Connection Error: {e}")
        return pd.DataFrame()

# 3. Fetch Data
df = load_live_data()

# 4. Header
st.title("🔴 LIVE: Global COVID-19 Telemetry")
st.markdown("Streaming real-time data via **AWS Kinesis** & **AWS Lambda**")
st.divider()

if df.empty:
    st.warning("⏳ Waiting for live data stream... Make sure your EC2 producer script is running!")
else:
    # Clean the data into integers
    for col in ['Confirmed', 'Deaths', 'Recovered']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    # Sort strictly by the time Lambda processed it, so the newest row is always at the bottom
    if 'processed_at_utc' in df.columns:
        df['processed_at_utc'] = pd.to_datetime(df['processed_at_utc'])
        df = df.sort_values('processed_at_utc').reset_index(drop=True)

    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'])

    # Get the latest row for the metrics
    latest_row = df.iloc[-1]
    
    # Calculate the differences for the metric arrows
    if len(df) > 1:
        previous_row = df.iloc[-2]
        cases_delta = int(latest_row['Confirmed'] - previous_row['Confirmed'])
        deaths_delta = int(latest_row['Deaths'] - previous_row['Deaths'])
        recovered_delta = int(latest_row['Recovered'] - previous_row['Recovered'])
    else:
        cases_delta = deaths_delta = recovered_delta = 0

    # 5. Metrics Row
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(label="📡 Records Processed", value=f"{len(df):,}")
    with col2:
        st.metric(label="😷 Total Confirmed", value=f"{latest_row['Confirmed']:,}", delta=cases_delta)
    with col3:
        st.metric(label="☠️ Total Deaths", value=f"{latest_row['Deaths']:,}", delta=deaths_delta, delta_color="inverse")
    with col4:
        st.metric(label="🛡️ Total Recovered", value=f"{latest_row['Recovered']:,}", delta=recovered_delta)

    st.divider()

    # 6. Advanced Charts Layout
    chart_col1, chart_col2 = st.columns([2, 1])

    with chart_col1:
        st.subheader("📈 Infection Spread (Area)")
        if 'Date' in df.columns:
            st.area_chart(df.set_index('Date')['Confirmed'], color="#FF4B4B")
        
        st.subheader("⚖️ Deaths vs. Recoveries")
        if 'Date' in df.columns:
            comparison_df = df.set_index('Date')[['Recovered', 'Deaths']]
            st.bar_chart(comparison_df, color=["#00C853", "#FF4B4B"])

    with chart_col2:
        st.subheader("📡 Traffic by Source")
        if 'Source_Hospital' in df.columns:
            # Group by hospital and count the records
            hospital_counts = df['Source_Hospital'].value_counts()
            st.bar_chart(hospital_counts, color="#3498db")
        else:
            st.info("Waiting for multi-source data...")

        st.subheader("🏥 Live Datastream Log")
        display_cols = ['Source_Hospital', 'Date', 'Confirmed', 'Deaths', 'Recovered']
        actual_cols = [col for col in display_cols if col in df.columns]
        st.dataframe(
            df[actual_cols].tail(10).sort_index(ascending=False), 
            use_container_width=True,
            hide_index=True
        )
