import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import os
import subprocess
import sys
import time

# --- Page Configuration ---
st.set_page_config(page_title="COVID-19 Live Dashboard", layout="wide", initial_sidebar_state="expanded")

# --- Custom Styling ---
st.markdown("""
<style>
    .reportview-container { background: #f0f2f6; }
    .metric-card {
        background-color: #ffffff;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)

OUTPUT_DIR = "pipeline_output"

# --- Data Loading (Cached for performance) ---
@st.cache_data
def load_data():
    try:
        data = {
            "top_cases": pd.read_csv(os.path.join(OUTPUT_DIR, "top_cases.csv")),
            "top_deaths": pd.read_csv(os.path.join(OUTPUT_DIR, "top_deaths.csv")),
            "who_summary": pd.read_csv(os.path.join(OUTPUT_DIR, "who_summary.csv")),
            "daily": pd.read_csv(os.path.join(OUTPUT_DIR, "daily.csv")),
            "death_growth": pd.read_csv(os.path.join(OUTPUT_DIR, "death_growth.csv")),
            "monthly": pd.read_csv(os.path.join(OUTPUT_DIR, "monthly_growth.csv")),
            "top_5_region": pd.read_csv(os.path.join(OUTPUT_DIR, "top_5_region.csv")),
            "usa_trend": pd.read_csv(os.path.join(OUTPUT_DIR, "usa_trend.csv")),
            "infection_rates": pd.read_csv(os.path.join(OUTPUT_DIR, "infection_rates.csv")),
            "usa_states": pd.read_csv(os.path.join(OUTPUT_DIR, "usa_states.csv")),
            "geo_data": pd.read_csv(os.path.join(OUTPUT_DIR, "geo_data.csv")),
            "best_rec": pd.read_csv(os.path.join(OUTPUT_DIR, "best_rec.csv")),
            "worst_rec": pd.read_csv(os.path.join(OUTPUT_DIR, "worst_rec.csv")),
            "peak_dates": pd.read_csv(os.path.join(OUTPUT_DIR, "peak_dates.csv")),
            "severity_counts": pd.read_csv(os.path.join(OUTPUT_DIR, "severity_counts.csv"))
        }
        return data
    except Exception as e:
        return None

dfs = load_data()

# --- Sidebar ---
with st.sidebar:
    st.title("Navigation")
    st.info("This dashboard displays the latest processed COVID-19 data.")
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

if not dfs:
    st.error("Data not found. Please run the orchestrator.py script first to process the data.")
    st.stop()

# --- Header KPIs ---
st.title("COVID-19 Analytics Dashboard")
st.markdown("---")

kpi1, kpi2, kpi3, kpi4 = st.columns(4)
total_cases = dfs["who_summary"]["Total_Cases"].sum()
total_deaths = dfs["who_summary"]["Total_Deaths"].sum()
total_rec = dfs["who_summary"]["Total_Recovered"].sum()
recovery_rate = (total_rec / total_cases) * 100

kpi1.metric(label="Total Global Cases", value=f"{total_cases:,.0f}")
kpi2.metric(label="Total Global Deaths", value=f"{total_deaths:,.0f}")
kpi3.metric(label="Total Recoveries", value=f"{total_rec:,.0f}")
kpi4.metric(label="Global Recovery Rate", value=f"{recovery_rate:.1f}%")

st.markdown("---")

# --- Dashboard Tabs ---
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🌍 Global Overview", 
    "📈 Time-Series Trends", 
    "🗺️ Geo & Infection Analysis", 
    "🏥 Recovery & Peaks",
    "⚙️ Admin: Orchestration"
])

with tab1:
    st.header("Global Aggregations")
    colA, colB, colC = st.columns(3)
    with colA:
        st.subheader("Top 10 Confirmed Cases")
        st.bar_chart(dfs["top_cases"].set_index("Country/Region")["Confirmed"], use_container_width=True)
    with colB:
        st.subheader("Top 10 Death Rates")
        fig, ax = plt.subplots(figsize=(5,3))
        ax.barh(dfs["top_deaths"]["Country/Region"], dfs["top_deaths"]["Deaths / 100 Cases"], color="#FF4B4B")
        st.pyplot(fig)
    with colC:
        st.subheader("Cases by WHO Region")
        fig, ax = plt.subplots(figsize=(5,3))
        ax.pie(dfs["who_summary"]["Total_Cases"], labels=dfs["who_summary"]["WHO Region"], autopct='%1.1f%%')
        st.pyplot(fig)

with tab2:
    st.header("Timelines")
    colA, colB = st.columns(2)
    with colA:
        st.subheader("Daily Global New Cases")
        daily_df = dfs["daily"].copy()
        daily_df["Date"] = pd.to_datetime(daily_df["Date"])
        st.line_chart(daily_df.set_index("Date")["New cases"], use_container_width=True)
    with colB:
        st.subheader("USA Daily Case Increase")
        usa_df = dfs["usa_trend"].copy()
        usa_df["Date"] = pd.to_datetime(usa_df["Date"])
        st.line_chart(usa_df.set_index("Date")["Daily_Increase"], use_container_width=True)

with tab3:
    st.header("Geographic and Severity Breakdown")
    colA, colB = st.columns(2)
    with colA:
        st.subheader("Top 20 Countries by Infection Rate (%)")
        st.bar_chart(dfs["infection_rates"].set_index("Country/Region")["Infection_Rate"], use_container_width=True)
    with colB:
        st.subheader("Global Severity Categories")
        fig, ax = plt.subplots(figsize=(5,3))
        colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
        ax.pie(dfs["severity_counts"]["count"], labels=dfs["severity_counts"]["Severity_Category"], autopct='%1.1f%%', colors=colors)
        st.pyplot(fig)

with tab4:
    st.header("Recovery Performance")
    colA, colB = st.columns(2)
    with colA:
        st.subheader("Top 10 Best Recovery Rates")
        st.bar_chart(dfs["best_rec"].set_index("Country/Region")["Rec_Rate"], color="#2ca02c")
    with colB:
        st.subheader("Top 10 Worst Recovery Rates")
        st.bar_chart(dfs["worst_rec"].set_index("Country/Region")["Rec_Rate"], color="#d62728")

with tab5:
    st.header("⚙️ Pipeline Orchestration & Admin")
    st.markdown("Trigger the PySpark backend directly from the UI and view the execution logs.")

    def render_last_run(run_info):
        with st.expander(run_info["label"], expanded=True):
            st.markdown("""
            <div style='background-color:#1e1e1e; padding:15px; border-radius:5px; font-family:monospace; color:#00ff00;'>
            > Waiting for manual trigger or scheduled execution... <br>
            > Background orchestrator daemon is decoupled.<br>
            > Output directory: <code>/pipeline_output/</code>
            </div>
            """, unsafe_allow_html=True)
            st.write("Execution log")
            if run_info["stdout"]:
                st.code(run_info["stdout"], language="shell")
            if run_info["stderr"]:
                st.code(run_info["stderr"], language="shell")
    
    col1, col2 = st.columns([1, 3])
    with col1:
        st.info("System Status: **Online**")
        run_button = st.button("🚀 Force Run Pipeline", use_container_width=True, type="primary")
    
    if "admin_last_run" not in st.session_state:
        st.session_state.admin_last_run = None

    with col2:
        if run_button:
            with st.status("🚀 Executing PySpark Pipeline", expanded=True) as status:
                st.write("• Launching Spark job from the local Python environment")
                st.write("• Initializing Spark Session and loading source CSV files")
                st.write("• Running joins, aggregations, window functions, and exports")
                start_time = time.time()
                
                # Execute the PySpark script
                process = subprocess.run(
                    [sys.executable, "scripts/process_covid_data.py"],
                    capture_output=True,
                    text=True
                )
                
                duration = time.time() - start_time
                if process.returncode == 0:
                    st.session_state.admin_last_run = {
                        "state": "success",
                        "label": f"✅ Pipeline Completed Successfully in {duration:.1f}s!",
                        "stdout": process.stdout,
                        "stderr": process.stderr,
                        "duration": duration,
                    }
                    status.update(label="✅ Pipeline completed successfully", state="complete", expanded=True)
                    render_last_run(st.session_state.admin_last_run)
                else:
                    st.session_state.admin_last_run = {
                        "state": "error",
                        "label": f"❌ Pipeline Failed after {duration:.1f}s",
                        "stdout": process.stdout,
                        "stderr": process.stderr,
                        "duration": duration,
                    }
                    status.update(label="❌ Pipeline failed", state="error", expanded=True)
                    render_last_run(st.session_state.admin_last_run)
        else:
            if st.session_state.admin_last_run:
                render_last_run(st.session_state.admin_last_run)
            else:
                st.markdown("""
                <div style='background-color:#1e1e1e; padding:15px; border-radius:5px; font-family:monospace; color:#00ff00;'>
                > Waiting for manual trigger or scheduled execution... <br>
                > Background orchestrator daemon is decoupled.<br>
                > Output directory: <code>/pipeline_output/</code>
                </div>
                """, unsafe_allow_html=True)