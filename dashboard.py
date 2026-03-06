"""
Pipeline Monitoring Dashboard.

Streamlit dashboard showing:
- Pipeline run history and status
- Record counts and trends
- Data quality metrics
- Latest extracted data preview
"""
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import func
from models import init_db, get_session, PipelineRun, CleanedRecord, RawExtraction

# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="Data Pipeline Dashboard",
    page_icon="📊",
    layout="wide",
)

st.markdown("""
<style>
    .main-header {
        font-size: 2rem;
        font-weight: 700;
        color: #1a1a2e;
        margin-bottom: 0.3rem;
    }
    .status-running { color: #ffc107; font-weight: 600; }
    .status-completed { color: #28a745; font-weight: 600; }
    .status-failed { color: #dc3545; font-weight: 600; }
</style>
""", unsafe_allow_html=True)

init_db()

# ============================================================
# HEADER
# ============================================================
st.markdown('<p class="main-header">📊 Data Pipeline — Monitoring Dashboard</p>', unsafe_allow_html=True)
st.caption("Real-time view of extraction, transformation, and loading operations")

# ============================================================
# KPI METRICS
# ============================================================
session = get_session()

total_records = session.query(func.count(CleanedRecord.id)).scalar() or 0
total_runs = session.query(func.count(PipelineRun.id)).scalar() or 0
successful_runs = session.query(func.count(PipelineRun.id)).filter(
    PipelineRun.status == "completed"
).scalar() or 0
failed_runs = session.query(func.count(PipelineRun.id)).filter(
    PipelineRun.status == "failed"
).scalar() or 0

# Last run info
last_run = session.query(PipelineRun).order_by(PipelineRun.started_at.desc()).first()

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric("Total Records", f"{total_records:,}")
with col2:
    st.metric("Pipeline Runs", total_runs)
with col3:
    success_rate = (successful_runs / total_runs * 100) if total_runs > 0 else 0
    st.metric("Success Rate", f"{success_rate:.0f}%")
with col4:
    if last_run:
        st.metric("Last Run", last_run.status.upper())
    else:
        st.metric("Last Run", "N/A")
with col5:
    if last_run and last_run.duration_seconds:
        st.metric("Last Duration", f"{last_run.duration_seconds:.1f}s")
    else:
        st.metric("Last Duration", "N/A")

st.markdown("---")

# ============================================================
# PIPELINE RUN HISTORY
# ============================================================
st.markdown("### 📋 Pipeline Run History")

runs = session.query(PipelineRun).order_by(PipelineRun.started_at.desc()).limit(20).all()

if runs:
    run_data = []
    for run in runs:
        run_data.append({
            "Run ID": run.run_id,
            "Started": run.started_at.strftime("%Y-%m-%d %H:%M") if run.started_at else "—",
            "Status": run.status.upper(),
            "Extracted": run.records_extracted or 0,
            "Cleaned": run.records_cleaned or 0,
            "Failed": run.records_failed or 0,
            "Duration": f"{run.duration_seconds:.1f}s" if run.duration_seconds else "—",
        })

    df_runs = pd.DataFrame(run_data)
    st.dataframe(df_runs, use_container_width=True, hide_index=True)
else:
    st.info("No pipeline runs yet. Run `python pipeline.py` to start.")

# ============================================================
# DATA BY SOURCE
# ============================================================
st.markdown("---")
col_source, col_category = st.columns(2)

with col_source:
    st.markdown("### 📁 Records by Source")
    source_counts = session.query(
        CleanedRecord.source,
        func.count(CleanedRecord.id).label("count"),
    ).group_by(CleanedRecord.source).all()

    if source_counts:
        df_sources = pd.DataFrame(source_counts, columns=["Source", "Count"])
        st.bar_chart(df_sources.set_index("Source"))
    else:
        st.info("No data yet")

with col_category:
    st.markdown("### 🏷️ Records by Category")
    category_counts = session.query(
        CleanedRecord.category,
        func.count(CleanedRecord.id).label("count"),
    ).group_by(CleanedRecord.category).order_by(func.count(CleanedRecord.id).desc()).all()

    if category_counts:
        df_categories = pd.DataFrame(category_counts, columns=["Category", "Count"])
        st.bar_chart(df_categories.set_index("Category"))
    else:
        st.info("No data yet")

# ============================================================
# LATEST DATA PREVIEW
# ============================================================
st.markdown("---")
st.markdown("### 🔍 Latest Extracted Records")

latest_records = session.query(CleanedRecord).order_by(
    CleanedRecord.cleaned_at.desc()
).limit(50).all()

if latest_records:
    preview_data = []
    for record in latest_records:
        preview_data.append({
            "Source": record.source,
            "Title": (record.title or "")[:80],
            "Company": (record.company or "")[:40],
            "Location": record.location or "—",
            "Category": record.category or "—",
            "Salary": f"{record.currency}{record.salary_min:,.0f}-{record.salary_max:,.0f}"
                      if record.salary_min else "—",
            "Tags": ", ".join(record.tags[:3]) if record.tags else "—",
        })

    df_preview = pd.DataFrame(preview_data)
    st.dataframe(df_preview, use_container_width=True, hide_index=True)
else:
    st.info("No records yet. Run the pipeline first.")

# ============================================================
# INFRASTRUCTURE INFO
# ============================================================
st.markdown("---")
st.markdown("### 🏗️ Infrastructure")

col_inf1, col_inf2 = st.columns(2)

with col_inf1:
    st.markdown("""
    **Pipeline Architecture:**
    ```
    Sources (HN, GitHub, ...)
        ↓
    Extractor (anti-detection, retry logic)
        ↓
    Transformer (clean, normalize, deduplicate)
        ↓
    Database (SQLite / PostgreSQL)
        ↓
    Export (CSV, JSON)
        ↓
    Dashboard (this page)
    ```
    """)

with col_inf2:
    st.markdown("""
    **Technical Details:**
    - All data processed and stored locally
    - No external APIs for processing
    - Polite scraping with delays and rotation
    - Full audit trail of every pipeline run
    - Export to CSV and JSON for downstream use
    """)

session.close()
