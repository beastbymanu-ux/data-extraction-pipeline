# Data Extraction Pipeline

> Own project by Manu (OpenClaw Labs). Reusable ETL pipeline for public web data.

## What it does

- **Extract** (`extractor.py`): multi-source scraper with polite random delays, User-Agent rotation and retries with exponential backoff. Subclass `DataExtractor` for each new source.
- **Transform** (`transformer.py`): cleans and validates every record before it's stored.
- **Load** (`models.py`, `pipeline.py`): SQLAlchemy keeps raw extractions, clean records and a log of every pipeline run, processed in batches.
- **Monitor** (`dashboard.py`): Streamlit dashboard with run history, record counts, data quality and a preview of the latest data.

## Stack

Python · requests + BeautifulSoup · SQLAlchemy (SQLite by default) · Streamlit · Docker

## Quick start

```bash
git clone https://github.com/beastbymanu-ux/data-extraction-pipeline
cd data-extraction-pipeline
cp .env.example .env
pip install -r requirements.txt
python pipeline.py
streamlit run dashboard.py
```

Or with Docker: `docker build -t data-pipeline . && docker run -p 8501:8501 data-pipeline`

---
*Built by Manu · OpenClaw Labs (my own AI lab)*
