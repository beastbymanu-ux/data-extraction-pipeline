"""
Data Pipeline Configuration.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# Database
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///pipeline_data.db")

# Scraper settings
SCRAPER_USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
]
REQUEST_DELAY_MIN = 1.5  # seconds
REQUEST_DELAY_MAX = 3.5  # seconds
MAX_RETRIES = 3

# Pipeline settings
BATCH_SIZE = 100
OUTPUT_DIR = "./output"

# Dashboard
DASHBOARD_PORT = 8503
