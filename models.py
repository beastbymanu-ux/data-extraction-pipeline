"""
Database models for the data pipeline.
Stores raw extractions, cleaned data, and pipeline run metadata.
"""
from datetime import datetime
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, DateTime,
    Text, Boolean, JSON,
)
from sqlalchemy.orm import declarative_base, sessionmaker
from config import DATABASE_URL

Base = declarative_base()
engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)


class RawExtraction(Base):
    """Raw data as scraped from source — before cleaning."""
    __tablename__ = "raw_extractions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String(100), nullable=False)
    source_url = Column(String(500))
    raw_data = Column(JSON, nullable=False)
    extracted_at = Column(DateTime, default=datetime.utcnow)
    batch_id = Column(String(50))
    status = Column(String(20), default="pending")  # pending, cleaned, error


class CleanedRecord(Base):
    """Normalized, validated data ready for analysis."""
    __tablename__ = "cleaned_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String(100), nullable=False)
    title = Column(String(500))
    company = Column(String(200))
    location = Column(String(200))
    category = Column(String(100))
    description = Column(Text)
    url = Column(String(500))
    salary_min = Column(Float)
    salary_max = Column(Float)
    currency = Column(String(10))
    tags = Column(JSON)
    raw_extraction_id = Column(Integer)
    cleaned_at = Column(DateTime, default=datetime.utcnow)
    is_valid = Column(Boolean, default=True)


class PipelineRun(Base):
    """Metadata for each pipeline execution."""
    __tablename__ = "pipeline_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(50), unique=True, nullable=False)
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime)
    status = Column(String(20), default="running")  # running, completed, failed
    records_extracted = Column(Integer, default=0)
    records_cleaned = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)
    source = Column(String(100))
    error_message = Column(Text)
    duration_seconds = Column(Float)


def init_db():
    """Create all tables."""
    Base.metadata.create_all(engine)


def get_session():
    """Get a database session."""
    return SessionLocal()
