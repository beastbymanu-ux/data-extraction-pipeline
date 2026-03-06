"""
Main Pipeline Orchestrator.

Runs the full ETL cycle:
  Extract → Transform → Load → Export

Each run is tracked with metadata (duration, record counts, errors).
Can run as one-shot or scheduled.
"""
import uuid
import logging
import time
import json
import csv
from datetime import datetime
from pathlib import Path

from models import init_db, get_session, PipelineRun, RawExtraction
from extractor import HackerNewsExtractor, GitHubJobsExtractor
from transformer import DataTransformer
from config import OUTPUT_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


class Pipeline:
    """
    ETL Pipeline orchestrator.

    Runs extraction, transformation, and loading in sequence.
    Tracks each run with metadata for monitoring.
    """

    def __init__(self):
        self.extractors = [
            HackerNewsExtractor(),
            GitHubJobsExtractor(),
        ]
        self.transformer = DataTransformer()
        init_db()
        Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    def run(self) -> dict:
        """
        Execute a full pipeline run.

        Returns:
            dict with run metadata (records processed, duration, etc.)
        """
        run_id = f"run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        start_time = time.time()

        logger.info("=" * 60)
        logger.info(f"PIPELINE RUN: {run_id}")
        logger.info("=" * 60)

        session = get_session()

        # Create pipeline run record
        pipeline_run = PipelineRun(
            run_id=run_id,
            status="running",
        )
        session.add(pipeline_run)
        session.commit()

        try:
            # ============================================
            # STAGE 1: EXTRACT
            # ============================================
            logger.info("[STAGE 1] Extracting data from sources...")
            all_raw = []

            for extractor in self.extractors:
                logger.info(f"  Running extractor: {extractor.source_name}")
                raw_records = extractor.extract()

                # Save raw extractions to database
                for raw in raw_records:
                    raw_entry = RawExtraction(
                        source=extractor.source_name,
                        source_url=raw.get("source_url", ""),
                        raw_data=raw,
                        batch_id=run_id,
                        status="pending",
                    )
                    session.add(raw_entry)

                all_raw.extend(raw_records)
                logger.info(f"  {extractor.source_name}: {len(raw_records)} records")

            session.commit()
            logger.info(f"[STAGE 1] Total extracted: {len(all_raw)} records")

            # ============================================
            # STAGE 2: TRANSFORM
            # ============================================
            logger.info("[STAGE 2] Cleaning and normalizing data...")
            cleaned_records = self.transformer.transform(all_raw)
            logger.info(f"[STAGE 2] Total cleaned: {len(cleaned_records)} records")

            # ============================================
            # STAGE 3: LOAD
            # ============================================
            logger.info("[STAGE 3] Loading into database...")
            for record in cleaned_records:
                session.add(record)
            session.commit()
            logger.info(f"[STAGE 3] Loaded {len(cleaned_records)} records into database")

            # ============================================
            # STAGE 4: EXPORT
            # ============================================
            logger.info("[STAGE 4] Exporting to files...")
            self._export_csv(cleaned_records, run_id)
            self._export_json(cleaned_records, run_id)

            # ============================================
            # COMPLETE
            # ============================================
            duration = time.time() - start_time

            pipeline_run.status = "completed"
            pipeline_run.completed_at = datetime.utcnow()
            pipeline_run.records_extracted = len(all_raw)
            pipeline_run.records_cleaned = len(cleaned_records)
            pipeline_run.records_failed = self.transformer.total_errors
            pipeline_run.duration_seconds = duration
            session.commit()

            result = {
                "run_id": run_id,
                "status": "completed",
                "records_extracted": len(all_raw),
                "records_cleaned": len(cleaned_records),
                "records_failed": self.transformer.total_errors,
                "duration_seconds": round(duration, 2),
            }

            logger.info("=" * 60)
            logger.info(f"PIPELINE COMPLETE: {run_id}")
            logger.info(f"  Extracted: {result['records_extracted']}")
            logger.info(f"  Cleaned: {result['records_cleaned']}")
            logger.info(f"  Failed: {result['records_failed']}")
            logger.info(f"  Duration: {result['duration_seconds']}s")
            logger.info("=" * 60)

            return result

        except Exception as e:
            duration = time.time() - start_time
            pipeline_run.status = "failed"
            pipeline_run.completed_at = datetime.utcnow()
            pipeline_run.error_message = str(e)
            pipeline_run.duration_seconds = duration
            session.commit()

            logger.error(f"PIPELINE FAILED: {e}")
            return {
                "run_id": run_id,
                "status": "failed",
                "error": str(e),
                "duration_seconds": round(duration, 2),
            }

        finally:
            session.close()

    def _export_csv(self, records: list, run_id: str):
        """Export cleaned records to CSV."""
        filepath = Path(OUTPUT_DIR) / f"{run_id}.csv"

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "source", "title", "company", "location", "category",
                "salary_min", "salary_max", "currency", "url", "tags",
            ])
            for record in records:
                writer.writerow([
                    record.source, record.title, record.company,
                    record.location, record.category,
                    record.salary_min, record.salary_max, record.currency,
                    record.url, "|".join(record.tags) if record.tags else "",
                ])

        logger.info(f"Exported CSV: {filepath}")

    def _export_json(self, records: list, run_id: str):
        """Export cleaned records to JSON."""
        filepath = Path(OUTPUT_DIR) / f"{run_id}.json"

        data = []
        for record in records:
            data.append({
                "source": record.source,
                "title": record.title,
                "company": record.company,
                "location": record.location,
                "category": record.category,
                "description": record.description[:500],
                "salary_min": record.salary_min,
                "salary_max": record.salary_max,
                "currency": record.currency,
                "url": record.url,
                "tags": record.tags,
            })

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info(f"Exported JSON: {filepath}")


if __name__ == "__main__":
    pipeline = Pipeline()
    result = pipeline.run()
    print(f"\nResult: {json.dumps(result, indent=2)}")
