"""
Data Transformer — ETL cleaning and normalization stage.

Takes raw extracted data and:
1. Validates required fields
2. Normalizes text (strip, lowercase where needed)
3. Extracts structured fields from unstructured text
4. Deduplicates records
5. Outputs clean, analysis-ready records
"""
import re
import logging
from datetime import datetime
from models import CleanedRecord

logger = logging.getLogger(__name__)


class DataTransformer:
    """
    Cleans and normalizes raw extraction data into structured records.
    """

    def __init__(self):
        self.total_cleaned = 0
        self.total_skipped = 0
        self.total_errors = 0
        self.seen_ids = set()

    def _validate_record(self, raw: dict) -> bool:
        """Check if a raw record has minimum required fields."""
        required = ["id", "source"]
        for field in required:
            if not raw.get(field):
                return False

        # Must have at least a title or description
        if not raw.get("title") and not raw.get("description") and not raw.get("company"):
            return False

        return True

    def _clean_text(self, text: str | None) -> str:
        """Normalize text: strip whitespace, remove excess spaces."""
        if not text:
            return ""
        # Remove HTML artifacts
        text = re.sub(r'<[^>]+>', ' ', text)
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def _extract_salary(self, text: str) -> tuple:
        """Extract salary range from text."""
        if not text:
            return None, None, None

        text_lower = text.lower()

        # Match patterns like "$100k-$150k", "$100,000 - $150,000", "€80k"
        patterns = [
            r'[\$€£](\d{2,3})[kK]\s*[-–to]+\s*[\$€£]?(\d{2,3})[kK]',
            r'[\$€£]([\d,]+)\s*[-–to]+\s*[\$€£]?([\d,]+)',
            r'[\$€£](\d{2,3})[kK]',
        ]

        currency = "$"
        if "€" in text:
            currency = "€"
        elif "£" in text:
            currency = "£"

        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                groups = match.groups()
                if len(groups) == 2:
                    min_val = float(groups[0].replace(",", ""))
                    max_val = float(groups[1].replace(",", ""))
                    # If values are small, they're in K
                    if min_val < 1000:
                        min_val *= 1000
                    if max_val < 1000:
                        max_val *= 1000
                    return min_val, max_val, currency
                elif len(groups) == 1:
                    val = float(groups[0].replace(",", ""))
                    if val < 1000:
                        val *= 1000
                    return val, val, currency

        return None, None, None

    def _extract_location(self, raw: dict) -> str:
        """Extract and normalize location."""
        location = raw.get("location", "")
        if location:
            return self._clean_text(location)

        # Try to find location in description
        desc = raw.get("description", "").lower()
        if "remote" in desc:
            return "Remote"
        if "hybrid" in desc:
            return "Hybrid"
        if "onsite" in desc or "on-site" in desc:
            return "Onsite"

        return "Not specified"

    def _extract_tags(self, raw: dict) -> list:
        """Extract relevant tags/skills from the record."""
        if raw.get("tags"):
            return raw["tags"][:10]

        text = f"{raw.get('title', '')} {raw.get('description', '')}".lower()
        tag_keywords = [
            "python", "javascript", "typescript", "react", "node",
            "llm", "ai", "machine learning", "data", "etl",
            "docker", "kubernetes", "aws", "gcp", "azure",
            "remote", "full-time", "contract", "senior", "junior",
            "langchain", "rag", "ollama", "pytorch", "tensorflow",
        ]

        found_tags = [tag for tag in tag_keywords if tag in text]
        return found_tags[:10]

    def _categorize(self, raw: dict) -> str:
        """Categorize the record based on content."""
        if raw.get("category"):
            return raw["category"]

        text = f"{raw.get('title', '')} {raw.get('description', '')}".lower()

        categories = {
            "AI/ML Engineering": ["llm", "machine learning", "ai engineer", "ml ", "nlp"],
            "Data Engineering": ["etl", "data pipeline", "data engineer", "airflow"],
            "Backend Development": ["backend", "api", "python developer", "django", "fastapi"],
            "Full Stack": ["full stack", "fullstack", "react", "frontend"],
            "DevOps/Infrastructure": ["devops", "kubernetes", "docker", "infrastructure"],
            "Data Science": ["data scientist", "analytics", "statistics"],
        }

        for category, keywords in categories.items():
            if any(kw in text for kw in keywords):
                return category

        return "General"

    def transform(self, raw_records: list) -> list:
        """
        Transform a batch of raw records into cleaned records.

        Args:
            raw_records: List of raw extraction dicts

        Returns:
            List of CleanedRecord objects ready for database insertion
        """
        cleaned = []

        for raw in raw_records:
            try:
                # Validate
                if not self._validate_record(raw):
                    self.total_skipped += 1
                    continue

                # Deduplicate
                record_id = raw.get("id", "")
                if record_id in self.seen_ids:
                    self.total_skipped += 1
                    continue
                self.seen_ids.add(record_id)

                # Extract salary
                text_for_salary = f"{raw.get('title', '')} {raw.get('description', '')}"
                salary_min, salary_max, currency = self._extract_salary(text_for_salary)

                # Build cleaned record
                record = CleanedRecord(
                    source=raw.get("source", "unknown"),
                    title=self._clean_text(raw.get("title") or raw.get("company", "")),
                    company=self._clean_text(raw.get("company", "")),
                    location=self._extract_location(raw),
                    category=self._categorize(raw),
                    description=self._clean_text(raw.get("description", ""))[:2000],
                    url=raw.get("source_url", ""),
                    salary_min=salary_min,
                    salary_max=salary_max,
                    currency=currency,
                    tags=self._extract_tags(raw),
                    raw_extraction_id=None,
                    is_valid=True,
                )

                cleaned.append(record)
                self.total_cleaned += 1

            except Exception as e:
                logger.error(f"Transform error for record {raw.get('id', '?')}: {e}")
                self.total_errors += 1

        logger.info(
            f"Transform complete: {self.total_cleaned} cleaned, "
            f"{self.total_skipped} skipped, {self.total_errors} errors"
        )
        return cleaned
