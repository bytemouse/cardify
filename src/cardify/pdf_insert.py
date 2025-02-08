import hashlib
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Union

import fitz

from .database import PDFAnkiDB

logger = logging.getLogger(__name__)  # Get the logger for this module


class PDFInsertError(Exception):
    """Custom exception for PDF insertion errors"""

    pass


def get_date_from_metadata(metadata: dict) -> str:
    """
    Extract and format a date from PDF metadata.
    Checks multiple date fields and offers user selection.
    """
    logger.debug("Extracting date from metadata")
    date_fields = {
        "creationDate": "Creation Date",
        "modDate": "Modification Date",
        "publicationDate": "Publication Date",
    }

    # Collect all available dates
    available_dates = {}
    for field, display_name in date_fields.items():
        if field in metadata and metadata[field]:
            logger.debug(f"Found date field: {field}")
            date_str = metadata[field]
            if date_str.startswith("D:"):
                date_str = date_str[2:]
            try:
                if len(date_str) >= 8:  # At least YYYYMMDD
                    year = date_str[:4]
                    month = date_str[4:6]
                    day = date_str[6:8]
                    formatted_date = f"{year}-{month}-{day}"
                    datetime.strptime(formatted_date, "%Y-%m-%d")
                    available_dates[field] = (formatted_date, display_name)
                    logger.debug(f"Parsed date: {formatted_date} from {field}")
            except ValueError:
                logger.debug(f"Failed to parse date from {field}: {date_str}")
                continue

    if not available_dates:
        logger.info("No valid dates found in metadata")
        return ""

    if len(available_dates) == 1:
        date = list(available_dates.values())[0][0]
        logger.info(f"Using single available date: {date}")
        return date

    # If multiple dates are available, let user choose
    print("\nMultiple dates found in the PDF:")
    print("0. Enter a custom date")
    for i, (field, (date, name)) in enumerate(available_dates.items(), 1):
        print(f"{i}. {name}: {date}")

    while True:
        try:
            choice = input(f"Please select a date option (0-{len(available_dates)}): ")
            choice = int(choice)
            if choice == 0:
                logger.debug("User chose to enter custom date")
                return ""
            elif 1 <= choice <= len(available_dates):
                selected_date = list(available_dates.values())[choice - 1][0]
                logger.debug(f"User selected date: {selected_date}")
                return selected_date
            else:
                logger.warning(f"Invalid choice entered: {choice}")
                print("Invalid choice. Please try again.")
        except ValueError:
            logger.warning("Non-numeric choice entered")
            print("Please enter a valid number.")


def extract_pdf_metadata(pdf_path: Path) -> Dict[str, Union[str, int]]:
    """
    Extract metadata from a PDF file using PyMuPDF.
    If metadata is missing, prompts user for input.
    """
    logger.debug(f"Extracting metadata from: {pdf_path}")
    doc = fitz.open(pdf_path)

    # Extract initial metadata
    metadata = doc.metadata if doc.metadata is not None else {}
    page_count = doc.page_count
    doc.close()
    logger.debug(f"Raw metadata: {metadata}")
    logger.debug(f"Page count: {page_count}")

    # Define required metadata fields
    required_fields = {
        "title": ("Title", None),
        "author": ("Author", None),
        "publication_date": (
            "Publication Date (YYYY-MM-DD)",
            get_date_from_metadata(metadata),
        ),
    }

    # Prepare final metadata dict
    final_metadata = {}

    # Check each required field
    for field, (display_name, default_value) in required_fields.items():
        value = metadata.get(field, default_value)
        if not value or value.strip() == "":
            logger.info(f"Missing metadata field: {field}")
            print(f"\nMetadata missing: {display_name}")
            print(f"PDF file: {pdf_path.name}")
            value = input(f"Please enter {display_name}: ").strip()

            # Validate publication date format if necessary
            if field == "publication_date":
                while True:
                    try:
                        datetime.strptime(value, "%Y-%m-%d")
                        break
                    except ValueError:
                        logger.warning(f"Invalid date format entered: {value}")
                        print("Invalid date format. Please use YYYY-MM-DD")
                        value = input(f"Please enter {display_name}: ").strip()

        final_metadata[field] = value
        logger.debug(f"Set metadata {field}: {value}")

    # Add page count
    final_metadata["page_count"] = page_count

    logger.debug("Metadata extraction completed")
    return final_metadata


def calculate_md5(file_path: Path) -> str:
    """Calculate MD5 hash of a file."""
    logger.debug(f"Calculating MD5 hash for: {file_path}")
    try:
        md5_hash = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                md5_hash.update(chunk)
        hash_value = md5_hash.hexdigest()
        logger.debug(f"MD5 hash: {hash_value}")
        return hash_value
    except Exception as e:
        logger.error(f"Failed to calculate MD5 hash: {e}")
        raise PDFInsertError(f"Failed to calculate MD5 hash: {str(e)}")


def insert_pdf(
    file_path: Union[str, Path],
    db_path: str = "pdf_anki.db",
    custom_metadata: Optional[Dict[str, Union[str, int]]] = None,
) -> int:
    """Insert a PDF file into the database with metadata."""
    logger.debug(f"Starting PDF insertion: {file_path}")
    try:
        # Convert string path to Path object
        pdf_path = Path(file_path)

        # Validate file exists and is PDF
        if not pdf_path.exists():
            logger.error(f"File not found: {pdf_path}")
            raise PDFInsertError(f"File not found: {pdf_path}")
        if pdf_path.suffix.lower() != ".pdf":
            logger.error(f"Not a PDF file: {pdf_path}")
            raise PDFInsertError(f"Not a PDF file: {pdf_path}")

        # Extract metadata and calculate hash
        metadata = extract_pdf_metadata(pdf_path)
        md5_hash = calculate_md5(pdf_path)

        # Update metadata with custom values if provided
        if custom_metadata:
            logger.debug(f"Updating with custom metadata: {custom_metadata}")
            metadata.update(custom_metadata)

        # Add MD5 hash to metadata
        metadata["md5_hash"] = md5_hash

        # Initialize database connection
        logger.debug(f"Connecting to database: {db_path}")
        db = PDFAnkiDB(db_path)

        try:
            # Insert PDF and return ID
            pdf_id = db.add_pdf(str(pdf_path), metadata)
            logger.info(f"Successfully inserted PDF with ID: {pdf_id}")
            return pdf_id

        finally:
            db.close()
            logger.debug("Database connection closed")

    except sqlite3.Error as e:
        logger.error(f"Database error during PDF insertion: {e}")
        raise PDFInsertError(f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error during PDF insertion: {e}")
        raise PDFInsertError(f"Failed to insert PDF: {str(e)}")
