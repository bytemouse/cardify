import hashlib
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Union

import fitz

from .database import PDFAnkiDB


class PDFInsertError(Exception):
    """Custom exception for PDF insertion errors"""

    pass


def get_date_from_metadata(metadata: dict) -> str:
    """
    Extract and format a date from PDF metadata.
    Checks multiple date fields and offers user selection.
    """
    date_fields = {
        "creationDate": "Creation Date",
        "modDate": "Modification Date",
        "publicationDate": "Publication Date",
    }

    # Collect all available dates
    available_dates = {}
    for field, display_name in date_fields.items():
        if field in metadata and metadata[field]:
            # PDF dates are often in format "D:20240208101010Z"
            # Strip the "D:" prefix if it exists
            date_str = metadata[field]
            if date_str.startswith("D:"):
                date_str = date_str[2:]
            try:
                # Try to parse the date string (handle various formats)
                if len(date_str) >= 8:  # At least YYYYMMDD
                    year = date_str[:4]
                    month = date_str[4:6]
                    day = date_str[6:8]
                    formatted_date = f"{year}-{month}-{day}"
                    # Validate the date
                    datetime.strptime(formatted_date, "%Y-%m-%d")
                    available_dates[field] = (formatted_date, display_name)
            except ValueError:
                continue

    if not available_dates:
        return ""

    if len(available_dates) == 1:
        # If only one date is available, use it
        return list(available_dates.values())[0][0]

    # If multiple dates are available, let user choose
    print("\nMultiple dates found in the PDF:")
    print("0. Enter a custom date")
    for i, (field, (date, name)) in enumerate(available_dates.items(), 1):
        print(f"{i}. {name}: {date}")

    while True:
        try:
            choice = input(
                "Please select a date option (0-{}): ".format(len(available_dates))
            )
            choice = int(choice)
            if choice == 0:
                return ""  # Will trigger manual date entry
            elif 1 <= choice <= len(available_dates):
                return list(available_dates.values())[choice - 1][0]
            else:
                print("Invalid choice. Please try again.")
        except ValueError:
            print("Please enter a valid number.")


def extract_pdf_metadata(pdf_path: Path) -> Dict[str, Union[str, int]]:
    """
    Extract metadata from a PDF file using PyMuPDF.
    If metadata is missing, prompts user for input..

    Args:
        pdf_path: Path to the PDF file

    Returns:
        Dictionary containing PDF metadata

    Raises:
        PDFInsertError: If metadata extraction fails
    """
    doc = fitz.open(pdf_path)

    # Extract initial metadata
    metadata = doc.metadata if doc.metadata is not None else {}
    page_count = doc.page_count
    doc.close()

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
            # Prompt user for missing metadata
            print(f"\nMetadata missing: {display_name}")
            print(f"PDF file: {pdf_path.name}")
            value = input(f"Please enter {display_name}: ").strip()

            # Validate publication date format if necessary
            if field == "publication_date":
                while True:
                    try:
                        # Try to parse the date to validate format
                        datetime.strptime(value, "%Y-%m-%d")
                        break
                    except ValueError:
                        print("Invalid date format. Please use YYYY-MM-DD")
                        value = input(f"Please enter {display_name}: ").strip()

        final_metadata[field] = value

    # Add page count
    final_metadata["page_count"] = page_count

    return final_metadata


def calculate_md5(file_path: Path) -> str:
    """
    Calculate MD5 hash of a file.

    Args:
        file_path: Path to the file

    Returns:
        MD5 hash string

    Raises:
        PDFInsertError: If hash calculation fails
    """
    try:
        md5_hash = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                md5_hash.update(chunk)
        return md5_hash.hexdigest()
    except Exception as e:
        raise PDFInsertError(f"Failed to calculate MD5 hash: {str(e)}")


def insert_pdf(
    file_path: Union[str, Path],
    db_path: str = "pdf_anki.sqlite3",
    custom_metadata: Optional[Dict[str, Union[str, int]]] = None,
) -> int:
    """
    Insert a PDF file into the database with metadata.

    Args:
        file_path: Path to the PDF file
        db_path: Path to the SQLite database
        custom_metadata: Optional custom metadata to override extracted metadata

    Returns:
        ID of the inserted PDF record

    Raises:
        PDFInsertError: If PDF insertion fails
    """
    try:
        # Convert string path to Path object
        pdf_path = Path(file_path)

        # Validate file exists and is PDF
        if not pdf_path.exists():
            raise PDFInsertError(f"File not found: {pdf_path}")
        if pdf_path.suffix.lower() != ".pdf":
            raise PDFInsertError(f"Not a PDF file: {pdf_path}")

        # Extract metadata and calculate hash
        metadata = extract_pdf_metadata(pdf_path)
        md5_hash = calculate_md5(pdf_path)

        # Update metadata with custom values if provided
        if custom_metadata:
            metadata.update(custom_metadata)

        # Add MD5 hash to metadata
        metadata["md5_hash"] = md5_hash

        # Initialize database connection
        db = PDFAnkiDB(db_path)

        try:
            # Insert PDF and return ID
            pdf_id = db.add_pdf(str(pdf_path), metadata)
            return pdf_id

        finally:
            db.close()

    except sqlite3.Error as e:
        raise PDFInsertError(f"Database error: {str(e)}")
    except Exception as e:
        raise PDFInsertError(f"Failed to insert PDF: {str(e)}")


# Example usage:
if __name__ == "__main__":
    try:
        # Insert a PDF with default metadata
        pdf_id = insert_pdf("../../fundamentals.pdf")
        print(f"Inserted PDF with ID: {pdf_id}")

    except PDFInsertError as e:
        print(f"Error inserting PDF: {e}")
