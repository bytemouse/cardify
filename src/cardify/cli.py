import argparse
import logging
import sys
from pathlib import Path
from typing import Optional, Tuple

from .chunking import process_markdown_to_chunks
from .database import PDFAnkiDB
from .logging_config import setup_logger
from .pdf_insert import PDFInsertError, insert_pdf


def find_markdown_file(pdf_path: Path) -> Optional[Path]:
    """
    Look for a markdown file with the same name as the PDF.
    Checks both .md and .markdown extensions.
    """
    for ext in [".md", ".markdown"]:
        md_path = pdf_path.with_suffix(ext)
        if md_path.exists():
            return md_path
    return None


def process_files(
    pdf_path: Path, markdown_path: Optional[Path] = None, db_path: str = "pdf_anki.db"
) -> Tuple[int, Path]:
    """
    Process PDF and its corresponding markdown file.

    Args:
        pdf_path: Path to the PDF file
        markdown_path: Optional specific path to markdown file
        db_path: Optional path to the database file

    Returns:
        Tuple of (pdf_id, markdown_path used)

    Raises:
        PDFInsertError: If PDF insertion fails
        FileNotFoundError: If required markdown file is not found
    """
    # Find markdown file if not specified
    if not markdown_path:
        markdown_path = find_markdown_file(pdf_path)
        if not markdown_path:
            raise FileNotFoundError(f"No markdown file found for {pdf_path}")
    elif not markdown_path.exists():
        raise FileNotFoundError(f"Specified markdown file not found: {markdown_path}")
    db = PDFAnkiDB(db_path)
    # Insert PDF
    pdf_id = insert_pdf(pdf_path, db_path)

    # Update markdown content in PDF record and process chunks

    try:
        with open(markdown_path, "r", encoding="utf-8") as f:
            markdown_content = f.read()
        db.update_markdown_content(pdf_id, markdown_content)

        # Process markdown into chunks
        process_markdown_to_chunks(markdown_content, pdf_id, db_path)
    finally:
        db.close()

    return pdf_id, markdown_path


def main():
    parser = argparse.ArgumentParser(
        description="Process PDF files and their corresponding markdown into the Anki database"
    )
    parser.add_argument(
        "pdf_files", nargs="+", type=Path, help="PDF file(s) to process"
    )
    parser.add_argument(
        "--markdown",
        type=Path,
        help="Specific markdown file to use (overrides automatic detection)",
    )
    parser.add_argument(
        "--db",
        type=str,
        default=None,
        help="Path to the database file (default: pdf_anki.db)",
    )
    parser.add_argument(
        "--optional-markdown",
        action="store_true",
        help="Don't fail if no markdown file is found",
    )
    parser.add_argument("--log-file", type=Path, help="Path to log file")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    # Setup logging
    log_level = logging.DEBUG if args.debug else logging.ERROR
    logger = setup_logger(args.log_file, log_level)

    # Validate that --markdown is only used with a single PDF
    if args.markdown and len(args.pdf_files) > 1:
        logger.error("--markdown can only be used when processing a single PDF file")
        sys.exit(1)

    success_count = 0
    total_files = len(args.pdf_files)

    for pdf_path in args.pdf_files:
        try:
            logger.info(f"Processing: {pdf_path}")

            # Validate PDF exists
            if not pdf_path.exists():
                logger.error(f"File not found: {pdf_path}")
                continue

            # Process files
            try:
                logger.debug(f"Starting PDF processing for {pdf_path}")
                pdf_id, md_path = process_files(
                    pdf_path, markdown_path=args.markdown, db_path=args.db
                )

                # Report success
                logger.info(f"Successfully processed PDF (ID: {pdf_id})")
                logger.info(f"Processed markdown from: {md_path}")
                logger.debug(f"Completed processing of {pdf_path}")

                success_count += 1

            except FileNotFoundError as e:
                if args.optional_markdown:
                    logger.warning(f"{e}")
                    logger.warning("Continuing without markdown processing")
                    success_count += 1
                else:
                    logger.error(f"{e}")

        except PDFInsertError as e:
            logger.error(f"Error processing {pdf_path}: {e}")
        except Exception:
            logger.exception(f"Unexpected error processing {pdf_path}")

    # Print summary
    logger.info(
        f"\nSummary: Successfully processed {success_count} out of {total_files} files"
    )

    # Return appropriate exit code
    if success_count < total_files:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
