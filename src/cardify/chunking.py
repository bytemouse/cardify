from typing import Optional

import polars as pl
from langchain_text_splitters.markdown import ExperimentalMarkdownSyntaxTextSplitter

from .database import PDFAnkiDB


def process_markdown_to_chunks(
    markdown_content: str, pdf_id: int, db_path: Optional[str] = None
) -> None:
    """
    Process markdown content into chunks and insert them into the database.

    Args:
        markdown_content: The markdown text to process
        pdf_id: The ID of the associated PDF in the database
        db_path: Optional path to the database file
    """
    # Initialize splitter and create chunks
    splitter = ExperimentalMarkdownSyntaxTextSplitter()
    chunks = splitter.split_text(markdown_content)

    # Create LazyFrame from chunks
    lazy_df = pl.LazyFrame(
        (
            {
                "id": chunk.id,
                **chunk.metadata,  # Unpack metadata dict into top-level keys
                "page_content": chunk.page_content,
                "type": chunk.type,
            }
            for chunk in chunks
        )
    )

    # Ensure Header columns exist with null values if missing
    for i in range(1, 5):
        header_col = f"Header {i}"
        if header_col not in lazy_df.columns:
            lazy_df = lazy_df.with_columns(pl.lit(None).alias(header_col))

    # Extract and process page markers lazily
    lazy_df = lazy_df.with_columns(
        pl.col("page_content").str.extract_all(r"\{\d+\}--+").alias("page_markers")
    )

    # Extract start and end pages lazily
    lazy_df = lazy_df.with_columns(
        pl.col("page_markers")
        .list.first()
        .str.extract(r"\{(\d+)\}")
        .cast(pl.Int64)
        .alias("start_page"),
        pl.col("page_markers")
        .list.last()
        .str.extract(r"\{(\d+)\}")
        .cast(pl.Int64)
        .alias("end_page"),
    )

    # Handle page number continuity lazily
    lazy_df = lazy_df.with_columns(
        pl.col("end_page").fill_null(strategy="forward").alias("_prev_end")
    )

    # Fill missing page numbers lazily
    lazy_df = lazy_df.with_columns(
        pl.when(pl.col("start_page").is_null())
        .then(pl.col("_prev_end"))
        .otherwise(pl.col("start_page"))
        .alias("start_page"),
        pl.when(pl.col("end_page").is_null())
        .then(pl.col("_prev_end"))
        .otherwise(pl.col("end_page"))
        .alias("end_page"),
    )

    # Process code flag lazily
    lazy_df = lazy_df.with_columns(pl.col("Code").is_not_null().alias("Code"))

    # Clean up temporary columns and rename headers lazily
    header_renames = {f"Header {i}": f"header_{i}" for i in range(1, 5)}

    lazy_df = lazy_df.drop(["page_markers", "_prev_end"]).rename(header_renames)

    # Initialize database connection
    db = PDFAnkiDB(db_path) if db_path else PDFAnkiDB()

    try:
        # Collect the processed data only when needed
        for i, row in enumerate(lazy_df.collect().iter_rows(named=True)):
            db.add_text_chunk(
                pdf_id=pdf_id,
                page_content=row["page_content"],
                chunk_type=row["type"],
                start_page=row["start_page"],
                end_page=row["end_page"],
                chunk_index=i,
                is_code=bool(row.get("Code", False)),
                header_1=row["header_1"],
                header_2=row["header_2"],
                header_3=row["header_3"],
                header_4=row["header_4"],
            )
    finally:
        db.close()


def process_markdown_file(
    markdown_path: str, pdf_id: int, db_path: Optional[str] = None
) -> None:
    """
    Process a markdown file and insert its chunks into the database.

    Args:
        markdown_path: Path to the markdown file
        pdf_id: The ID of the associated PDF in the database
        db_path: Optional path to the database file
    """
    with open(markdown_path, "r", encoding="utf-8") as f:
        markdown_content = f.read()

    process_markdown_to_chunks(markdown_content, pdf_id, db_path)
