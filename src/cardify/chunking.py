import polars as pl
from langchain_text_splitters.markdown import ExperimentalMarkdownSyntaxTextSplitter

if __name__ == "__main__":
    with open("../../tmp/model_checking.md", "r") as f:
        markdown_content = f.read()

    splitter = ExperimentalMarkdownSyntaxTextSplitter()
    chunks = splitter.split_text(markdown_content)

    # Flatten metadata into top-level columns during generation
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

    # 1. Extract page markers
    lazy_df = lazy_df.with_columns(
        pl.col("page_content").str.extract_all(r"\{\d+\}--+").alias("page_markers")
    )

    # 2. Extract initial start and end pages (may contain nulls)
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

    # 3. Create temporary column with forward-filled end pages
    lazy_df = lazy_df.with_columns(
        pl.col("end_page").fill_null(strategy="forward").alias("_prev_end")
    )

    # 4. Fill nulls in start_page and end_page using _prev_end
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

    lazy_df = lazy_df.with_columns(pl.col("Code").is_not_null().alias("Code"))

    # 5. Drop temporary columns
    lazy_df = lazy_df.drop(["page_markers", "_prev_end"])

    # 6. Collect and save to parquet
    df = lazy_df.collect()
    df.write_parquet("../../tmp/chunks.parquet")
