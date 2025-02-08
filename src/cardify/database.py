import logging
import sqlite3
import sys
from pathlib import Path
from typing import Dict, Optional, Union

logger = logging.getLogger(__name__)  # Get the logger for this module


class PDFAnkiDB:
    def __init__(self, db_path: str = "pdf_anki.db"):
        """Initialize the database connection and create tables if they don't exist.

        Args:
            db_path (str): Path to the SQLite database file. Defaults to "pdf_anki.db"

        Raises:
            sqlite3.Error: If database initialization fails
        """
        if db_path is None:
            db_path = "pdf_anki.db"
            logger.debug("db_path None using default path")
        logger.debug(f"Initializing database with path: {db_path}")
        path = Path(db_path)

        if not path.exists():
            logger.info(f"Database file not found at: {db_path}")

            while True:
                response = (
                    input(f"Database file {db_path} does not exist. Create it? [y/n]: ")
                    .lower()
                    .strip()
                )
                if response in ["y", "n"]:
                    break
                logger.warning("Invalid input received, expected 'y' or 'n'")
                print("Please enter 'y' or 'n'")

            if response == "n":
                logger.info("User declined database creation, exiting")
                sys.exit(0)

            logger.info(f"Creating new database at: {db_path}")
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                logger.debug(f"Created parent directory: {path.parent}")
            except Exception as e:
                logger.error(f"Failed to create parent directory: {e}")
                raise

        try:
            self.conn = sqlite3.connect(db_path)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
            self._create_tables()
            logger.debug("Successfully initialized database connection and tables")
        except sqlite3.Error as e:
            logger.error(f"Database initialization failed: {e}")
            raise

    def _create_tables(self):
        """Create all necessary tables for the PDF to Anki system."""
        # PDFs table stores the original PDF files and their metadata
        self.cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS pdfs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            file_content BLOB NOT NULL,
            markdown_content TEXT,
            title TEXT,
            author TEXT,
            publication_date TEXT,
            page_count INTEGER,
            file_size INTEGER,
            md5_hash TEXT UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        )

        # Text chunks table stores segments of the PDF content with specific headers
        self.cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS text_chunks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pdf_id INTEGER NOT NULL,
            chunk_index INTEGER NOT NULL,
            page_content TEXT NOT NULL,
            type TEXT NOT NULL,
            header_1 TEXT,
            header_2 TEXT,
            header_3 TEXT,
            header_4 TEXT,
            code BOOLEAN NOT NULL DEFAULT 0,
            start_page INTEGER NOT NULL,
            end_page INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (pdf_id) REFERENCES pdfs (id) ON DELETE CASCADE,
            UNIQUE(pdf_id, chunk_index)
        )
        """
        )

        # Anki cards table stores the card information
        self.cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS anki_cards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chunk_id INTEGER NOT NULL,
            front_content TEXT NOT NULL,
            back_content TEXT NOT NULL,
            note_type TEXT DEFAULT 'Basic',
            tags TEXT,
            deck_name TEXT DEFAULT 'Default',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (chunk_id) REFERENCES text_chunks (id) ON DELETE CASCADE
        )
        """
        )

        # Create indexes for better query performance
        self.cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_pdf_hash ON pdfs (md5_hash)"
        )
        self.cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_chunks_pdf ON text_chunks (pdf_id)"
        )
        self.cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_cards_chunk ON anki_cards (chunk_id)"
        )

        self.conn.commit()

    def add_pdf(
        self, file_path: str, metadata: Optional[Dict[str, Union[str, int]]] = None
    ) -> int:
        """
        Add a PDF file and its metadata to the database.

        Args:
            file_path: Path to the PDF file
            metadata: Dictionary containing PDF metadata

        Returns:
            id: The ID of the inserted PDF record

        Raises:
            sqlite3.Error: If the insertion fails
        """
        if metadata is None:
            metadata = {}

        with open(file_path, "rb") as f:
            pdf_content = f.read()

        self.cursor.execute(
            """
        INSERT INTO pdfs (
            filename,
            file_content,
            title,
            author,
            publication_date,
            page_count,
            file_size,
            md5_hash
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                Path(file_path).name,
                pdf_content,
                metadata.get("title"),
                metadata.get("author"),
                metadata.get("publication_date"),
                metadata.get("page_count"),
                len(pdf_content),
                metadata.get("md5_hash"),
            ),
        )

        self.conn.commit()
        last_id = self.cursor.lastrowid
        if last_id is None:
            raise sqlite3.Error("Failed to insert PDF record")
        return last_id

    def add_text_chunk(
        self,
        pdf_id: int,
        page_content: str,
        chunk_type: str,
        start_page: int,
        end_page: int,
        chunk_index: int,
        header_1: Optional[str] = None,
        header_2: Optional[str] = None,
        header_3: Optional[str] = None,
        header_4: Optional[str] = None,
        is_code: bool = False,
    ) -> int:
        """
        Add a text chunk associated with a PDF.

        Args:
            pdf_id: ID of the associated PDF
            page_content: The main text content of the chunk
            chunk_type: Type of the chunk
            start_page: Starting page number of the chunk
            end_page: Ending page number of the chunk
            chunk_index: Index of the chunk in sequence
            header_1: First level header
            header_2: Second level header
            header_3: Third level header
            header_4: Fourth level header
            is_code: Boolean indicating if the chunk contains code

        Returns:
            id: The ID of the inserted chunk record

        Raises:
            sqlite3.Error: If the insertion fails
            sqlite3.IntegrityError: If chunk_index already exists for this pdf_id
                                  or if page_content is duplicate
        """
        self.cursor.execute(
            """
        INSERT INTO text_chunks (
            pdf_id,
            chunk_index,
            page_content,
            type,
            start_page,
            end_page,
            header_1,
            header_2,
            header_3,
            header_4,
            code
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                pdf_id,
                chunk_index,
                page_content,
                chunk_type,
                start_page,
                end_page,
                header_1,
                header_2,
                header_3,
                header_4,
                1 if is_code else 0,
            ),
        )

        self.conn.commit()
        last_id = self.cursor.lastrowid
        if last_id is None:
            raise sqlite3.Error("Failed to insert text chunk record")
        return last_id

    def add_anki_card(
        self,
        chunk_id: int,
        front_content: str,
        back_content: str,
        note_type: str = "Basic",
        tags: Optional[str] = None,
        deck_name: str = "Default",
    ) -> int:
        """
        Add an Anki card associated with a text chunk.

        Args:
            chunk_id: ID of the associated text chunk
            front_content: Content for the front of the card
            back_content: Content for the back of the card
            note_type: Type of Anki note
            tags: Space-separated tags for the card
            deck_name: Name of the deck for the card

        Returns:
            id: The ID of the inserted card record

        Raises:
            sqlite3.Error: If the insertion fails
        """
        self.cursor.execute(
            """
        INSERT INTO anki_cards (
            chunk_id,
            front_content,
            back_content,
            note_type,
            tags,
            deck_name
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
            (chunk_id, front_content, back_content, note_type, tags, deck_name),
        )

        self.conn.commit()
        last_id = self.cursor.lastrowid
        if last_id is None:
            raise sqlite3.Error("Failed to insert Anki card record")
        return last_id

    def get_pdf_chunks(self, pdf_id: int):
        """Get all text chunks for a given PDF."""
        self.cursor.execute(
            """
        SELECT * FROM text_chunks 
        WHERE pdf_id = ? 
        ORDER BY chunk_index ASC
        """,
            (pdf_id,),
        )
        return self.cursor.fetchall()

    def get_chunk_cards(self, chunk_id: int):
        """Get all Anki cards associated with a text chunk."""
        self.cursor.execute(
            """
        SELECT * FROM anki_cards 
        WHERE chunk_id = ?
        """,
            (chunk_id,),
        )
        return self.cursor.fetchall()

    def update_markdown_content(self, pdf_id: int, markdown_content: str):
        """Update the markdown content for a PDF."""
        self.cursor.execute(
            """
        UPDATE pdfs 
        SET markdown_content = ?,
            last_modified = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
            (markdown_content, pdf_id),
        )
        self.conn.commit()

    def close(self):
        """Close the database connection."""
        self.conn.close()
