import sqlite3
from pathlib import Path
from typing import Dict, Optional, Union


class PDFAnkiDB:
    def __init__(self, db_path: str = "pdf_anki.sqlite3"):
        """Initialize the database connection and create tables if they don't exist."""
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        self._create_tables()

    def _create_tables(self):
        """Create all necessary tables for the PDF to Anki system."""
        # PDFs table stores the original PDF files and their metadata
        self.cursor.execute("""
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
            md5_hash TEXT UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        # Text chunks table stores segments of the PDF content
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS text_chunks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pdf_id INTEGER NOT NULL,
            content TEXT NOT NULL,
            start_page INTEGER,
            end_page INTEGER,
            chunk_index INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (pdf_id) REFERENCES pdfs (id) ON DELETE CASCADE
        )
        """)

        # Anki cards table stores the card information
        self.cursor.execute("""
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
        """)

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
        content: str,
        start_page: Optional[int] = None,
        end_page: Optional[int] = None,
        chunk_index: Optional[int] = None,
    ) -> int:
        """
        Add a text chunk associated with a PDF.

        Args:
            pdf_id: ID of the associated PDF
            content: The text content of the chunk
            start_page: Starting page number of the chunk
            end_page: Ending page number of the chunk
            chunk_index: Index of the chunk in sequence

        Returns:
            id: The ID of the inserted chunk record

        Raises:
            sqlite3.Error: If the insertion fails
        """
        self.cursor.execute(
            """
        INSERT INTO text_chunks (
            pdf_id,
            content,
            start_page,
            end_page,
            chunk_index
        ) VALUES (?, ?, ?, ?, ?)
        """,
            (pdf_id, content, start_page, end_page, chunk_index),
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


# Example usage:
if __name__ == "__main__":
    db = PDFAnkiDB()

    db.close()
