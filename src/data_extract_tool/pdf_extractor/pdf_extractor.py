import os
import hashlib
import pdfplumber
import logging
import argparse
from datetime import datetime, timezone
from ..utils import connect_to_teradata

LOG_FILE = "./logs/pdf_ingestion.log"

# Setup logging
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def compute_checksum(file_path):
    with open(file_path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()

def extract_text_from_pdf(file_path):
    text = ""
    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
    return text.strip()

def check_and_create_tables(conn, base_table_name):
    """
    Check if the required tables exist and create them if they don't.
    
    Args:
        conn: Teradata connection
        base_table_name: Base name for the tables
    
    Returns:
        tuple: (metadata_table, contents_table) - the actual table names
    """
    metadata_table = f"{base_table_name}_metadata"
    contents_table = f"{base_table_name}_contents"
    
    cursor = conn.cursor()
    
    try:
        # Check if metadata table exists
        check_query = """
        SELECT COUNT(*) 
        FROM DBC.TablesV 
        WHERE TableName = ? 
        AND DatabaseName = DATABASE
        """
        
        cursor.execute(check_query, (metadata_table.upper(),))
        metadata_exists = cursor.fetchone()[0] > 0
        
        if not metadata_exists:
            print(f"[INFO] Creating metadata table {metadata_table}...")
            create_metadata_query = f"""
            CREATE TABLE {metadata_table} (
                id INTEGER GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) NOT NULL,
                file_type VARCHAR(10) NOT NULL,
                file_name VARCHAR(255) NOT NULL,
                ingestion_time_utc TIMESTAMP(6) NOT NULL,
                checksum VARCHAR(64) NOT NULL,
                success BYTEINT NOT NULL,
                PRIMARY KEY (id)
            )
            """
            cursor.execute(create_metadata_query)
            print(f"[OK] Created metadata table {metadata_table}")
        else:
            print(f"[OK] Metadata table {metadata_table} already exists")
        
        # Check if contents table exists
        cursor.execute(check_query, (contents_table.upper(),))
        contents_exists = cursor.fetchone()[0] > 0
        
        if not contents_exists:
            print(f"[INFO] Creating contents table {contents_table}...")
            create_contents_query = f"""
            CREATE TABLE {contents_table} (
                id INTEGER GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) NOT NULL,
                file_id INTEGER NOT NULL,
                text_content CLOB,
                PRIMARY KEY (id),
                FOREIGN KEY (file_id) REFERENCES {metadata_table}(id)
            )
            """
            cursor.execute(create_contents_query)
            print(f"[OK] Created contents table {contents_table}")
        else:
            print(f"[OK] Contents table {contents_table} already exists")
        
        conn.commit()
        return metadata_table, contents_table
        
    except Exception as e:
        print(f"[ERROR] Error checking/creating tables: {e}")
        raise
    finally:
        cursor.close()

def bulk_ingest(files, conn, metadata_table, contents_table):
    content_records = []
    cursor = conn.cursor()

    for file_path in files:
        try:
            file_name = os.path.basename(file_path)
            file_type = "pdf"
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            checksum = compute_checksum(file_path)

            try:
                text_content = extract_text_from_pdf(file_path)
                success = True
            except Exception as e:
                logging.error(f"Failed to extract text from {file_name}: {e}")
                text_content = f"[ERROR] {str(e)}"
                success = False

            cursor.execute(f"""
                INSERT INTO {metadata_table} (file_type, file_name, ingestion_time_utc, checksum, success)
                VALUES (?, ?, ?, ?, ?)
            """, (file_type, file_name, timestamp, checksum, success))

            cursor.execute(f"SELECT TOP 1 id FROM {metadata_table} ORDER BY id DESC")
            file_id = cursor.fetchone()[0]

            content_records.append((file_id, text_content))
            logging.info(f"Ingested file: {file_name} with ID: {file_id}")

        except Exception as e:
            logging.exception(f"Unexpected error processing file {file_path}")

    if content_records:
        try:
            cursor.executemany(
                f"INSERT INTO {contents_table} (file_id, text_content) VALUES (?, ?)",
                content_records
            )
            logging.info(f"Inserted {len(content_records)} file contents into {contents_table}.")
        except Exception as e:
            logging.exception(f"Bulk insert into {contents_table} failed.")

    cursor.close()

def main(argv=None):
    parser = argparse.ArgumentParser(description="Ingest PDF files into Teradata.")
    parser.add_argument(
        "--pdf-dir",
        required=True,
        help="Path to directory containing PDF files"
    )
    parser.add_argument(
        "--table",
        required=True,
        help="Base table name for storing PDF data (will create {table}_metadata and {table}_contents)"
    )
    
    # Use provided arguments or default to sys.argv
    args = parser.parse_args(argv)

    try:
        conn = connect_to_teradata()
        
        # Check and create tables if necessary
        print(f"[INFO] Checking tables for base name: {args.table}")
        metadata_table, contents_table = check_and_create_tables(conn, args.table)
        
        pdf_dir = args.pdf_dir
        files = [os.path.join(pdf_dir, f) for f in os.listdir(pdf_dir) if f.lower().endswith(".pdf")]
        if not files:
            logging.warning(f"No PDF files found in {pdf_dir}")
            print(f"[WARNING] No PDF files found in {pdf_dir}")
        else:
            print(f"[INFO] Found {len(files)} PDF files to process")
            
        bulk_ingest(files, conn, metadata_table, contents_table)
        conn.commit()
        conn.close()
        logging.info("PDF ingestion completed successfully.")
        print("[OK] PDF ingestion completed successfully.")
    except Exception as e:
        logging.exception("Failed to connect or ingest PDFs.")
        print(f"[ERROR] Failed to connect or ingest PDFs: {e}")

if __name__ == "__main__":
    main()
