"""
PDF Extractor module for extracting text from PDF files and storing in Teradata.
"""

from .pdf_extractor import (
    compute_checksum,
    extract_text_from_pdf,
    bulk_ingest,
    main as pdf_extractor_main
)

__all__ = [
    'compute_checksum',
    'extract_text_from_pdf', 
    'bulk_ingest',
    'pdf_extractor_main'
]
