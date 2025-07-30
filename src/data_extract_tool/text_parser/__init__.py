"""
Text Parser module for parsing extracted text using OpenAI and storing structured data in Teradata.
"""
from .flexible_text_parser import (
    extract_data_from_text,
    check_and_create_table,
    insert_parsed_data_to_teradata,
    main as flexible_text_parser_main
)

__all__ = [
    'extract_data_from_text',
    'validate_required_fields',
    'check_and_create_table',
    'insert_parsed_data_to_teradata',
    'flexible_text_parser_main'
]
