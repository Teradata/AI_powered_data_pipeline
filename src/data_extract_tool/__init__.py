"""
Data Extract Tool Package

A package for extracting text from PDF files and parsing it into structured data using OpenAI and Teradata.
"""

from . import pdf_extractor
from . import text_parser
from . import utils

# Import main functions for easy access
from .pdf_extractor import pdf_extractor_main
from .text_parser import flexible_text_parser_main
from .utils import (
    get_config, 
    get_teradata_config, 
    get_openai_config, 
    connect_to_teradata, 
    test_connection, 
    validate_config
)

__version__ = "1.0.0"
__all__ = [
    'pdf_extractor',
    'utils',
    'pdf_extractor_main',
    'text_parser_main',
    'flexible_text_parser_main',
    'get_config',
    'get_teradata_config',
    'get_openai_config',
    'connect_to_teradata',
    'test_connection',
    'validate_config'
]