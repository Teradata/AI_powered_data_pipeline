#!/usr/bin/env python3
"""
Main entry point for the data_extract_tool package.
This script demonstrates how to use the pdf_extractor and text_parser modules.
"""

import sys
import os
import argparse

# Add the src directory to the Python path so we can import the package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from data_extract_tool import pdf_extractor_main, flexible_text_parser_main


def main():
    parser = argparse.ArgumentParser(description="Healthcare AI UX Data Extract Tool")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # PDF extraction command
    pdf_parser = subparsers.add_parser("extract-pdf", help="Extract text from PDF files")
    pdf_parser.add_argument("--pdf-dir", required=True, help="Directory containing PDF files")
    pdf_parser.add_argument("--table", required=True, help="Base table name for storing extracted data")

    # Flexible text parsing command
    flex_parser = subparsers.add_parser("parse-flexible", help="Parse text into flexible JSON format")
    flex_parser.add_argument("--schema", required=True, help="Path to JSON schema file")
    flex_parser.add_argument("--parsed-data-destination", required=True, help="Table name for parsed data")
    flex_parser.add_argument("--parsed-data-origin", required=True, help="Table name containing source file contents")
    flex_parser.add_argument("--schema-name", help="Name to identify the schema")
    flex_parser.add_argument("--sample", type=int, help="Number of records to randomly sample (default: process all)")
    
    # Full pipeline command
    pipeline_parser = subparsers.add_parser("full-pipeline", help="Run complete PDF extraction and parsing pipeline")
    pipeline_parser.add_argument("--pdf-dir", required=True, help="Directory containing PDF files")
    pipeline_parser.add_argument("--table", required=True, help="Base table name for PDF extraction (defaults to 'healthcare_docs')")
    pipeline_parser.add_argument("--schema", required=True, help="Path to JSON schema file")
    pipeline_parser.add_argument("--parsed-data-destination", help="Table name for parsed data, defaults to '{table}_parsed'")
    pipeline_parser.add_argument("--sample", type=int, help="Number of records to randomly sample (default: process all)")
    
    args = parser.parse_args()
    
    if args.command == "extract-pdf":
        pdf_extractor_main(["--pdf-dir", args.pdf_dir, "--table", args.table])
        
    elif args.command == "parse-flexible":
        cmd_args = [
            "--parsed-data-destination", args.parsed_data_destination or f"{args.table}_parsed",
            "--parsed-data-origin", args.parsed_data_origin,
            "--schema", args.schema
        ]
        if args.sample:
            cmd_args.extend(["--sample", str(args.sample)])
        flexible_text_parser_main(cmd_args)
        
    elif args.command == "full-pipeline":
        # Step 1: Extract PDFs
        
        print("=== STEP 1: PDF EXTRACTION ===")
        pdf_extractor_main(["--pdf-dir", args.pdf_dir, "--table", args.table])
        
        print("\n=== STEP 2: TEXT PARSING ===")
        cmd_args = [
            "--parsed-data-destination", args.parsed_data_destination or f"{args.table}_parsed",
            "--parsed-data-origin", f'{args.table}_contents',
            "--schema", args.schema, 
        ]
        if args.sample:
            cmd_args.extend(["--sample", str(args.sample)])
            
            flexible_text_parser_main(cmd_args)    
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
