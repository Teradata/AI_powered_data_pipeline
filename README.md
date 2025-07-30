# AI Powered Data Ingestion Pipeline

A comprehensive tool for extracting text from PDF files and parsing it into structured data using OpenAI and Teradata.

## Features

- **PDF Text Extraction**: Extract text content from PDF files and store in Teradata database
- **AI-Powered Parsing**: Use OpenAI to parse unstructured text into structured JSON data
- **Flexible Schema Support**: Define custom JSON schemas for different document types
- **Teradata Integration**: Direct integration with Teradata for data storage and retrieval
- **Sampling Support**: Process all documents or sample a subset for testing/development
- **Command Line Interface**: Easy-to-use CLI with multiple operation modes

## Installation

### Prerequisites

- Python 3.10 or higher
- Teradata database access, get one for free at ClearScape Analytics Experience [https://www.teradata.com/getting-started/demos/clearscape-analytics?utm_campaign=gbl-clearscape-analytics-devrel&utm_content=demo&utm_id=7016R000001n3bCQAQ]
- OpenAI API key

### Setup

1. Clone the repository:
```bash
git clone https://github.com/Teradata/AI_powered_data_pipeline
cd AI_powered_data_pipeline
```

2. Install dependencies using uv (recommended):
```bash
uv sync
```

Or using pip:
```bash
pip install -e .
```

3. Set up environment variables:
Create a `.env` file in the project root:
```env
OPENAI_API_KEY=your_openai_api_key_here
TD_HOST=your_teradata_host
TD_USERNAME=your_username
TD_PASSWORD=your_password
TD_DATABASE=your_database
```

## Usage

The tool provides three main commands:

### 1. Extract PDF Files Only

Extract text from PDF files and store in Teradata:

```bash
python main.py extract-pdf --pdf-dir ./data/pdfs --table healthcare_docs
```

This creates two tables:
- `healthcare_docs_metadata` - PDF file metadata
- `healthcare_docs_contents` - Extracted text content

### 2. Parse Text Only (Flexible Parsing)

Parse existing text data using OpenAI with a custom schema:

```bash
python main.py parse-flexible \
    --schema schema_alt.json \
    --parsed-data-destination parsed_results \
    --parsed-data-origin healthcare_docs_contents \
    --sample 5
```

### 3. Full Pipeline

Run complete extraction and parsing pipeline:

```bash
python main.py full-pipeline \
    --pdf-dir ./data/pdfs \
    --schema schema_alt.json \
    --table healthcare_docs \
    --parsed-data-destination final_results \
    --sample 10
```

## Schema Configuration

Define your parsing schema in JSON format. Example `schema_alt.json`:

```json
{
  "type": "object",
  "properties": {
    "patient_info": {
      "type": "object",
      "properties": {
        "name": {"type": "string"},
        "dob": {"type": "string"},
        "member_id": {"type": "string"}
      }
    },
    "claim_details": {
      "type": "object",
      "properties": {
        "claim_number": {"type": "string"},
        "service_date": {"type": "string"},
        "amount": {"type": "number"}
      }
    }
  }
}
```

## Command Line Options

### Common Options

- `--sample N` - Process only N random records (useful for testing)
- `--schema PATH` - Path to JSON schema file
- `--table NAME` - Base table name for operations

### Extract PDF Options

- `--pdf-dir PATH` - Directory containing PDF files
- `--table NAME` - Base table name (creates `{name}_metadata` and `{name}_contents`)

### Parse Flexible Options

- `--parsed-data-destination NAME` - Output table for parsed data
- `--parsed-data-origin NAME` - Input table containing text to parse
- `--schema-name NAME` - Optional identifier for the schema (defaults to filename)

### Full Pipeline Options

- `--pdf-dir PATH` - Directory containing PDF files
- `--parsed-data-destination NAME` - Output table (defaults to `{table}_parsed`)

## Database Schema

### Metadata Table Structure
```sql
CREATE TABLE {table}_metadata (
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    filename VARCHAR(255),
    file_path VARCHAR(500),
    file_size INTEGER,
    checksum VARCHAR(64),
    processing_timestamp TIMESTAMP(6),
    PRIMARY KEY (id)
)
```

### Contents Table Structure
```sql
CREATE TABLE {table}_contents (
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    file_id INTEGER,
    text_content CLOB,
    extraction_timestamp TIMESTAMP(6),
    PRIMARY KEY (id),
    FOREIGN KEY (file_id) REFERENCES {table}_metadata(id)
)
```

### Parsed Data Table Structure
```sql
CREATE TABLE {table}_parsed (
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    file_id INTEGER,
    schema_name VARCHAR(255),
    parsed_data JSON,
    parsing_timestamp TIMESTAMP(6),
    PRIMARY KEY (id)
)
```

## Development

### Project Structure

```
healthcare_AI_ux/
├── src/
│   └── data_extract_tool/
│       ├── pdf_extractor/
│       │   ├── __init__.py
│       │   └── pdf_extractor.py
│       ├── text_parser/
│       │   ├── __init__.py
│       │   └── flexible_text_parser.py
│       ├── utils.py
│       └── __init__.py
├── main.py
├── pyproject.toml
├── .env
└── README.md
```

## Error Handling

- Failed PDF extractions are logged but don't stop processing
- Failed OpenAI parsing attempts are stored as NULL in the database
- All operations provide detailed logging and progress feedback
- Database transactions ensure data consistency

## Performance Considerations

- Use `--sample N` for development and testing to limit API costs
- Teradata's `SAMPLE` clause provides efficient random sampling
- Large PDF files are processed incrementally
- OpenAI API calls are made sequentially to respect rate limits

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
This project is intended for educational purposes as a simple demo, it is not production grade software and it is provided as part of illustrative materials for developer advocacy content as is.

## Support

For questions or issues, please open an issue on the GitHub repository.