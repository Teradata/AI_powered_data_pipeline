import os
import argparse
import json
from openai import OpenAI
from ..utils import connect_to_teradata, get_openai_config

def get_openai_client():
    """Get OpenAI client with configuration from centralized config."""
    openai_config = get_openai_config()
    return OpenAI(api_key=openai_config["api_key"])

def get_file_contents(conn, parsed_data_origin, sample=None):
    """
    Get file contents from the specified table.
    
    Args:
        conn: Database connection
        parsed_data_origin: Name of the table containing file contents
        sample: Number of records to sample (None for all records)
    
    Returns:
        List of tuples: (file_id, text_content)
    """
    cursor = conn.cursor()
    
    if sample is not None:
        # Use simple SAMPLE for efficient random sampling (randomization is default)
        query = f"SELECT file_id, text_content FROM {parsed_data_origin} SAMPLE {sample}"
    else:
        query = f"SELECT file_id, text_content FROM {parsed_data_origin}"
    
    cursor.execute(query)
    return cursor.fetchall()

def load_schema(schema_path):
    with open(schema_path, 'r') as f:
        return json.load(f)

def extract_data_from_text(text, schema):
    """Extract data from text using OpenAI with centralized configuration."""
    client = get_openai_client()
    
    # Handle both array and object schemas
    if "items" in schema and "properties" in schema["items"]:
        # Array schema (like schema_alt.json)
        schema_str = json.dumps(schema["items"]["properties"], indent=2)
        schema_type = "array"
    else:
        # Object schema (like schema.json)
        schema_str = json.dumps(schema["properties"], indent=2)
        schema_type = "object"
    
    system_prompt = (
        f"""
        You are a medical data analyst. Extract structured data from health insurance documents according to the provided schema.
        Return a JSON matching the schema structure exactly don't return object when array is requested or array when object is requested.
        """
    )
    
    user_prompt = f"""
        Schema:
        {schema_str}

        Document Text:
        {text}

        Extract all available data according to the schema. Return as JSON {schema_type}.
        Don't include data that is not in the document to complete all data required by the schema.
        """

    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.2
        )
        content = response.choices[0].message.content
        
        # Try to parse the JSON response
        try:
            parsed_data = json.loads(content)
            return parsed_data, None
        except json.JSONDecodeError as e:
            print(f"Failed to parse OpenAI response as JSON: {e}")
            print(f"Raw response: {content}")
            return None, str(e)
            
    except Exception as e:
        print(f"OpenAI API call failed: {e}")
        return None, str(e)

def check_and_create_table(conn, table_name):
    """
    Check if the parsed_data_general table exists, and create it if it doesn't.
    
    Args:
        conn: Teradata connection
        table_name: Name of the table to check/create
    
    Returns:
        bool: True if table exists or was created successfully, False otherwise
    """
    cursor = conn.cursor()
    
    try:
        # Check if table exists
        check_query = """
        SELECT COUNT(*) 
        FROM DBC.TablesV 
        WHERE TableName = ? 
        AND DatabaseName = DATABASE
        """
        
        cursor.execute(check_query, (table_name.upper(),))
        table_exists = cursor.fetchone()[0] > 0
        
        if table_exists:
            print(f"[OK] Table {table_name} already exists")
            return True
        
        # Table doesn't exist, create it
        print(f"[INFO] Creating table {table_name}...")
        
        create_table_query = f"""
        CREATE TABLE {table_name} (
            id INTEGER GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) NOT NULL,
            file_id INTEGER NOT NULL,
            schema_name VARCHAR(255) NOT NULL,
            parsed_data JSON,
            parsing_timestamp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id)
        )
        """
        
        cursor.execute(create_table_query)
        
        conn.commit()
        print(f"[OK] Successfully created table {table_name}")
        return True
        
    except Exception as e:
        print(f"[ERROR] Error checking/creating table {table_name}: {e}")
        return False
    finally:
        cursor.close()

def insert_parsed_data_to_teradata(conn, file_id, schema_name, parsed_data, table_name):
    """
    Insert parsed data into a general Teradata table.
    
    Table structure should be:
    CREATE TABLE parsed_data_general (
        id INTEGER GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) NOT NULL,
        file_id INTEGER NOT NULL,
        schema_name VARCHAR(255) NOT NULL,
        parsed_data JSON,
        parsing_timestamp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id)
    );
    """
    cursor = conn.cursor()
    
    insert_query = f"""
    INSERT INTO {table_name} (
        file_id, 
        schema_name, 
        parsed_data
    ) VALUES (?, ?, ?)
    """
    
    try:
        # Convert data to JSON string for storage
        parsed_data_json = json.dumps(parsed_data) if parsed_data is not None else None
        
        values = [
            file_id,
            schema_name,
            parsed_data_json
        ]
        
        cursor.execute(insert_query, values)
        print(f"[OK] Inserted parsed data for file {file_id} into {table_name}")
        print(f"   Schema: {schema_name}")
        print(f"   Data length: {len(str(parsed_data)) if parsed_data else 0} characters")
        
    except Exception as e:
        print(f"[ERROR] Error inserting parsed data for file {file_id}: {e}")
        print(f"   Data length: {len(str(parsed_data)) if parsed_data else 0} characters")
    finally:
        cursor.close()

def main(argv=None):
    parser = argparse.ArgumentParser(description="Extract flexible structured data from insurance documents using OpenAI.")
    parser.add_argument("--schema", required=True, help="Path to the JSON schema file.")
    parser.add_argument("--parsed-data-destination", required=True, help="Teradata table name to insert parsed data.")
    parser.add_argument("--parsed-data-origin", required=True, help="Teradata table name containing the source file contents.")
    parser.add_argument("--schema-name", help="Name to identify the schema (defaults to filename).")
    parser.add_argument("--sample", type=int, help="Number of records to randomly sample (default: process all files)")
    args = parser.parse_args(argv)

    schema = load_schema(args.schema)
    schema_name = args.schema_name or os.path.basename(args.schema)
    conn = connect_to_teradata()

    # Check if table exists and create if necessary
    print(f"[INFO] Checking table {args.parsed_data_destination}...")
    if not check_and_create_table(conn, args.parsed_data_destination):
        print(f"[ERROR] Failed to verify/create table {args.parsed_data_destination}. Exiting.")
        conn.close()
        return

    # Show processing info
    if args.sample:
        print(f"[INFO] Sampling {args.sample} files from {args.parsed_data_origin}")
    else:
        print(f"[INFO] Processing all files from {args.parsed_data_origin}")

    processed_files = 0
    successful_parses = 0
    
    for file_id, text in get_file_contents(conn, args.parsed_data_origin, args.sample):
        print(f"\n[INFO] Processing file ID: {file_id} with text length: {len(text)}")
        
        if not text.strip():
            print("   [WARNING] Skipping empty file")
            continue
        
        # Extract data using OpenAI
        parsed_data, error = extract_data_from_text(text, schema)
        
        if error:
            print(f"   [ERROR] Parsing failed: {error}")
            # Still insert the record to track the failure
            insert_parsed_data_to_teradata(
                conn, file_id, schema_name, error, args.parsed_data_destination
            )
        else:
            print(f"   [OK] Parsing successful")
            successful_parses += 1
            
            # Insert into general table
            insert_parsed_data_to_teradata(
                conn, file_id, schema_name, parsed_data, args.parsed_data_destination
            )
        
        processed_files += 1

    conn.commit()
    conn.close()
    
    print(f"\n[SUMMARY] Processing Summary:")
    print(f"   Files processed: {processed_files}")
    print(f"   Successful parses: {successful_parses}")
    print(f"   Failed parses: {processed_files - successful_parses}")
    print(f"   Success rate: {(successful_parses/processed_files*100):.1f}%" if processed_files > 0 else "   Success rate: 0%")

if __name__ == "__main__":
    main()
