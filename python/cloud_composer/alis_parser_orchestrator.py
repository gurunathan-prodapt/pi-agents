# Python Script for Cloud Composer/Functions: alis_parser_orchestrator
# Legacy Source: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh (main parsing logic)
# Job: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh
# Description: Re-implements the complex, dynamic parsing and inclusion logic
#              of h_alis_parser.ksh, interacting with BigQuery for templates and data.
#              This script would typically be deployed to Cloud Composer (Apache Airflow)
#              or Cloud Functions.

import os
from google.cloud import bigquery
from datetime import datetime, timedelta
import re

# Initialize BigQuery Client
bq_client = bigquery.Client()

# --- Configuration ---
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "<PROJECT_ID>")
DATASET_ID = os.environ.get("BQ_DATASET_ID", "<DATASET_ID>")

# --- Helper Functions (Mimicking h_alis_parser.ksh logic) ---

def fetch_template_content(template_name: str) -> str:
    """Fetches template content from BigQuery."""
    query = f"""
        SELECT template_content
        FROM `{PROJECT_ID}.{DATASET_ID}.templates`
        WHERE template_name = @template_name
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("template_name", "STRING", template_name),
        ]
    )
    query_job = bq_client.query(query, job_config=job_config)
    results = query_job.result()
    for row in results:
        return row.template_content
    return None

def fetch_attribute_value(attribute_name: str) -> str:
    """Fetches attribute value from BigQuery."""
    query = f"""
        SELECT attribute_value
        FROM `{PROJECT_ID}.{DATASET_ID}.attributes`
        WHERE attribute_name = @attribute_name
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("attribute_name", "STRING", attribute_name),
        ]
    )
    query_job = bq_client.query(query, job_config=job_config)
    results = query_job.result()
    for row in results:
        return row.attribute_value
    return None

def fetch_list_elements(list_name: str) -> list:
    """Fetches list elements from BigQuery."""
    query = f"""
        SELECT element_value
        FROM `{PROJECT_ID}.{DATASET_ID}.list_data`
        WHERE list_name = @list_name
        ORDER BY element_order
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("list_name", "STRING", list_name),
        ]
    )
    query_job = bq_client.query(query, job_config=job_config)
    results = query_job.result()
    return [row.element_value for row in results]

def apply_attribute_substitution(text: str, attribute_name: str, attribute_value: str) -> str:
    """Replaces a specific attribute placeholder in text."""
    # This is a direct replacement, similar to the BQ SP, but allows Python control
    return text.replace(f"<{attribute_name}>", attribute_value)

def apply_list_substitution(text: str, list_name: str, elements: list) -> str:
    """Applies list-based substitutions (<FIRST>, <MIDDLE>, <END>, <SINGLE>)."""
    num_elements = len(elements)

    if num_elements == 1:
        text = text.replace('<SINGLE>', elements[0])
        text = text.replace('<FIRST>', '')
        text = text.replace('<MIDDLE>', '')
        text = text.replace('<END>', '')
    elif num_elements > 1:
        text = text.replace('<FIRST>', elements[0])
        text = text.replace('<MIDDLE>', '\n'.join(elements[1:-1])) # Assuming newline separation
        text = text.replace('<END>', elements[-1])
        text = text.replace('<SINGLE>', '')
    else: # Empty list
        text = text.replace('<SINGLE>', '')
        text = text.replace('<FIRST>', '')
        text = text.replace('<MIDDLE>', '')
        text = text.replace('<END>', '')
    return text

def resolve_includes(text: str, processed_files: set = None) -> str:
    """Recursively resolves <NODE INCLUDE file> directives."""
    if processed_files is None:
        processed_files = set()

    # Avoid infinite loops for circular includes
    if hash(text) in processed_files:
        print("Warning: Circular include detected. Skipping further processing for this block.")
        return text
    processed_files.add(hash(text))

    # Regex to find <NODE INCLUDE file>
    include_pattern = r"<NODE\s+INCLUDE\s+([\w\-\./]+)\s*>"
    
    match = re.search(include_pattern, text)
    while match:
        include_file_name = match.group(1)
        included_content = fetch_template_content(include_file_name)
        if included_content is None:
            print(f"Warning: Included file '{include_file_name}' not found. Replacing with empty string.")
            text = text.replace(match.group(0), '')
        else:
            # Recursively resolve includes within the included content
            resolved_included_content = resolve_includes(included_content, processed_files.copy())
            text = text.replace(match.group(0), resolved_included_content)
        match = re.search(include_pattern, text) # Search again for next include

    # After includes, resolve regular <NODE name>...</NODE> patterns
    node_pattern = r"(?sU)<NODE\s+([^>]+)>(.*)</NODE>"
    # This might require multiple passes or careful design if nodes can be nested
    # For simplicity, this example assumes non-nested nodes or a specific order of processing.
    # The original ksh script parsed nodes as part of its main loop.
    # For a full reimplementation, this might need a more stateful parser.
    return text

def apply_timestamp_substitutions(text: str) -> str:
    """Applies timestamp substitutions like <SYSDATE:YYYYMMDD>, <SYSDATE:-5d:YYYY-MM-DD>."""
    # Pattern for <SYSDATE[:offset][:format]>
    timestamp_pattern = r"<SYSDATE(?::([^\s:]+))?(?::([^\s>]+))?>"

    def replace_ts_match(match):
        offset_str = match.group(1)
        format_str = match.group(2) if match.group(2) else "%Y%m%d" # Default format

        base_date = datetime.now()

        if offset_str:
            offset_match = re.match(r"([-+]?\d+)([dhmy])", offset_str)
            if offset_match:
                value = int(offset_match.group(1))
                unit = offset_match.group(2)
                if unit == 'd':
                    base_date += timedelta(days=value)
                elif unit == 'h':
                    base_date += timedelta(hours=value)
                # Add more units as needed (m=month, y=year would be more complex)
            else:
                print(f"Warning: Unknown offset format '{offset_str}'. Using current date.")
        
        return base_date.strftime(format_str)

    return re.sub(timestamp_pattern, replace_ts_match, text)

def parse_template(template_name: str, config: dict) -> str:
    """
    Main function to parse a template, applying all substitutions.
    The 'config' dictionary would contain mappings for attributes, lists, etc.
    """
    processed_content = fetch_template_content(template_name)
    if processed_content is None:
        return f"Error: Template '{template_name}' not found."

    # 1. Resolve includes
    processed_content = resolve_includes(processed_content)

    # 2. Apply attribute substitutions
    for attr_name, attr_value in config.get('attributes', {}).items():
        processed_content = apply_attribute_substitution(processed_content, attr_name, attr_value)
    
    # Also fetch attributes from BQ table
    bq_attributes = bq_client.query(f"SELECT attribute_name, attribute_value FROM `{PROJECT_ID}.{DATASET_ID}.attributes`").result()
    for row in bq_attributes:
        processed_content = apply_attribute_substitution(processed_content, row.attribute_name, row.attribute_value)

    # 3. Apply list substitutions
    for list_name, list_elements in config.get('lists', {}).items():
        processed_content = apply_list_substitution(processed_content, list_name, list_elements)
    
    # Also fetch lists from BQ table
    bq_list_names_query = bq_client.query(f"SELECT DISTINCT list_name FROM `{PROJECT_ID}.{DATASET_ID}.list_data`").result()
    for row in bq_list_names_query:
        list_elements = fetch_list_elements(row.list_name)
        processed_content = apply_list_substitution(processed_content, row.list_name, list_elements)

    # 4. Apply timestamp substitutions
    processed_content = apply_timestamp_substitutions(processed_content)

    # 5. Remove comments and trim (equivalent to sed/perl in ksh)
    processed_content = re.sub(r"^\s*#.*$", "", processed_content, flags=re.MULTILINE) # Remove full-line comments
    processed_content = re.sub(r"\s*#.*$", "", processed_content) # Remove inline comments
    processed_content = re.sub(r"^\s+|\s+$", "", processed_content, flags=re.MULTILINE) # Trim leading/trailing whitespace per line
    processed_content = re.sub(r"\n{2,}", "\n", processed_content) # Reduce multiple newlines

    return processed_content.strip() # Final trim

# --- Example Usage (for local testing or Cloud Function trigger) ---
if __name__ == '__main__':
    # This is a mock configuration for demonstration purposes.
    # In a real Cloud Composer DAG, these values would come from XComs, DAG parameters, or environment variables.
    example_config = {
        "attributes": {
            "REPORT_DATE": "20231027",
            "SCHEMA_NAME": "my_dataset"
        },
        "lists": {
            "TABLE_LIST": ["table_a", "table_b", "table_c"],
            "SINGLE_ITEM_LIST": ["only_item"]
        }
    }

    # Populate BigQuery tables with some example data for testing
    # In a real scenario, this data would be managed by other processes.
    print("Populating example BigQuery data...")
    try:
        bq_client.query(f"""
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.templates` (template_name, template_content) VALUES
            ('main_template.sql', E'-- Main SQL Template\\nSELECT <REPORT_DATE> AS report_date,\\n<FIRST> FROM <SCHEMA_NAME>.<FIRST_TABLE>\\n<MIDDLE> UNION ALL\\n<MIDDLE> FROM <SCHEMA_NAME>.<MIDDLE_TABLE>\\n<END> UNION ALL\\n<END> FROM <SCHEMA_NAME>.<END_TABLE>\\nWHERE event_date = \\'<SYSDATE:-1d:%Y-%m-%d>\\'\\n<NODE INCLUDE header_node_include>\\n-- End Main Template'),
            ('header_node_include', E'-- Header comments from an include file\\n-- Generated on <SYSDATE:%Y%m%d_%H%M%S>'),
            ('another_node_content', E'-- This is content from another node.'),
            ('single_list_template.sql', E'SELECT * FROM <SINGLE_ITEM_LIST>_table WHERE dt = \\'<SYSDATE:-7d:%Y%m%d>\\' # inline comment')
        """).result()

        bq_client.query(f"""
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.list_data` (list_name, element_order, element_value) VALUES
            ('TABLE_LIST_BQ', 1, 'bq_table_x'),
            ('TABLE_LIST_BQ', 2, 'bq_table_y'),
            ('SINGLE_BQ_LIST', 1, 'bq_single_table')
        """).result()

        bq_client.query(f"""
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.attributes` (attribute_name, attribute_value) VALUES
            ('FIRST_TABLE', 'my_first_table'),
            ('MIDDLE_TABLE', 'my_middle_table'),
            ('END_TABLE', 'my_end_table'),
            ('ANOTHER_ATTR', 'some_value_from_bq')
        """).result()
        print("Example data populated.")
    except Exception as e:
        print(f"Error populating BQ data (might already exist): {e}")

    print("\n--- Processing main_template.sql ---")
    output_sql = parse_template("main_template.sql", example_config)
    print(output_sql)

    print("\n--- Processing single_list_template.sql ---")
    output_single_sql = parse_template("single_list_template.sql", example_config)
    print(output_single_sql)

    print("\n--- Example of calling BigQuery Stored Procedures ---")
    # This demonstrates how the Python orchestrator might call BQ SPs for isolated logic
    # In practice, much of the Python logic above might be pushed into BQ SPs if feasible.

    test_template_bq_sp = "Initial text with <ATTRIBUTE_TO_REPLACE> and <BQ_SINGLE_LIST>."
    print(f"\nOriginal text for SP demo: {test_template_bq_sp}")

    # Call the attribute replacement SP
    bq_client.query(f"""
        CALL `{PROJECT_ID}.{DATASET_ID}.fill_attribute_placeholder`('{test_template_bq_sp}', '<ATTRIBUTE_TO_REPLACE>', 'ReplacedValue');
    """).result() # This call needs to store the result in a temp table or use a more complex UDF structure for INOUT params

    # Note: Direct INOUT parameter manipulation in a simple Python call like this is not
    # straightforward with BigQuery Client Library. A more robust approach for INOUT
    # would involve a temp table or passing the value back and forth.
    # For demonstration, let's assume we would get the modified value back.
    # Alternatively, the Python script would perform all string manipulations.
    print(f"\nAttribute replacement would occur within the BQ SP, e.g., if we modify a column in a temp table.")

    # Demonstrating the UDF directly
    result_ts_udf = bq_client.query(f"""
        SELECT `{PROJECT_ID}.{DATASET_ID}.format_timestamp_with_offset`('%Y-%m-%d %H:%M:%S', CURRENT_TIMESTAMP(), '1 DAY') AS tomorrow_ts;
    """).result()
    for row in result_ts_udf:
        print(f"Timestamp UDF Example (Tomorrow): {row.tomorrow_ts}")

    result_ts_udf_yesterday = bq_client.query(f"""
        SELECT `{PROJECT_ID}.{DATASET_ID}.format_timestamp_with_offset`('%Y%m%d', CURRENT_DATE(), '-1 DAY') AS yesterday_date;
    """).result()
    for row in result_ts_udf_yesterday:
        print(f"Timestamp UDF Example (Yesterday): {row.yesterday_date}")

    print("\n--- End of Orchestrator Script ---")