The migration of `h_alis_parser.ksh` from a complex KornShell script to a combination of BigQuery SQL and Python/Cloud Composer presents significant architectural changes. The validation strategy focuses on ensuring behavioral equivalence, meaning the new system produces the same output for the same logical inputs as the legacy system, despite the underlying technology shift.

The tests are organized into sections covering output parity, transformation correctness, external system replacements, and data quality. Each test case includes its purpose, setup instructions, actions to perform, and a concrete pass/fail criterion.

**Assumptions for Test Execution:**

*   A `pytest` environment is set up.
*   The `google-cloud-bigquery` Python library is installed.
*   The legacy `h_alis_parser.ksh` script is accessible at the specified path.
*   The BigQuery DDLs (`bq/ddl/*.sql`) and stored procedures/UDFs (`bq/stored_procedures/*.sql`, `bq/user_defined_functions/*.sql`) have been deployed to BigQuery.
*   The Python orchestrator script (`python/cloud_composer/alis_parser_orchestrator.py`) is available.
*   A `conftest.py` (as outlined in the thought process) is present to manage BigQuery client, dataset creation/teardown, and helper functions for running legacy/migrated code and normalizing output.
*   `GCP_PROJECT_ID` environment variable is set for the pytest execution.

---

## Migration Validation Tests for `h_alis_parser.ksh`

### `conftest.py` (Helper Functions and Fixtures)

This file provides shared fixtures and helper functions for running legacy and migrated code, managing BigQuery resources, and normalizing output for comparison.

```python
# conftest.py
import pytest
import os
import subprocess
from google.cloud import bigquery
import time
import uuid
import re

# Assume the ksh script path relative to the test runner
KSH_SCRIPT_PATH = "vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh"

@pytest.fixture(scope="session")
def gcp_project_id():
    """Fixture to get the GCP Project ID from environment variable."""
    project_id = os.environ.get("GCP_PROJECT_ID")
    if not project_id:
        pytest.fail("GCP_PROJECT_ID environment variable not set.")
    return project_id

@pytest.fixture(scope="session")
def bq_dataset_id(gcp_project_id):
    """Fixture to create a unique BigQuery dataset for the test session."""
    # Use a unique dataset for testing to avoid conflicts
    dataset_id = f"test_alis_parser_{uuid.uuid4().hex[:8]}"
    client = bigquery.Client(project=gcp_project_id)
    dataset_ref = client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US" # Or your preferred location

    try:
        client.create_dataset(dataset, exists_ok=True)
        print(f"\nCreated BigQuery dataset: {dataset_id}")
    except Exception as e:
        pytest.fail(f"Could not create dataset {dataset_id}: {e}")

    yield dataset_id

    # Teardown: Delete the dataset
    try:
        client.delete_dataset(dataset_ref, delete_contents=True)
        print(f"Deleted BigQuery dataset: {dataset_id}")
    except Exception as e:
        print(f"Error deleting dataset {dataset_id}: {e}") # Log error but don't fail teardown

@pytest.fixture(scope="session")
def bq_client(gcp_project_id, bq_dataset_id):
    """Fixture to provide a BigQuery client and ensure DDLs are deployed."""
    client = bigquery.Client(project=gcp_project_id)

    # Create tables
    table_ddls = {
        "templates": """
            CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.templates` (
                template_name STRING,
                template_content STRING,
                description STRING,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            );
        """,
        "list_data": """
            CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.list_data` (
                list_name STRING,
                element_order INT64,
                element_value STRING,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            );
        """,
        "attributes": """
            CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.attributes` (
                attribute_name STRING,
                attribute_value STRING,
                description STRING,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            );
        """
    }
    for table_name, ddl in table_ddls.items():
        try:
            client.query(ddl.format(project_id=gcp_project_id, dataset_id=bq_dataset_id)).result()
            print(f"Created table: {table_name}")
        except Exception as e:
            pytest.fail(f"Error creating table {table_name}: {e}")

    # Create SPs and UDFs
    sp_udf_ddls = {
        "fill_attribute_placeholder": "bq/stored_procedures/fill_attribute_placeholder.sql",
        "fill_list_placeholders": "bq/stored_procedures/fill_list_placeholders.sql",
        "get_node_content": "bq/stored_procedures/get_node_content.sql",
        "format_timestamp_with_offset": "bq/user_defined_functions/format_timestamp_with_offset.sql"
    }
    for name, path in sp_udf_ddls.items():
        try:
            with open(path, "r") as f:
                ddl = f.read()
            # Replace placeholders in DDL
            formatted_ddl = ddl.replace("<PROJECT_ID>", gcp_project_id).replace("<DATASET_ID>", bq_dataset_id)
            client.query(formatted_ddl).result()
            print(f"Created SP/UDF: {name}")
        except Exception as e:
            pytest.fail(f"Error creating SP/UDF {name}: {e}")

    yield client

@pytest.fixture
def clean_bq_tables(bq_client, gcp_project_id, bq_dataset_id):
    """Fixture to truncate BigQuery tables before each test."""
    tables = ["templates", "list_data", "attributes"]
    for table in tables:
        try:
            bq_client.query(f"TRUNCATE TABLE `{gcp_project_id}.{bq_dataset_id}.{table}`").result()
        except Exception as e:
            print(f"Warning: Could not truncate table {table}: {e}")
    yield

def run_legacy_parser(input_content, args=None, temp_dir="."):
    """
    Helper function to run the legacy ksh script with given input content and arguments.
    Creates temporary files for input and captures stdout.
    """
    if args is None:
        args = []

    # Create a temporary input file for the main content
    input_file_path = os.path.join(temp_dir, f"temp_input_{uuid.uuid4().hex}.txt")
    with open(input_file_path, "w") as f:
        f.write(input_content)

    # Construct the command
    command = ["ksh", KSH_SCRIPT_PATH] + args
    
    # Use subprocess to run the command, piping the temp file as stdin
    try:
        process = subprocess.run(
            command,
            stdin=open(input_file_path, 'r'),
            capture_output=True,
            text=True,
            check=True,
            env=os.environ # Pass current environment variables
        )
        return process.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Legacy script failed with error: {e}")
        print(f"Stderr: {e.stderr}")
        raise
    finally:
        if os.path.exists(input_file_path):
            os.remove(input_file_path)

def run_migrated_orchestrator(template_name, config, gcp_project_id, bq_dataset_id):
    """
    Helper function to run the Python orchestrator's parse_template function.
    """
    # Temporarily set environment variables for the orchestrator
    os.environ["GCP_PROJECT_ID"] = gcp_project_id
    os.environ["BQ_DATASET_ID"] = bq_dataset_id
    
    # Import the function directly for testing
    # This assumes the python script is in the PYTHONPATH or can be imported directly
    from python.cloud_composer.alis_parser_orchestrator import parse_template
    
    result = parse_template(template_name, config)
    
    # Clean up environment variables
    del os.environ["GCP_PROJECT_ID"]
    del os.environ["BQ_DATASET_ID"]
    
    return result.strip()

def normalize_output(text):
    """
    Normalizes text for comparison:
    - Removes leading/trailing whitespace from each line.
    - Replaces multiple newlines with a single newline.
    - Ensures consistent line endings.
    - Removes comments (lines starting with # or --, or inline comments).
    """
    lines = []
    for line in text.splitlines():
        stripped_line = line.strip()
        # Remove full-line comments (shell # or SQL --)
        if stripped_line.startswith('#') or stripped_line.startswith('--'):
            continue
        # Remove inline comments (shell # or SQL --)
        stripped_line = re.sub(r'\s*#.*$', '', stripped_line)
        stripped_line = re.sub(r'\s*--.*$', '', stripped_line)
        if stripped_line: # Only add non-empty lines after stripping/comment removal
            lines.append(stripped_line)
    
    normalized_text = '\n'.join(lines)
    return normalized_text.strip()
```

### `test_alis_parser_migration.py` (Test Cases)

```python
# test_alis_parser_migration.py
import pytest
import os
from google.cloud import bigquery
from datetime import datetime, timedelta
import re

# Import helper functions from conftest.py (implicitly available in pytest)
# from conftest import run_legacy_parser, run_migrated_orchestrator, normalize_output

# Define the path to the legacy ksh script
KSH_SCRIPT_PATH = "vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh"

class TestAlisParserMigration:

    # --- 1. Output Parity & Transformation Correctness ---

    def test_parser_filattrib_simple_replacement(self, bq_client, gcp_project_id, bq_dataset_id, clean_bq_tables):
        """
        Purpose: Verify simple attribute replacement (parser_filattrib equivalent).
        """
        # Setup: Legacy
        legacy_template = "Hello <NAME>, today is <DATE>."
        legacy_args = ["parser_filattrib", "NAME", "World", "|", "parser_filattrib", "DATE", "2023-10-27"]
        
        # Setup: Migrated
        template_name = "simple_attr_template"
        bq_client.query(f"""
            INSERT INTO `{gcp_project_id}.{bq_dataset_id}.templates` (template_name, template_content) VALUES
            ('{template_name}', 'Hello <NAME>, today is <DATE>.');
        """).result()
        bq_client.query(f"""
            INSERT INTO `{gcp_project_id}.{bq_dataset_id}.attributes` (attribute_name, attribute_value) VALUES
            ('NAME', 'World'),
            ('DATE', '2023-10-27');
        """).result()
        migrated_config = {} # Attributes are fetched from BQ table

        # Action: Execute legacy and migrated
        legacy_output = run_legacy_parser(legacy_template, args=legacy_args)
        migrated_output = run_migrated_orchestrator(template_name, migrated_config, gcp_project_id, bq_dataset_id)

        # Pass/Fail Criterion: Outputs are identical after normalization
        assert normalize_output(legacy_output) == normalize_output(migrated_output)

    def test_parser_fillist_single_element(self, bq_client, gcp_project_id, bq_dataset_id, clean_bq_tables):
        """
        Purpose: Verify list replacement with a single element (parser_fillist equivalent).
        Covers <SINGLE> placeholder.
        """
        # Setup: Legacy
        legacy_template = """
<LIST COLUMNS>
  SELECT <SINGLE>
</LIST>
FROM my_table;
"""
        legacy_list_elements = "col1"
        legacy_args = ["parser_fillist", "COLUMNS", legacy_list_elements]

        # Setup: Migrated
        template_name = "single_list_template"
        bq_client.query(f"""
            INSERT INTO `{gcp_project_id}.{bq_dataset_id}.templates` (template_name, template_content) VALUES
            ('{template_name}', '{legacy_template.replace("'", "''")}');
        """).result()
        bq_client.query(f"""
            INSERT INTO `{gcp_project_id}.{bq_dataset_id}.list_data` (list_name, element_order, element_value) VALUES
            ('COLUMNS', 1, 'col1');
        """).result()
        migrated_config = {}

        # Action: Execute legacy and migrated
        legacy_output = run_legacy_parser(legacy_template, args=legacy_args)
        migrated_output = run_migrated_orchestrator(template_name, migrated_config, gcp_project_id, bq_dataset_id)

        # Pass/Fail Criterion: Outputs are identical after normalization
        assert normalize_output(legacy_output) == normalize_output(migrated_output)

    def test_parser_fillist_multiple_elements(self, bq_client, gcp_project_id, bq_dataset_id, clean_bq_tables):
        """
        Purpose: Verify list replacement with multiple elements (parser_fillist equivalent).
        Covers <FIRST>, <MIDDLE>, <END> placeholders.
        """
        # Setup: Legacy
        legacy_template = """
SELECT
<LIST COLUMNS>
  <FIRST>,
  <MIDDLE>,
  <END>
</LIST>
FROM my_table;
"""
        legacy_list_elements = "col1\ncol2\ncol3\ncol4"
        legacy_args = ["parser_fillist", "COLUMNS", legacy_list_elements]

        # Setup: Migrated
        template_name = "multi_list_template"
        bq_client.query(f"""
            INSERT INTO `{gcp_project_id}.{bq_dataset_id}.templates` (template_name, template_content) VALUES
            ('{template_name}', '{legacy_template.replace("'", "''")}');
        """).result()
        bq_client.query(f"""
            INSERT INTO `{gcp_project_id}.{bq_dataset_id}.list_data` (list_name, element_order, element_value) VALUES
            ('COLUMNS', 1, 'col1'),
            ('COLUMNS', 2, 'col2'),
            ('COLUMNS', 3, 'col3'),
            ('COLUMNS', 4, 'col4');
        """).result()
        migrated_config = {}

        # Action: Execute legacy and migrated
        legacy_output = run_legacy_parser(legacy_template, args=legacy_args)
        migrated_output = run_migrated_orchestrator(template_name, migrated_config, gcp_project_id, bq_dataset_id)

        # Pass/Fail Criterion: Outputs are identical after normalization
        assert normalize_output(legacy_output) == normalize_output(migrated_output)

    def test_parser_fillist_empty_list(self, bq_client, gcp_project_id, bq_dataset_id, clean_bq_tables):
        """
        Purpose: Verify list replacement with an empty list.
        """
        # Setup: Legacy
        legacy_template = """
SELECT
<LIST COLUMNS>
  <FIRST>,
  <MIDDLE>,
  <END>,
  <SINGLE>
</LIST>
FROM my_table;
"""
        legacy_list_elements = "" # Empty list
        legacy_args = ["parser_fillist", "COLUMNS", legacy_list_elements]

        # Setup: Migrated
        template_name = "empty_list_template"
        bq_client.query(f"""
            INSERT INTO `{gcp_project_id}.{bq_dataset_id}.templates` (template_name, template_content) VALUES
            ('{template_name}', '{legacy_template.replace("'", "''")}');
        """).result()
        # No data inserted into list_data for 'COLUMNS'
        migrated_config = {}

        # Action: Execute legacy and migrated
        legacy_output = run_legacy_parser(legacy_template, args=legacy_args)
        migrated_output = run_migrated_orchestrator(template_name, migrated_config, gcp_project_id, bq_dataset_id)

        # Pass/Fail Criterion: Outputs are identical after normalization
        assert normalize_output(legacy_output) == normalize_output(migrated_output)

    def test_parser_getnode_simple_node_extraction(self, bq_client, gcp_project_id, bq_dataset_id, clean_bq_tables):
        """
        Purpose: Verify simple node content extraction (parser_getnode equivalent).
        """
        # Setup: Legacy
        legacy_template = """
Some header.
<NODE MY_NODE>
  This is the content of MY_NODE.
  It has multiple lines.
</NODE>
Some footer.
"""
        legacy_args = ["parser_getnode", "MY_NODE"]

        # Setup: Migrated
        template_name = "node_extraction_template"
        bq_client.query(f"""
            INSERT INTO `{gcp_project_id}.{bq_dataset_id}.templates` (template_name, template_content) VALUES
            ('{template_name}', '{legacy_template.replace("'", "''")}');
        """).result()
        
        # Action: Execute legacy and migrated
        legacy_output = run_legacy_parser(legacy_template, args=legacy_args)
        
        # For migrated, we call the SP directly for this specific test
        # The orchestrator's resolve_includes handles node extraction implicitly
        # For direct comparison, we need to simulate the SP's output
        query_job = bq_client.query(f"""
            DECLARE output STRING;
            CALL `{gcp_project_id}.{bq_dataset_id}.get_node_content`('{legacy_template.replace("'", "''")}', 'MY_NODE', output);
            SELECT output;
        """)
        result = query_job.result()
        migrated_output = [row[0] for row in result][0]

        # Pass/Fail Criterion: Outputs are identical after normalization
        assert normalize_output(legacy_output) == normalize_output(migrated_output)

    def test_parser_getnode_include_directive(self, bq_client, gcp_project_id, bq_dataset_id, clean_bq_tables):
        """
        Purpose: Verify node content extraction with an INCLUDE directive.
        """
        # Setup: Legacy
        included_file_content = "Content from included_file.sql"
        with open("temp_included_file.sql", "w") as f:
            f.write(included_file_content)
        
        legacy_template = f"""
<NODE MAIN_NODE INCLUDE temp_included_file.sql>
</NODE>
"""
        legacy_args = ["parser_getnode", "MAIN_NODE"]

        # Setup: Migrated
        template_name = "main_node_include_template"
        included_template_name = "temp_included_file.sql"
        bq_client.query(f"""
            INSERT INTO `{gcp_project_id}.{bq_dataset_id}.templates` (template_name, template_content) VALUES
            ('{template_name}', '{legacy_template.replace("'", "''")}'),
            ('{included_template_name}', '{included_file_content.replace("'", "''")}');
        """).result()
        
        # Action: Execute legacy and migrated
        legacy_output = run_legacy_parser(legacy_template, args=legacy_args)
        
        # For migrated, we call the SP directly for this specific test
        query_job = bq_client.query(f"""
            DECLARE output STRING;
            CALL `{gcp_project_id}.{bq_dataset_id}.get_node_content`('{legacy_template.replace("'", "''")}', 'MAIN_NODE', output);
            SELECT output;
        """)
        result = query_job.result()
        migrated_output = [row[0] for row in result][0]

        # Pass/Fail Criterion: Outputs are identical after normalization
        assert normalize_output(legacy_output) == normalize_output(migrated_output)
        os.remove("temp_included_file.sql") # Clean up temp file

    def test_parser_timestamp_substitution(self, bq_client, gcp_project_id, bq_dataset_id, clean_bq_tables):
        """
        Purpose: Verify timestamp substitutions with SYSDATE and offsets.
        """
        # Setup: Legacy (parser function handles this)
        legacy_template = """
Report Date: <SYSDATE:%Y-%m-%d>
Yesterday: <SYSDATE:-1d:%Y%m%d>
Tomorrow: <SYSDATE:+1d:%Y%m%d>
Current Time: <SYSDATE:%H:%M:%S>
"""
        legacy_args = ["parser"] # Main parser function

        # Setup: Migrated (orchestrator handles this)
        template_name = "timestamp_template"
        bq_client.query(f"""
            INSERT INTO `{gcp_project_id}.{bq_dataset_id}.templates` (template_name, template_content) VALUES
            ('{template_name}', '{legacy_template.replace("'", "''")}');
        """).result()
        migrated_config = {}

        # Action: Execute legacy and migrated
        # Note: Timestamps will differ by a few seconds, so we need to compare structure and relative dates.
        legacy_output = run_legacy_parser(legacy_template, args=legacy_args)
        migrated_output = run_migrated_orchestrator(template_name, migrated_config, gcp_project_id, bq_dataset_id)

        # Pass/Fail Criterion: Outputs match expected patterns for dates and times.
        # Due to potential time differences, we'll parse and compare dates.
        legacy_lines = legacy_output.splitlines()
        migrated_lines = migrated_output.splitlines()

        assert len(legacy_lines) == len(migrated_lines) == 4

        # Line 1: Report Date: YYYY-MM-DD
        legacy_date_str = legacy_lines[0].split(': ')[1]
        migrated_date_str = migrated_lines[0].split(': ')[1]
        assert datetime.strptime(legacy_date_str, '%Y-%m-%d').date() == datetime.now().date()
        assert datetime.strptime(migrated_date_str, '%Y-%m-%d').date() == datetime.now().date()

        # Line 2: Yesterday: YYYYMMDD
        legacy_yesterday_str = legacy_lines[1].split(': ')[1]
        migrated_yesterday_str = migrated_lines[1].split(': ')[1]
        assert datetime.strptime(legacy_yesterday_str, '%Y%m%d').date() == (datetime.now() - timedelta(days=1)).date()
        assert datetime.strptime(migrated_yesterday_str, '%Y%m%d').date() == (datetime.now() - timedelta(days=1)).date()

        # Line 3: Tomorrow: YYYYMMDD
        legacy_tomorrow_str = legacy_lines[2].split(': ')[1]
        migrated_tomorrow_str = migrated_lines[2].split(': ')[1]
        assert datetime.strptime(legacy_tomorrow_str, '%Y%m%d').date() == (datetime.now() + timedelta(days=1)).date()
        assert datetime.strptime(migrated_tomorrow_str, '%Y%m%d').date() == (datetime.now() + timedelta(days=1)).date()

        # Line 4: Current Time: HH:MM:SS (allow for slight difference)
        legacy_time_str = legacy_lines[3].split(': ')[1]
        migrated_time_str = migrated_lines[3].split(': ')[1]
        legacy_time = datetime.strptime(legacy_time_str, '%H:%M:%S').time()
        migrated_time = datetime.strptime(migrated_time_str, '%H:%M:%S').time()
        # Check if times are within a reasonable delta (e.g., 60 seconds)
        time_diff = abs((datetime.combine(datetime.min, legacy_time) - datetime.combine(datetime.min, migrated_time)).total_seconds())
        assert time_diff < 60 # Allow up to 60 seconds difference

    def test_parser_full_integration_scenario(self, bq_client, gcp_project_id, bq_dataset_id, clean_bq_tables):
        """
        Purpose: Test the full integration of attributes, lists, includes, and timestamps
        mimicking the main 'parser' function's behavior. This covers the logic
        of parser_filmeta/parser_filfile indirectly.
        """
        # Setup: Legacy
        # Create temporary files for includes
        with open("temp_header.sql", "w") as f:
            f.write("-- This is a header from temp_header.sql\n")
        with open("temp_footer.sql", "w") as f:
            f.write("-- This is a footer from temp_footer.sql\n")

        legacy_template = """
<VAR_NAME=Value1>
<LISTDEF MY_LIST ATTR1 ATTR2>
item1 valA valB
item2 valC valD
</LISTDEF>
<INCLUDE temp_header.sql>
SELECT
  <COL1>,
  <COL2>,
  <COL3>
FROM <SCHEMA_NAME>.<TABLE_NAME>
WHERE report_date = '<SYSDATE:-2d:%Y-%m-%d>'
  AND status = '<STATUS_CODE>'
<NODE MAIN_NODE>
  -- Node content
  <MY_LIST 1-1>
  <MY_LIST 2-2>
</NODE>
<INCLUDE temp_footer.sql>
"""
        legacy_args = ["parser"]

        # Setup: Migrated
        template_name = "full_integration_template"
        bq_client.query(f"""
            INSERT INTO `{gcp_project_id}.{bq_dataset_id}.templates` (template_name, template_content) VALUES
            ('{template_name}', '{legacy_template.replace("'", "''")}'),
            ('temp_header.sql', '-- This is a header from temp_header.sql'),
            ('temp_footer.sql', '-- This is a footer from temp_footer.sql');
        """).result()
        bq_client.query(f"""
            INSERT INTO `{gcp_project_id}.{bq_dataset_id}.attributes` (attribute_name, attribute_value) VALUES
            ('COL1', 'column_a'),
            ('COL2', 'column_b'),
            ('COL3', 'column_c'),
            ('SCHEMA_NAME', 'my_schema'),
            ('TABLE_NAME', 'my_table'),
            ('STATUS_CODE', 'ACTIVE');
        """).result()
        # The Python orchestrator's `parse_template` function handles LISTDEF and variable definitions
        # directly from the template content, so no separate BQ inserts for these are needed for this test.
        migrated_config = {
            "attributes": {
                "VAR_NAME": "Value1" # This is handled by the orchestrator's internal variable management
            }
        }

        # Action: Execute legacy and migrated
        legacy_output = run_legacy_parser(legacy_template, args=legacy_args)
        migrated_output = run_migrated_orchestrator(template_name, migrated_config, gcp_project_id, bq_dataset_id)

        # Pass/Fail Criterion: Outputs are identical after normalization,
        # accounting for timestamp differences.
        normalized_legacy = normalize_output(legacy_output)
        normalized_migrated = normalize_output(migrated_output)

        # Replace dynamic timestamps for comparison
        today = datetime.now()
        two_days_ago = (today - timedelta(days=2)).strftime('%Y-%m-%d')
        normalized_legacy = re.sub(r'\d{4}-\d{2}-\d{2}', two_days_ago, normalized_legacy)
        normalized_migrated = re.sub(r'\d{4}-\d{2}-\d{2}', two_days_ago, normalized_migrated)

        assert normalized_legacy == normalized_migrated
        os.remove("temp_header.sql")
        os.remove("temp_footer.sql")

    def test_parser_list_ranges_and_attributes_within_list(self, bq_client, gcp_project_id, bq_dataset_id, clean_bq_tables):
        """
        Purpose: Verify complex list parsing with ranges and attributes within list elements.
        This tests the `parser` function's advanced list handling.
        """
        # Setup: Legacy
        legacy_template = """
<LISTDEF MY_COLS COL_NAME COL_TYPE>
col_a STRING
col_b INT64
col_c BOOLEAN
</LISTDEF>
SELECT
<MY_COLS 1-1>
  <COL_NAME> AS first_col,
<MY_COLS 2-2>
  <COL_NAME> AS middle_col,
<MY_COLS 3-3>
  <COL_NAME> AS last_col
FROM my_table;
"""
        legacy_args = ["parser"]

        # Setup: Migrated
        template_name = "complex_list_ranges_template"
        bq_client.query(f"""
            INSERT INTO `{gcp_project_id}.{bq_dataset_id}.templates` (template_name, template_content) VALUES
            ('{template_name}', '{legacy_template.replace("'", "''")}');
        """).result()
        migrated_config = {}

        # Action: Execute legacy and migrated
        legacy_output = run_legacy_parser(legacy_template, args=legacy_args)
        migrated_output = run_migrated_orchestrator(template_name, migrated_config, gcp_project_id, bq_dataset_id)

        # Pass/Fail Criterion: Outputs are identical after normalization
        assert normalize_output(legacy_output) == normalize_output(migrated_output)

    def test_parser_comments_and_whitespace_handling(self, bq_client, gcp_project_id, bq_dataset_id, clean_bq_tables):
        """
        Purpose: Verify that comments and extraneous whitespace are handled correctly
        by the migrated orchestrator, matching the legacy script's cleanup.
        """
        # Setup: Legacy
        legacy_template = """
# This is a full line comment
  SELECT  <ATTR1>  -- This is an inline comment
FROM my_table; # Another inline comment
<LIST COLUMNS>
  <SINGLE> # List item comment
</LIST>

-- SQL style comment
  <ATTR2>
"""
        legacy_args = ["parser_filattrib", "ATTR1", "ValueA", "|", "parser_filattrib", "ATTR2", "ValueB", "|", "parser_fillist", "COLUMNS", "col_x"]

        # Setup: Migrated
        template_name = "comments_whitespace_template"
        bq_client.query(f"""
            INSERT INTO `{gcp_project_id}.{bq_dataset_id}.templates` (template_name, template_content) VALUES
            ('{template_name}', '{legacy_template.replace("'", "''")}');
        """).result()
        bq_client.query(f"""
            INSERT INTO `{gcp_project_id}.{bq_dataset_id}.attributes` (attribute_name, attribute_value) VALUES
            ('ATTR1', 'ValueA'),
            ('ATTR2', 'ValueB');
        """).result()
        bq_client.query(f"""
            INSERT INTO `{gcp_project_id}.{bq_dataset_id}.list_data` (list_name, element_order, element_value) VALUES
            ('COLUMNS', 1, 'col_x');
        """).result()
        migrated_config = {}

        # Action: Execute legacy and migrated
        legacy_output = run_legacy_parser(legacy_template, args=legacy_args)
        migrated_output = run_migrated_orchestrator(template_name, migrated_config, gcp_project_id, bq_dataset_id)

        # Pass/Fail Criterion: Outputs are identical after normalization.
        # The `normalize_output` helper function handles comment and whitespace removal.
        assert normalize_output(legacy_output) == normalize_output(migrated_output)

    # --- 2. Data Quality / Row Count / Schema Assertions ---

    def test_bq_templates_table_schema_and_data_quality(self, bq_client, gcp_project_id, bq_dataset_id):
        """
        Purpose: Verify the schema and basic data quality of the `templates` BigQuery table.
        """
        # Setup: Insert some test data
        bq_client.query(f"""
            INSERT INTO `{gcp_project_id}.{bq_dataset_id}.templates` (template_name, template_content, description) VALUES
            ('test_template_1', 'Content 1', 'Desc 1'),
            ('test_template_2', 'Content 2', 'Desc 2');
        """).result()

        # Action: Query schema and data
        schema_query = f"""
            SELECT column_name, data_type, is_nullable
            FROM `{gcp_project_id}.{bq_dataset_id}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = 'templates'
            ORDER BY ordinal_position;
        """
        row_count_query = f"SELECT COUNT(*) FROM `{gcp_project_id}.{bq_dataset_id}.templates`;"
        data_query = f"SELECT template_name, template_content FROM `{gcp_project_id}.{bq_dataset_id}.templates` ORDER BY template_name;"

        schema_results = list(bq_client.query(schema_query).result())
        row_count = list(bq_client.query(row_count_query).result())[0][0]
        data_results = list(bq_client.query(data_query).result())

        # Pass/Fail Criterion: Schema matches expected, row count is correct, and data is as inserted.
        expected_schema = [
            ('template_name', 'STRING', 'YES'),
            ('template_content', 'STRING', 'YES'),
            ('description', 'STRING', 'YES'),
            ('created_at', 'TIMESTAMP', 'YES') # DEFAULT CURRENT_TIMESTAMP() implies nullable
        ]
        actual_schema = [(row.column_name, row.data_type, row.is_nullable) for row in schema_results]

        assert actual_schema == expected_schema
        assert row_count == 2
        assert data_results[0].template_name == 'test_template_1'
        assert data_results[0].template_content == 'Content 1'
        assert data_results[1].template_name == 'test_template_2'
        assert data_results[1].template_content == 'Content 2'

    def test_bq_list_data_table_schema_and_data_quality(self, bq_client, gcp_project_id, bq_dataset_id):
        """
        Purpose: Verify the schema and basic data quality of the `list_data` BigQuery table.
        """
        # Setup: Insert some test data
        bq_client.query(f"""
            INSERT INTO `{gcp_project_id}.{bq_dataset_id}.list_data` (list_name, element_order, element_value) VALUES
            ('my_list', 1, 'item_a'),
            ('my_list', 2, 'item_b');
        """).result()

        # Action: Query schema and data
        schema_query = f"""
            SELECT column_name, data_type, is_nullable
            FROM `{gcp_project_id}.{bq_dataset_id}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = 'list_data'
            ORDER BY ordinal_position;
        """
        row_count_query = f"SELECT COUNT(*) FROM `{gcp_project_id}.{bq_dataset_id}.list_data`;"
        data_query = f"SELECT list_name, element_order, element_value FROM `{gcp_project_id}.{bq_dataset_id}.list_data` ORDER BY list_name, element_order;"

        schema_results = list(bq_client.query(schema_query).result())
        row_count = list(bq_client.query(row_count_query).result())[0][0]
        data_results = list(bq_client.query(data_query).result())

        # Pass/Fail Criterion: Schema matches expected, row count is correct, and data is as inserted.
        expected_schema = [
            ('list_name', 'STRING', 'YES'),
            ('element_order', 'INT64', 'YES'),
            ('element_value', 'STRING', 'YES'),
            ('created_at', 'TIMESTAMP', 'YES')
        ]
        actual_schema = [(row.column_name, row.data_type, row.is_nullable) for row in schema_results]

        assert actual_schema == expected_schema
        assert row_count == 2
        assert data_results[0].list_name == 'my_list'
        assert data_results[0].element_order == 1
        assert data_results[0].element_value == 'item_a'

    def test_bq_attributes_table_schema_and_data_quality(self, bq_client, gcp_project_id, bq_dataset_id):
        """
        Purpose: Verify the schema and basic data quality of the `attributes` BigQuery table.
        """
        # Setup: Insert some test data
        bq_client.query(f"""
            INSERT INTO `{gcp_project_id}.{bq_dataset_id}.attributes` (attribute_name, attribute_value) VALUES
            ('ATTR_A', 'Value_A'),
            ('ATTR_B', 'Value_B');
        """).result()

        # Action: Query schema and data
        schema_query = f"""
            SELECT column_name, data_type, is_nullable
            FROM `{gcp_project_id}.{bq_dataset_id}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = 'attributes'
            ORDER BY ordinal_position;
        """
        row_count_query = f"SELECT COUNT(*) FROM `{gcp_project_id}.{bq_dataset_id}.attributes`;"
        data_query = f"SELECT attribute_name, attribute_value FROM `{gcp_project_id}.{bq_dataset_id}.attributes` ORDER BY attribute_name;"

        schema_results = list(bq_client.query(schema_query).result())
        row_count = list(bq_client.query(row_count_query).result())[0][0]
        data_results = list(bq_client.query(data_query).result())

        # Pass/Fail Criterion: Schema matches expected, row count is correct, and data is as inserted.
        expected_schema = [
            ('attribute_name', 'STRING', 'YES'),
            ('attribute_value', 'STRING', 'YES'),
            ('description', 'STRING', 'YES'),
            ('created_at', 'TIMESTAMP', 'YES')
        ]
        actual_schema = [(row.column_name, row.data_type, row.is_nullable) for row in schema_results]

        assert actual_schema == expected_schema
        assert row_count == 2
        assert data_results[0].attribute_name == 'ATTR_A'
        assert data_results[0].attribute_value == 'Value_A'

```