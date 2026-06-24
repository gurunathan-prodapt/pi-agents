As a senior data-migration QA engineer, I've analyzed the migration design and both the legacy and migrated code for `customer/crm_lineage_tracker.py`. The migration involves a significant re-platforming from Oracle to Google Cloud Platform (GCP), specifically BigQuery and GCS. This means direct output parity (byte-for-byte JSON comparison) is not feasible or even desirable, as the underlying metadata sources and the conceptual "lineage chains" have been redesigned for the GCP ecosystem.

Therefore, the validation tests will focus on:
1.  **Semantic Equivalence:** Ensuring the *intent* of lineage tracking is maintained, and that equivalent information is captured in the new GCP context.
2.  **Correctness of GCP Implementations:** Verifying that the BigQuery Information Schema queries, IAM interpretations, custom BigQuery audit table reads, and GCS writes function as expected.
3.  **Transformation Logic:** Validating the new `build_lineage_graph` logic correctly constructs a graph from GCP metadata.
4.  **Data Quality & Completeness:** Asserting that the extracted data is accurate, complete, and the output structure is consistent.

For runnable test code, I will use `pytest` with `unittest.mock` to simulate GCP client interactions, and provide example SQL assertions for BigQuery data setup.

---

## Migration Validation Tests for `customer/crm_lineage_tracker.py`

### Test Environment Setup (Pre-requisites)

To run these tests, a dedicated GCP project and resources are required:
*   **GCP Project:** `test-gcp-project-id` (replace with actual)
*   **BigQuery Datasets:**
    *   `test_crm_dataset`
    *   `test_sales_dataset`
    *   `test_finance_dataset`
    *   `test_audit_dataset` (for `etl_job_audit` table)
*   **BigQuery Tables/Views/Routines:** Populate these datasets with sample data that mimics a real environment, including:
    *   Tables: `test_crm_dataset.customers`, `test_sales_dataset.orders`, `test_finance_dataset.transactions`
    *   Views: `test_crm_dataset.customer_summary_view` (referencing `customers`), `test_sales_dataset.daily_sales_view` (referencing `orders`)
    *   Routines: `test_crm_dataset.sp_update_customer_status` (referencing `customers`)
*   **BigQuery `etl_job_audit` Table:** Create and populate `test_audit_dataset.etl_job_audit` with test records for a specific `run_date`.
    *   **Schema:** `job_id STRING, job_name STRING, start_time TIMESTAMP, end_time TIMESTAMP, status STRING, source_table STRING, target_table STRING, workflow_name STRING`
    *   **Sample Data (for `run_date = '2023-10-26'`):**
        ```sql
        INSERT INTO `test-gcp-project-id.test_audit_dataset.etl_job_audit` (job_id, job_name, start_time, end_time, status, source_table, target_table, workflow_name) VALUES
        ('job_123', 'CustomerDataflow', '2023-10-26 10:00:00 UTC', '2023-10-26 10:15:00 UTC', 'SUCCESS', 'test-gcp-project-id.test_sales_dataset.orders', 'test-gcp-project-id.test_crm_dataset.customers', 'CRM_DAILY_LOAD'),
        ('job_124', 'SalesAggregation', '2023-10-26 11:00:00 UTC', '2023-10-26 11:05:00 UTC', 'SUCCESS', 'test-gcp-project-id.test_sales_dataset.daily_sales_view', 'test-gcp-project-id.test_finance_dataset.transactions', 'FINANCE_DAILY_AGG');
        ```
*   **Google Cloud Storage (GCS) Bucket:** `test-lineage-output-bucket` (replace with actual)
*   **Service Account:** The service account running the migrated job must have:
    *   `BigQuery Data Viewer` on all relevant datasets.
    *   `BigQuery Metadata Viewer` on the project.
    *   `Storage Object Creator` on the output GCS bucket.
    *   `BigQuery Job User` (for running queries).

---

### Test Case 1: BigQuery Information Schema - Table Extraction

*   **Purpose:** Verify that the migrated job correctly extracts metadata for base tables from BigQuery's `INFORMATION_SCHEMA.TABLES`. This replaces the legacy Oracle `DBA_DEPENDENCIES` for basic table discovery.
*   **Setup:**
    1.  Ensure the BigQuery datasets (`test_crm_dataset`, `test_sales_dataset`, `test_finance_dataset`) are populated with at least one base table each.
    2.  Configure the migrated script's `DEFAULT_GCP_PROJECT` and `DEFAULT_BIGQUERY_DATASETS` to point to these test resources.
*   **Action:**
    1.  Run the migrated `crm_lineage_tracker.py` script with appropriate arguments:
        ```bash
        python customer/crm_lineage_tracker.py \
          --gcp_projects test-gcp-project-id \
          --bigquery_datasets test_crm_dataset test_sales_dataset test_finance_dataset \
          --run_date 2023-10-26 \
          --output_gcs_bucket test-lineage-output-bucket
        ```
    2.  Download the generated JSON output from GCS.
*   **Pass/Fail Criterion:**
    *   **Pass:** The `nodes` array in the generated JSON contains entries for all expected base tables (e.g., `test_crm_dataset.customers`, `test_sales_dataset.orders`, `test_finance_dataset.transactions`). Each table node should have:
        *   `type`: "BigQuery Table"
        *   `full_path`: Correctly formatted (e.g., `test-gcp-project-id.test_crm_dataset.customers`)
        *   `domain`: Correctly mapped (e.g., "customer", "sales", "finance")
        *   `attributes`: Contains `table_catalog`, `table_schema`, `table_name`, `table_type`, `creation_time`, `row_count`, `num_partitions`.
    *   **Fail:** Missing tables, incorrect types, incorrect `full_path` or `domain` mappings, or missing `attributes`.

*   **Example Pytest Assertion (Conceptual, after loading JSON):**
    ```python
    import json
    import pytest

    def test_table_extraction(gcs_output_json):
        # gcs_output_json is a fixture that downloads and parses the JSON output
        nodes = gcs_output_json['nodes']
        
        expected_tables = {
            "test-gcp-project-id.test_crm_dataset.customers": "customer",
            "test-gcp-project-id.test_sales_dataset.orders": "sales",
            "test-gcp-project-id.test_finance_dataset.transactions": "finance",
        }
        
        found_tables = {}
        for node in nodes:
            if node['type'] == 'BigQuery Table':
                found_tables[node['full_path']] = node['domain']
                assert 'table_catalog' in node['attributes']
                assert 'table_schema' in node['attributes']
                assert 'table_name' in node['attributes']
                assert node['attributes']['table_type'] == 'BASE TABLE'
        
        assert len(found_tables) >= len(expected_tables), "Not all expected tables were found."
        for full_path, domain in expected_tables.items():
            assert full_path in found_tables, f"Table {full_path} not found in output."
            assert found_tables[full_path] == domain, f"Domain mismatch for {full_path}."
    ```

---

### Test Case 2: BigQuery Information Schema - Views and Inferred Dependencies

*   **Purpose:** Verify that the migrated job correctly extracts metadata for views and infers dependencies from `view_definition` by parsing the SQL. This replaces the legacy Oracle `DBA_DEPENDENCIES` for view-to-table/view dependencies.
*   **Setup:**
    1.  Ensure `test_crm_dataset.customer_summary_view` is created and its `view_definition` explicitly references `test_crm_dataset.customers`.
    2.  Ensure `test_sales_dataset.daily_sales_view` is created and its `view_definition` explicitly references `test_sales_dataset.orders`.
    3.  Configure the migrated script as in Test 1.
*   **Action:**
    1.  Run the migrated `crm_lineage_tracker.py` script.
    2.  Download the generated JSON output from GCS.
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   The `nodes` array contains entries for `customer_summary_view` and `daily_sales_view` with `type`: "BigQuery View".
        *   The `edges` array contains entries representing the inferred dependencies:
            *   `source`: `BQ_TABLE:test-gcp-project-id.test_crm_dataset.customers`
            *   `target`: `BQ_VIEW:test-gcp-project-id.test_crm_dataset.customer_summary_view`
            *   `type`: "READS_FROM_VIEW_DEFINITION"
            *   Similar edge for `daily_sales_view` and `orders`.
    *   **Fail:** Missing view nodes, incorrect view types, or missing/incorrect dependency edges.

*   **Example Pytest Assertion (Conceptual, after loading JSON):**
    ```python
    def test_view_and_dependency_extraction(gcs_output_json):
        nodes = gcs_output_json['nodes']
        edges = gcs_output_json['edges']

        # Check for view nodes
        crm_view_node = next((n for n in nodes if n['name'] == 'customer_summary_view' and n['type'] == 'BigQuery View'), None)
        sales_view_node = next((n for n in nodes if n['name'] == 'daily_sales_view' and n['type'] == 'BigQuery View'), None)
        assert crm_view_node is not None, "CRM view node not found."
        assert sales_view_node is not None, "Sales view node not found."

        # Check for dependencies
        crm_dep_edge = next((e for e in edges if 
                             e['source'] == 'BQ_TABLE:test-gcp-project-id.test_crm_dataset.customers' and
                             e['target'] == 'BQ_VIEW:test-gcp-project-id.test_crm_dataset.customer_summary_view' and
                             e['type'] == 'READS_FROM_VIEW_DEFINITION'), None)
        assert crm_dep_edge is not None, "CRM view dependency edge not found."

        sales_dep_edge = next((e for e in edges if 
                               e['source'] == 'BQ_TABLE:test-gcp-project-id.test_sales_dataset.orders' and
                               e['target'] == 'BQ_VIEW:test-gcp-project-id.test_sales_dataset.daily_sales_view' and
                               e['type'] == 'READS_FROM_VIEW_DEFINITION'), None)
        assert sales_dep_edge is not None, "Sales view dependency edge not found."
    ```

---

### Test Case 3: BigQuery Information Schema - Routines and Inferred Dependencies

*   **Purpose:** Verify that the migrated job correctly extracts metadata for BigQuery routines (stored procedures, functions) and infers dependencies from their definitions. This replaces the legacy Oracle `DBA_DEPENDENCIES` for PL/SQL objects.
*   **Setup:**
    1.  Ensure `test_crm_dataset.sp_update_customer_status` is created and its `routine_definition` explicitly references `test_crm_dataset.customers`.
    2.  Configure the migrated script as in Test 1.
*   **Action:**
    1.  Run the migrated `crm_lineage_tracker.py` script.
    2.  Download the generated JSON output from GCS.
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   The `nodes` array contains an entry for `sp_update_customer_status` with `type`: "BigQuery PROCEDURE".
        *   The `edges` array contains an entry representing the inferred dependency:
            *   `source`: `BQ_TABLE:test-gcp-project-id.test_crm_dataset.customers`
            *   `target`: `BQ_ROUTINE:test-gcp-project-id.test_crm_dataset.sp_update_customer_status`
            *   `type`: "READS_FROM_ROUTINE_DEFINITION"
    *   **Fail:** Missing routine node, incorrect routine type, or missing/incorrect dependency edge.

*   **Example Pytest Assertion (Conceptual, after loading JSON):**
    ```python
    def test_routine_and_dependency_extraction(gcs_output_json):
        nodes = gcs_output_json['nodes']
        edges = gcs_output_json['edges']

        # Check for routine node
        crm_sp_node = next((n for n in nodes if n['name'] == 'sp_update_customer_status' and n['type'] == 'BigQuery PROCEDURE'), None)
        assert crm_sp_node is not None, "CRM stored procedure node not found."

        # Check for dependency
        crm_sp_dep_edge = next((e for e in edges if 
                                e['source'] == 'BQ_TABLE:test-gcp-project-id.test_crm_dataset.customers' and
                                e['target'] == 'BQ_ROUTINE:test-gcp-project-id.test_crm_dataset.sp_update_customer_status' and
                                e['type'] == 'READS_FROM_ROUTINE_DEFINITION'), None)
        assert crm_sp_dep_edge is not None, "CRM stored procedure dependency edge not found."
    ```

---

### Test Case 4: BigQuery ETL Job Audit Table Extraction

*   **Purpose:** Verify that the migrated job correctly queries the custom `etl_job_audit` table in BigQuery for job execution records for a specific `run_date`. This replaces the legacy Oracle `ETL_JOB_AUDIT` table.
*   **Setup:**
    1.  Ensure `test_audit_dataset.etl_job_audit` is created and populated with test data for `run_date = '2023-10-26'` (as described in Test Environment Setup).
    2.  Configure `DEFAULT_AUDIT_DATASET` and `DEFAULT_AUDIT_TABLE` in the migrated script.
*   **Action:**
    1.  Run the migrated `crm_lineage_tracker.py` script with `--run_date 2023-10-26`.
    2.  Download the generated JSON output from GCS.
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   The `nodes` array contains entries for the expected ETL jobs (e.g., `CustomerDataflow`, `SalesAggregation`) with `type`: "ETL Job".
        *   Each ETL job node should contain `job_id`, `job_name`, `status`, `workflow`, `start_time`, `end_time`, and `attributes` matching the audit table data.
        *   The `edges` array contains `READS` and `WRITES` edges linking the ETL job nodes to their respective source and target tables as defined in the audit records.
    *   **Fail:** Missing ETL job nodes, incorrect job details, or missing/incorrect `READS`/`WRITES` edges.

*   **Example Pytest Assertion (Conceptual, after loading JSON):**
    ```python
    def test_etl_job_audit_extraction(gcs_output_json):
        nodes = gcs_output_json['nodes']
        edges = gcs_output_json['edges']

        # Check for ETL Job nodes
        customer_job_node = next((n for n in nodes if n['name'] == 'CustomerDataflow' and n['type'] == 'ETL Job'), None)
        sales_job_node = next((n for n in nodes if n['name'] == 'SalesAggregation' and n['type'] == 'ETL Job'), None)
        assert customer_job_node is not None, "CustomerDataflow job node not found."
        assert sales_job_node is not None, "SalesAggregation job node not found."
        assert customer_job_node['status'] == 'SUCCESS'
        assert customer_job_node['workflow'] == 'CRM_DAILY_LOAD'

        # Check for READS/WRITES edges
        customer_reads_edge = next((e for e in edges if
                                    e['source'] == 'BQ_TABLE:test-gcp-project-id.test_sales_dataset.orders' and
                                    e['target'] == customer_job_node['job_id'] and # Target is the job_id for READS
                                    e['type'] == 'READS'), None)
        customer_writes_edge = next((e for e in edges if
                                     e['source'] == customer_job_node['job_id'] and # Source is the job_id for WRITES
                                     e['target'] == 'BQ_TABLE:test-gcp-project-id.test_crm_dataset.customers' and
                                     e['type'] == 'WRITES'), None)
        assert customer_reads_edge is not None, "CustomerDataflow READS edge not found."
        assert customer_writes_edge is not None, "CustomerDataflow WRITES edge not found."
    ```

---

### Test Case 5: GCS Output Storage

*   **Purpose:** Verify that the generated JSON lineage graph is correctly uploaded to the specified GCS bucket with the expected naming convention. This replaces the legacy local file system output.
*   **Setup:**
    1.  Ensure the `test-lineage-output-bucket` exists and the service account has write permissions.
    2.  Configure `DEFAULT_OUTPUT_GCS_BUCKET` and `DEFAULT_OUTPUT_GCS_PREFIX` in the migrated script.
*   **Action:**
    1.  Run the migrated `crm_lineage_tracker.py` script with `--run_date 2023-10-26`.
    2.  Use `gsutil` or the GCS client library to list objects in the target bucket/prefix.
*   **Pass/Fail Criterion:**
    *   **Pass:** A JSON file named `lineage_CRM_WORKFLOW_20231026_YYYYMMDDHHMMSS.json` (where `YYYYMMDDHHMMSS` is the timestamp of execution) exists in `gs://test-lineage-output-bucket/lineage/crm/`. The file should not be empty and should contain valid JSON.
    *   **Fail:** File is not found, has an incorrect name/path, is empty, or contains invalid JSON.

*   **Example Pytest Assertion (using `google.cloud.storage` client):**
    ```python
    from google.cloud import storage
    from datetime import datetime
    import re

    def test_gcs_output_storage():
        bucket_name = "test-lineage-output-bucket"
        prefix = "lineage/crm"
        run_date_str = "20231026" # From --run_date 2023-10-26

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        # List blobs in the prefix
        blobs = list(bucket.list_blobs(prefix=prefix))

        # Find the most recent file matching the pattern
        expected_pattern = rf"^{prefix}/lineage_CRM_WORKFLOW_{run_date_str}_\d{{14}}\.json$"
        found_blob = None
        for blob in blobs:
            if re.match(expected_pattern, blob.name):
                found_blob = blob
                break # Assuming the most recent one is the one we just created

        assert found_blob is not None, f"No lineage JSON file found in gs://{bucket_name}/{prefix}/ matching pattern."
        
        # Verify content is valid JSON and not empty
        content = found_blob.download_as_text()
        assert len(content) > 0, "Generated JSON file is empty."
        try:
            json.loads(content)
        except json.JSONDecodeError:
            pytest.fail("Generated file does not contain valid JSON.")
    ```

---

### Test Case 6: `_schema_to_domain` Mapping

*   **Purpose:** Verify that the `_schema_to_domain` helper function correctly maps BigQuery project/dataset IDs to business domains as defined in the migrated code. This replaces the legacy Oracle schema-to-domain mapping.
*   **Setup:** None beyond the function definition in the migrated code.
*   **Action:** Directly call the `_schema_to_domain` function with various inputs.
*   **Pass/Fail Criterion:**
    *   **Pass:** The function returns the expected domain string ("customer", "sales", "finance", "unknown") for given project/dataset combinations.
    *   **Fail:** Incorrect domain mapping.

*   **Example Pytest Assertion (Unit Test):**
    ```python
    import pytest
    from customer.crm_lineage_tracker import _schema_to_domain

    @pytest.mark.parametrize("project_id, dataset_id, expected_domain", [
        ("crm-proj", "crm_data", "customer"),
        ("test-gcp-project-id", "crm_dataset_prod", "customer"), # Matches dataset prefix
        ("sales-proj", "sales_transactions", "sales"),
        ("test-gcp-project-id", "sales_reporting", "sales"), # Matches dataset prefix
        ("finance-proj", "finance_ledgers", "finance"),
        ("test-gcp-project-id", "finance_analytics", "finance"), # Matches dataset prefix
        ("other-proj", "some_dataset", "unknown"),
        ("crm-proj", "other_dataset", "customer"), # Project ID takes precedence
        ("unknown-proj", "crm_dataset", "customer"), # Dataset ID takes precedence
    ])
    def test_schema_to_domain_mapping(project_id, dataset_id, expected_domain):
        assert _schema_to_domain(project_id, dataset_id) == expected_domain
    ```

---

### Test Case 7: Handling of Missing/Empty Data

*   **Purpose:** Verify the job's robustness when certain metadata sources are empty or unavailable (e.g., no views, no audit records for a given date, no cross-dataset access).
*   **Setup:**
    1.  Create a dedicated "empty" test GCP project/dataset with no tables, views, or routines.
    2.  Ensure the `etl_job_audit` table is empty for a specific `run_date` (or does not exist, though the code handles non-existence with a warning).
    3.  Configure the migrated script to point to these empty resources.
*   **Action:**
    1.  Run the migrated `crm_lineage_tracker.py` script against the empty test environment.
    2.  Download the generated JSON output from GCS.
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   The script completes without crashing.
        *   The generated JSON is valid.
        *   The `nodes` and `edges` arrays in the JSON are empty or contain only default/placeholder entries, reflecting the lack of metadata.
        *   No `etl_job_audits` are present if the table was empty for the `run_date`.
    *   **Fail:** Script crashes, generates invalid JSON, or incorrectly reports metadata when none exists.

*   **Example Pytest Assertion (Conceptual, after loading JSON):**
    ```python
    def test_empty_metadata_handling(gcs_output_json_empty_env):
        # gcs_output_json_empty_env is a fixture that runs the job against empty resources
        nodes = gcs_output_json_empty_env['nodes']
        edges = gcs_output_json_empty_env['edges']

        assert isinstance(nodes, list)
        assert isinstance(edges, list)
        assert len(nodes) == 0, "Nodes array should be empty for an empty environment."
        assert len(edges) == 0, "Edges array should be empty for an empty environment."
        # Further checks for specific keys if they are expected to be present but empty
    ```

---

### Test Case 8: Semantic Equivalence of Lineage Chains (Conceptual)

*   **Purpose:** While direct output parity is not possible, this test aims to ensure that the *conceptual* lineage captured by the new GCP-centric `gcp_lineage_chains` and the inferred dependencies aligns with the business understanding of data flow, similar to how the legacy `lineage_chains` did for Oracle. This is a high-level, business-driven validation.
*   **Setup:**
    1.  **Legacy Baseline:** Obtain a representative JSON output from the *legacy* `crm_lineage_tracker.py` for a known Oracle environment and `run_date`.
    2.  **GCP Equivalent:** Set up a GCP environment that *semantically mirrors* the Oracle environment (e.g., `SOURCE_CRM.CUSTOMERS` in Oracle maps to `test-gcp-project-id.test_crm_dataset.customers` in BigQuery; `CRM_WEEKLY_WORKFLOW` maps to a Composer DAG that orchestrates Dataflow jobs).
    3.  **Document Mapping:** Create a mapping document that explicitly links legacy components/flows to their GCP counterparts. This is crucial for validation.
*   **Action:**
    1.  Run the migrated `crm_lineage_tracker.py` against the GCP equivalent environment.
    2.  Download the generated JSON output from GCS.
    3.  Manually or semi-automatically compare the *information content* of the legacy and migrated JSONs based on the documented mapping.
*   **Pass/Fail Criterion:**
    *   **Pass:** For each critical lineage chain identified in the legacy system (e.g., "UC4 Trigger -> ksh Script -> Oracle Staging -> PL/SQL Job -> Target Analytical Table"), a corresponding, semantically equivalent lineage path can be identified in the migrated JSON's `nodes` and `edges` (e.g., "Composer DAG -> Dataflow Job -> BigQuery Staging Table -> BigQuery Stored Procedure -> BigQuery Analytical Table"). The key entities (tables, jobs) and their relationships should be present and correct according to the mapping document.
    *   **Fail:** Significant discrepancies in the captured lineage, missing critical data flows, or incorrect relationships between GCP components that were expected to mirror legacy flows.

*   **Example Validation (Manual/Semi-Automated):**
    *   **Legacy Output Snippet (Conceptual):**
        ```json
        "lineage_chains": [
            {
                "chain_id": "CRM-001",
                "uc4_workflow": "CRM_WEEKLY_WORKFLOW",
                "ksh_script": "process_customer_data.ksh",
                "staging_table": "CRM_SCHEMA.STG_CUSTOMER_PROFILE",
                "plsql_package": "CRM_SCHEMA.PKG_CUSTOMER_HISTORIZATION.LOAD_DIM_CUSTOMER_CRM",
                "target_table": "CRM_SCHEMA.DIM_CUSTOMER_CRM",
                "plsql_table_refs": ["SOURCE_CRM.CUSTOMERS"]
            }
        ]
        ```
    *   **Migrated Output Snippet (Conceptual):**
        ```json
        "nodes": [
            {"id": "BQ_TABLE:...", "name": "customers", "type": "BigQuery Table"},
            {"id": "ETL_JOB:...", "name": "CustomerDataflow", "type": "ETL Job"},
            {"id": "BQ_TABLE:...", "name": "stg_customer_profile", "type": "BigQuery Table"},
            {"id": "BQ_ROUTINE:...", "name": "load_dim_customer_crm", "type": "BigQuery PROCEDURE"},
            {"id": "BQ_TABLE:...", "name": "dim_customer_crm", "type": "BigQuery Table"}
        ],
        "edges": [
            {"source": "BQ_TABLE:...customers", "target": "ETL_JOB:...CustomerDataflow", "type": "READS"},
            {"source": "ETL_JOB:...CustomerDataflow", "target": "BQ_TABLE:...stg_customer_profile", "type": "WRITES"},
            {"source": "BQ_TABLE:...stg_customer_profile", "target": "BQ_ROUTINE:...load_dim_customer_crm", "type": "READS_FROM_ROUTINE_DEFINITION"},
            {"source": "BQ_ROUTINE:...load_dim_customer_crm", "target": "BQ_TABLE:...dim_customer_crm", "type": "WRITES"}
        ]
        ```
    *   **Comparison:** The legacy `CRM-001` chain's flow `SOURCE_CRM.CUSTOMERS` -> `STG_CUSTOMER_PROFILE` -> `DIM_CUSTOMER_CRM` (via PL/SQL) should be traceable through the `nodes` and `edges` of the migrated output, using the GCP equivalents. This requires a human expert to verify the conceptual mapping.

---

### Test Case 9: Data Quality and Schema Consistency of Output

*   **Purpose:** Verify that the generated JSON output adheres to the expected schema (nodes/edges structure) and that the data types and values within the nodes and edges are consistent and of high quality. This ensures the DataStreak Discovery Engine can consume the new format.
*   **Setup:**
    1.  Run the migrated job against a fully populated test environment (as in Test 1-4).
    2.  Define a JSON schema (e.g., using `jsonschema` library) that represents the expected structure of the migrated job's output (nodes, edges, their properties, and data types).
*   **Action:**
    1.  Download the generated JSON output from GCS.
    2.  Validate the JSON against the predefined schema.
    3.  Perform spot checks on data types (e.g., `creation_time` is a valid timestamp string, `row_count` is an integer).
*   **Pass/Fail Criterion:**
    *   **Pass:** The generated JSON is valid and conforms to the expected `nodes`/`edges` schema. All required fields are present, and data types are correct.
    *   **Fail:** Schema validation errors, missing required fields, or incorrect data types.

*   **Example Pytest Assertion (using `jsonschema`):**
    ```python
    import json
    import pytest
    from jsonschema import validate, ValidationError

    # Define the expected JSON schema for the migrated output
    lineage_output_schema = {
        "type": "object",
        "properties": {
            "metadata_extracted_at": {"type": "string", "format": "date-time"},
            "nodes": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "type": {"type": "string"},
                        "full_path": {"type": "string"},
                        "domain": {"type": "string"},
                        "attributes": {"type": "object"} # More detailed schema for attributes can be added
                    },
                    "required": ["name", "type", "full_path", "domain", "attributes"]
                }
            },
            "edges": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "source": {"type": "string"},
                        "target": {"type": "string"},
                        "type": {"type": "string"},
                        "evidence": {"type": "string"}
                    },
                    "required": ["source", "target", "type", "evidence"]
                }
            }
        },
        "required": ["metadata_extracted_at", "nodes", "edges"]
    }

    def test_output_schema_and_data_quality(gcs_output_json):
        try:
            validate(instance=gcs_output_json, schema=lineage_output_schema)
        except ValidationError as e:
            pytest.fail(f"JSON output schema validation failed: {e.message}")

        # Additional data quality checks
        for node in gcs_output_json['nodes']:
            assert node['domain'] in ["customer", "sales", "finance", "unknown"], f"Invalid domain: {node['domain']}"
            if node['type'] == 'BigQuery Table':
                assert 'row_count' in node['attributes']
                assert isinstance(node['attributes']['row_count'], (int, type(None))) # Allow None for empty tables
            # Add more specific checks for other node types

        for edge in gcs_output_json['edges']:
            assert edge['type'] in ["READS_FROM_VIEW_DEFINITION", "READS_FROM_ROUTINE_DEFINITION", "READS", "WRITES"], f"Invalid edge type: {edge['type']}"
            assert edge['source'].startswith(('BQ_TABLE:', 'BQ_VIEW:', 'BQ_ROUTINE:', 'ETL_JOB:'))
            assert edge['target'].startswith(('BQ_TABLE:', 'BQ_VIEW:', 'BQ_ROUTINE:', 'ETL_JOB:'))
    ```