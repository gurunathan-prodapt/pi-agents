Here is a comprehensive suite of migration-validation tests for the `d_alis_spaufruf_p0.sql` BigQuery script. 

These tests are designed to validate that the migrated BigQuery dynamic wrapper behaves identically to the legacy Oracle SQL*Plus wrapper, covering parameter handling, transaction boundaries, SQL injection prevention, and error propagation.

---

# Test Suite: `d_alis_spaufruf_p0.sql` Migration Validation

## 1. Output Parity & Dynamic Execution Tests

### Test Case 1.1: Parameterized Execution Without Arguments
*   **Purpose:** Verify that passing a procedure name without arguments successfully constructs and executes the correct `CALL` statement, matching the legacy `EXEC &1;` behavior.
*   **Setup:**
    1. Create a dummy target dataset: `test_dataset_validation`.
    2. Create a dummy stored procedure in BigQuery that writes a record to a log table:
       ```sql
       CREATE OR REPLACE TABLE `test_dataset_validation.execution_log` (
           proc_name STRING,
           executed_at TIMESTAMP
       );

       CREATE OR REPLACE PROCEDURE `test_dataset_validation.sp_no_args`()
       BEGIN
           INSERT INTO `test_dataset_validation.execution_log` VALUES ('sp_no_args', CURRENT_TIMESTAMP());
       END;
       ```
*   **Action:** Execute the migrated script `d_alis_spaufruf_p0.sql` using the BigQuery Python Client with the following parameters:
    *   `@target_dataset` = `'test_dataset_validation'`
    *   `@procedure_name` = `'sp_no_args'`
    *   `@arguments` = `NULL`
*   **Pass/Fail Criterion:** 
    *   **Pass:** The script executes successfully. A query on `test_dataset_validation.execution_log` returns exactly 1 row with `proc_name = 'sp_no_args'`.
    *   **Fail:** The script throws a syntax error, fails to resolve the procedure, or no log entry is written.

### Test Case 1.2: Parameterized Execution With Arguments
*   **Purpose:** Verify that passing a procedure name with arguments successfully constructs and executes the parameterized `CALL` statement (legacy parameter `P2`).
*   **Setup:**
    1. Create a dummy stored procedure that accepts arguments:
       ```sql
       CREATE OR REPLACE PROCEDURE `test_dataset_validation.sp_with_args`(x INT64, y STRING)
       BEGIN
           INSERT INTO `test_dataset_validation.execution_log` VALUES (FORMAT('sp_with_args: %d, %s', x, y), CURRENT_TIMESTAMP());
       END;
       ```
*   **Action:** Execute the migrated script with the following parameters:
    *   `@target_dataset` = `'test_dataset_validation'`
    *   `@procedure_name` = `'sp_with_args'`
    *   `@arguments` = `'42, "test_string"'`
*   **Pass/Fail Criterion:**
    *   **Pass:** The script executes successfully. A query on `test_dataset_validation.execution_log` returns a row containing `'sp_with_args: 42, test_string'`.
    *   **Fail:** The script fails to parse the arguments or throws a signature mismatch error.

---

## 2. Transformation & Edge-Case Correctness Tests

### Test Case 2.1: SQL Injection Prevention (Sanitization)
*   **Purpose:** Verify that the regex sanitization logic (`REGEXP_REPLACE`) successfully strips out malicious SQL injection payloads from the dataset and procedure parameters.
*   **Setup:** None.
*   **Action:** Execute the migrated script with a malicious payload designed to break out of the identifier block:
    *   `@target_dataset` = `'test_dataset_validation; DROP TABLE execution_log; --'`
    *   `@procedure_name` = `'sp_no_args'`
    *   `@arguments` = `NULL`
*   **Pass/Fail Criterion:**
    *   **Pass:** The script fails with a controlled error (e.g., `Dataset "test_dataset_validationDROPTABLEexecution_log" not found`), proving that the semicolons, spaces, and comment dashes were stripped out, preventing the execution of the second command.
    *   **Fail:** The script executes the injected command, or throws an unhandled system parsing error instead of a clean routing error.

### Test Case 2.2: Null and Empty Parameter Handling
*   **Purpose:** Verify that the script raises a clear, user-friendly error when the procedure name is null or empty, preventing execution of invalid dynamic SQL.
*   **Setup:** None.
*   **Action:** Execute the migrated script with:
    *   `@target_dataset` = `'test_dataset_validation'`
    *   `@procedure_name` = `''` (Empty string)
    *   `@arguments` = `NULL`
*   **Pass/Fail Criterion:**
    *   **Pass:** The execution fails immediately with the exact exception message: `"Error: The Stored Procedure name (procedure_name) cannot be empty."`
    *   **Fail:** The script attempts to execute `CALL test_dataset_validation.()` and throws a generic BigQuery syntax error.

---

## 3. Transaction & Error Handling Tests

### Test Case 3.1: Transaction Rollback on Failure
*   **Purpose:** Verify that if the called procedure fails midway, the entire transaction is rolled back (equivalent to Oracle's `WHENEVER OSERROR EXIT FAILURE ROLLBACK`).
*   **Setup:**
    1. Create a table to track state:
       ```sql
       CREATE OR REPLACE TABLE `test_dataset_validation.transaction_test` (val STRING);
       ```
    2. Create a procedure that inserts a row and then deliberately throws an error:
       ```sql
       CREATE OR REPLACE PROCEDURE `test_dataset_validation.sp_failing_tx`()
       BEGIN
           INSERT INTO `test_dataset_validation.transaction_test` VALUES ('temporary_state');
           -- Force division by zero error
           SELECT 1 / 0;
       END;
       ```
*   **Action:** Execute the migrated script with:
    *   `@target_dataset` = `'test_dataset_validation'`
    *   `@procedure_name` = `'sp_failing_tx'`
    *   `@arguments` = `NULL`
*   **Pass/Fail Criterion:**
    *   **Pass:** 
        1. The wrapper script fails and raises an exception starting with `'SP_Execution_Error: Division by zero'`.
        2. A query on `test_dataset_validation.transaction_test` returns **0 rows** (proving the rollback was successful).
    *   **Fail:** The script fails but the row `'temporary_state'` remains committed in the table.

---

## 4. Automated Integration Test (Pytest Implementation)

This runnable Python test script automates the validation of the dynamic wrapper using the official Google Cloud BigQuery SDK.

```python
import pytest
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

PROJECT_ID = "your-gcp-project"  # Replace with target GCP Project
DATASET_ID = "test_dataset_validation"

@pytest.fixture(scope="session")
def bq_client():
    return bigquery.Client(project=PROJECT_ID)

@pytest.fixture(scope="session", autouse=True)
def setup_test_environment(bq_client):
    # Create clean test dataset
    dataset_ref = bigquery.DatasetReference(PROJECT_ID, DATASET_ID)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "EU"
    bq_client.create_dataset(dataset, exists_ok=True)

    # Create log table
    bq_client.query(f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.execution_log` (
            proc_name STRING,
            executed_at TIMESTAMP
        );
    """).result()

    # Create dummy procedures
    bq_client.query(f"""
        CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.sp_success_no_args`()
        BEGIN
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.execution_log` VALUES ('sp_success_no_args', CURRENT_TIMESTAMP());
        END;
    """).result()

    yield

    # Cleanup
    bq_client.delete_dataset(dataset_ref, delete_contents=True, not_found_ok=True)


def run_migration_wrapper(bq_client, target_dataset, procedure_name, arguments=None):
    """Helper to execute the migrated wrapper script with parameters"""
    with open("d_alis_spaufruf_p0.sql", "r") as f:
        sql_script = f.read()

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("target_dataset", "STRING", target_dataset),
            bigquery.ScalarQueryParameter("procedure_name", "STRING", procedure_name),
            bigquery.ScalarQueryParameter("arguments", "STRING", arguments),
        ]
    )
    query_job = bq_client.query(sql_script, job_config=job_config)
    return query_job.result()


def test_successful_execution_no_args(bq_client):
    # Clear log table
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.execution_log`").result()

    # Run wrapper
    run_migration_wrapper(
        bq_client=bq_client,
        target_dataset=DATASET_ID,
        procedure_name="sp_success_no_args",
        arguments=None
    )

    # Assert row was written
    query = f"SELECT COUNT(*) as cnt FROM `{PROJECT_ID}.{DATASET_ID}.execution_log` WHERE proc_name = 'sp_success_no_args'"
    results = list(bq_client.query(query).result())
    assert results[0]["cnt"] == 1


def test_empty_procedure_name_validation(bq_client):
    with pytest.raises(BadRequest) as excinfo:
        run_migration_wrapper(
            bq_client=bq_client,
            target_dataset=DATASET_ID,
            procedure_name="",
            arguments=None
        )
    assert "The Stored Procedure name (procedure_name) cannot be empty" in str(excinfo.value)


def test_sql_injection_sanitization(bq_client):
    # Attempt injection in dataset parameter
    with pytest.raises(BadRequest) as excinfo:
        run_migration_wrapper(
            bq_client=bq_client,
            target_dataset=f"{DATASET_ID}; DROP TABLE `{PROJECT_ID}.{DATASET_ID}.execution_log`; --",
            procedure_name="sp_success_no_args",
            arguments=None
        )
    # Verify the table was NOT dropped (injection failed and table still exists)
    query = f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.execution_log`"
    bq_client.query(query).result()  # Should not raise "Table not found"
```