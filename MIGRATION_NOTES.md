# MIGRATION_NOTES.md: k_ausd_bp_ta_msisdn.ksh

## 1. Summary
The legacy Korn Shell script `k_ausd_bp_ta_msisdn.ksh` has been migrated from its on-premises Unix/Oracle environment to **Google Cloud Platform (GCP)**. 

*   **Source Platform:** Unix/Korn Shell (`ksh`) orchestrating Oracle SQL*Plus database calls, local file-system operations, and custom shell-based date utilities.
*   **Target Platform:** **Google Cloud Composer (Apache Airflow)** orchestrating native **Google BigQuery** execution via BigQuery Stored Procedures.
*   **Core Functionality:** The job acts as an orchestration and validation wrapper. It parses runtime parameters, validates date formats, calculates relative execution dates (today/yesterday), executes core business logic transformations, and records execution metrics (record counts, job status) in audit tables.

---

## 2. Generated Artifacts
The migration process has generated the following modular artifacts to replace the legacy shell script:

| No. | Target File Path | Target Language | Role / Description |
|---|---|---|---|
| 1 | `stored_procedures/r_ausd_bp_ta_msisdn.sql` | GoogleSQL | **Main Stored Procedure:** Replaces the shell wrapper logic. Handles parameter validation, date parsing, execution orchestration, and final audit logging. |
| 2 | `stored_procedures/sp_validate_required_param.sql` | GoogleSQL | **Validation Helper:** Reusable utility procedure to verify that mandatory parameters are present and non-empty. |
| 3 | `stored_procedures/sp_raise_and_log_error.sql` | GoogleSQL | **Error Handler:** Reusable utility procedure that logs execution failures to `error_log` and raises a runtime exception using `SIGNAL SQLSTATE`. |
| 4 | `stored_procedures/sp_execute_core_sql.sql` | GoogleSQL | **Core Execution Wrapper:** Encapsulates the core transformation logic. Contains a placeholder query to be populated with the translated logic of `d_ausd_bp_ta_msisdn.sql`. |
| 5 | `dags/k_ausd_bp_ta_msisdn_dag.py` | Python (Airflow) | **Orchestration DAG:** Defines the Airflow workflow, handles parameter parsing (with fallback defaults), and executes the main BigQuery stored procedure using parameterized queries. |

---

## 3. Key Design Decisions

### A. Shift from Shell Orchestration to BigQuery Stored Procedures
*   **Decision:** Instead of running a Python or Bash script on an Airflow worker to validate parameters and parse dates, this logic was pushed entirely into BigQuery Stored Procedures (`r_ausd_bp_ta_msisdn`).
*   **Reasoning:** Minimizes data egress/ingress and reduces compute overhead on Airflow workers. BigQuery handles date parsing (`SAFE.PARSE_DATE`) and conditional logic natively and highly efficiently.

### B. Modularization of Database Utilities
*   **Decision:** Created standalone helper procedures (`sp_validate_required_param`, `sp_raise_and_log_error`, and `sp_execute_core_sql`) instead of writing monolithic code.
*   **Reasoning:** Promotes code reuse across the wider migrated ISBERT codebase, simplifies unit testing, and mirrors the modular structure of the legacy shell helpers (`f_alis_msgerr.ksh`, etc.).

### C. Elimination of Local File System Dependencies
*   **Decision:** Replaced the temporary file `$DW_DIR_UTL/bert_k_ausd_bp_ta_msisdn.tmp` (used to pass record counts) with a BigQuery `OUT` parameter and native SQL variables.
*   **Reasoning:** Cloud-native architectures should avoid local disk state. Using memory variables and temporary tables (`CREATE TEMP TABLE`) ensures thread safety and horizontal scalability.

### D. Parameterized Airflow Execution
*   **Decision:** The Airflow DAG uses `bigquery.ScalarQueryParameter` to pass parameters to the stored procedure.
*   **Reasoning:** Prevents SQL injection risks and allows the DAG to be triggered dynamically via the Airflow UI or CLI with custom JSON configurations (e.g., `{"p_Stichtag": "31122024"}`).

---

## 4. Manual Steps Before Go-Live

### A. Schema & Dataset Creation
Ensure the target dataset exists in your GCP project. Run the following DDL if not already created by your CI/CD pipeline:
```sql
CREATE SCHEMA IF NOT EXISTS `gcp-project-placeholder.dw_isbert_dataset`
OPTIONS(location="europe-west3"); -- Adjust region as needed
```

### B. Table DDL Deployment
Deploy the tracking and logging tables used by the stored procedures:
*   `dw_isbert_dataset.error_log`
*   `dw_isbert_dataset.job_table`
*   `dw_isbert_dataset.process_log`

*(Note: The DDL statements for these tables are embedded at the top of `stored_procedures/r_ausd_bp_ta_msisdn.sql` for convenience).*

### C. IAM & Permissions
The service account running Cloud Composer / Airflow must have the following IAM roles:
*   **BigQuery Connection User** (to execute queries)
*   **BigQuery Data Editor** (on the `dw_isbert_dataset` dataset to write logs and target tables)
*   **BigQuery Job User** (at the project level to run query jobs)

### D. Connection Strings & Environment Variables
In the Airflow Environment:
1.  Verify that the default BigQuery connection (`bigquery_default`) is configured.
2.  If using a custom connection, update the `bigquery.Client()` initialization in `k_ausd_bp_ta_msisdn_dag.py`.
3.  Replace the placeholder `gcp-project-placeholder` in all SQL and Python files with your actual GCP Project ID.

### E. Scheduling
The DAG is currently configured with `schedule_interval=None` (manual/triggered execution). If this job must run on a daily cadence, update the DAG definition:
```python
schedule_interval="0 2 * * *" # Example: Run daily at 02:00 AM
```

---

## 5. Known Gaps & Unresolved References

### A. Core Transformation Logic (B4 Redesign Item)
*   **Gap:** The business logic contained in the legacy SQL script `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_msisdn.sql` was not part of the shell script and is therefore missing from this migration package.
*   **Action Required:** Translate `d_ausd_bp_ta_msisdn.sql` from Oracle SQL to GoogleSQL and paste the query logic inside the placeholder section of `stored_procedures/sp_execute_core_sql.sql`.

### B. Commented-Out File Processing Pipeline
*   **Gap:** The legacy script contained commented-out steps utilizing Unix utilities (`sed`, `sort`, `join`) to generate a consolidated CSV file (`cibasisprodukt.csv`).
*   **Action Required:** Confirm with business stakeholders if this CSV export is still required. If yes, implement a downstream Airflow task using the `BigQueryToGCSOperator` to export the final table to a Google Cloud Storage bucket as a CSV.

---

## 6. Validation
To validate the migration, execute the following test cases:

### Test Case 1: Missing Parameters (Failure Validation)
1.  Trigger the Airflow DAG with empty parameters: `{}`.
2.  **Expected Result:** The DAG task fails. Inspect the BigQuery `error_log` table; it should contain a entry with `error_text = 'Bitte ueber Rahmenscript aufrufen'` and `error_arg = 'Jobkennung'`.

### Test Case 2: Invalid Date Format (Failure Validation)
1.  Trigger the Airflow DAG with an invalid date format:
    ```json
    {
      "p_JobKennung": "TEST_JOB",
      "p_EintragsNr": "100",
      "p_Stichtag": "2024-12-31"
    }
    ```
    *(Note: Format must be `DDMMYYYY`, so `2024-12-31` is invalid).*
2.  **Expected Result:** The DAG task fails. The `error_log` table should record `error_text = 'Ungueltiges Datum'`.

### Test Case 3: Successful Execution (Happy Path)
1.  Trigger the Airflow DAG with valid parameters:
    ```json
    {
      "p_JobKennung": "TEST_JOB",
      "p_EintragsNr": "100",
      "p_Stichtag": "31122024",
      "p_wiederanlaufWert": "0"
    }
    ```
2.  **Expected Result:** The DAG completes with a `SUCCESS` status.
3.  **Database Verification:**
    *   Query `dw_isbert_dataset.job_table`: Verify a row exists for `tab_name = 'PoolBasisprodukt'` with `start_date = '2024-12-31'`.
    *   Query `dw_isbert_dataset.process_log`: Verify a row exists logging the execution parameters and the processed record count.

---

## 7. Rollback Procedure
In the event of an issue during deployment or execution, perform the following rollback steps:

1.  **Pause the Airflow DAG:**
    Go to the Airflow UI and toggle the switch for `k_ausd_bp_ta_msisdn` to **Off** to prevent scheduled or accidental executions.
2.  **Drop Stored Procedures:**
    Clean up the deployed database objects by executing:
    ```sql
    DROP PROCEDURE IF EXISTS `gcp-project-placeholder.dw_isbert_dataset.r_ausd_bp_ta_msisdn`;
    DROP PROCEDURE IF EXISTS `gcp-project-placeholder.dw_isbert_dataset.sp_validate_required_param`;
    DROP PROCEDURE IF EXISTS `gcp-project-placeholder.dw_isbert_dataset.sp_raise_and_log_error`;
    DROP PROCEDURE IF EXISTS `gcp-project-placeholder.dw_isbert_dataset.sp_execute_core_sql`;
    ```
3.  **Remove DAG File:**
    Delete the DAG file `k_ausd_bp_ta_msisdn_dag.py` from the Cloud Composer `/dags` folder in the environment's GCS bucket.
4.  **Revert to Legacy:**
    If a fallback to the on-premises system is required, ensure the legacy scheduler (e.g., UC4/Automic or crontab) is reactivated for `k_ausd_bp_ta_msisdn.ksh`.