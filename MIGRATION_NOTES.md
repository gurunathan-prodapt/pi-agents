# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh` and its associated data processing logic. The original script served as an orchestration wrapper, handling parameter validation, environment setup, and executing a core SQL transformation (`d_ausd_bp_ta_apn_vertrag.sql`), followed by record counting and potential file-based post-processing.

The migration targets Google Cloud BigQuery, transforming the shell script's functionality into a BigQuery-native solution. This includes:
*   **Orchestration and Parameter Handling:** Replaced by a BigQuery Stored Procedure (`project.dataset.r_ausd_bp_ta_apn_vertrag`).
*   **Core Data Transformation:** Encapsulated within a separate BigQuery Stored Procedure (`project.dataset.d_ausd_bp_ta_apn_vertrag_core_logic`).
*   **Logging and Auditing:** Centralized into a dedicated BigQuery logging table (`project.dataset.job_log`).
*   **Temporary Data and File Operations:** Replaced by BigQuery temporary tables and SQL transformations.
*   **Post-processing:** Commented `sed`, `sort`, and `join` operations are translated into BigQuery SQL statements, creating new tables.

## 2. Generated artifacts

The following BigQuery SQL files were generated as part of this migration:

*   **`project.dataset.job_log.sql`**
    *   **Role:** Defines the DDL for the `job_log` table. This table serves as a centralized repository for tracking the execution status, parameters, and outcomes of BigQuery jobs, replacing the legacy script's error messaging and implied job tracking.

*   **`project.dataset.d_ausd_bp_ta_apn_vertrag_core_logic.sql`**
    *   **Role:** This BigQuery Stored Procedure is a placeholder for the core data transformation logic originally contained within `d_ausd_bp_ta_apn_vertrag.sql`. It is designed to receive parameters from the main orchestration procedure and return the count of processed records. **ACTION REQUIRED:** This procedure needs to be populated with the actual migrated SQL logic from the legacy `d_ausd_bp_ta_apn_vertrag.sql` file.

*   **`project.dataset.r_ausd_bp_ta_apn_vertrag.sql`**
    *   **Role:** This is the main BigQuery Stored Procedure that replaces the `k_ausd_bp_ta_apn_vertrag.ksh` shell script. It handles parameter validation, date derivation, logging job status, and orchestrates the execution of the `d_ausd_bp_ta_apn_vertrag_core_logic` procedure. It is the primary entry point for the migrated job.

*   **`project.dataset.cibasis_data24_clean.sql`**
    *   **Role:** Creates a BigQuery table by cleaning and deduplicating data, simulating the `sed` and `sort -u -k 1` operations that were commented out in the original KSH script for `cibasis_data24.dat`. It expects a raw input table `project.dataset.cibasis_data24_raw`.

*   **`project.dataset.cibasis_data96_clean.sql`**
    *   **Role:** Similar to `cibasis_data24_clean.sql`, this creates a BigQuery table by cleaning and deduplicating data from `cibasis_data96.dat` (simulating `sed` and `sort -u -k 1`). It expects a raw input table `project.dataset.cibasis_data96_raw`.

*   **`project.dataset.cibasis_fax_clean.sql`**
    *   **Role:** Cleans and deduplicates fax data from `cibasis_fax.dat` (simulating `sed` and `sort -u -k 1`), creating a BigQuery table. It expects a raw input table `project.dataset.cibasis_fax_raw`.

*   **`project.dataset.cibasis_24_96.sql`**
    *   **Role:** Performs a `JOIN` operation between the cleaned `cibasis_data24_clean` and `cibasis_data96_clean` tables, simulating the commented `join` command in the original KSH script.

*   **`project.dataset.cibasisprodukt.sql`**
    *   **Role:** This is the final output table, representing the `cibasisprodukt.csv` file implied by the legacy script. It combines data from `cibasis_24_96` and potentially `cibasis_fax_clean` to produce the final product data.

## 3. Key design decisions

*   **BigQuery-Native Orchestration:** The KornShell script's orchestration logic (parameter parsing, validation, conditional execution) has been fully migrated to a BigQuery Stored Procedure (`r_ausd_bp_ta_apn_vertrag`). This leverages BigQuery's scripting capabilities, providing a more robust, auditable, and integrated solution within the Google Cloud ecosystem, eliminating dependencies on shell environments.
*   **Modularity for Core Logic:** The core data transformation logic from `d_ausd_bp_ta_apn_vertrag.sql` is encapsulated in its own BigQuery Stored Procedure (`d_ausd_bp_ta_apn_vertrag_core_logic`). This promotes modularity, reusability, and easier maintenance, allowing the core transformation to be developed and tested independently.
*   **Structured Logging:** The legacy script's implicit error handling and commented job tracking are replaced by a dedicated `project.dataset.job_log` BigQuery table. This provides structured, queryable logs for job status, parameters, and errors, significantly improving observability and debugging capabilities.
*   **BigQuery-Native Utilities:** All shell-based utility functions (e.g., `gestern.ksh` for date derivation, `h_alis_date.ksh` for date validation, `h_alis_parameter.ksh` for parameter checks) are replaced with equivalent BigQuery SQL functions (`CURRENT_DATE()`, `DATE_SUB()`, `PARSE_DATE()`, `REGEXP_CONTAINS()`) and scripting constructs (`IF`, `RAISE`). This removes external script dependencies and streamlines the execution within BigQuery.
*   **Elimination of Filesystem Operations:** Temporary files (`.tmp`) and file-based post-processing (`sed`, `sort`, `join`) are entirely replaced by BigQuery's in-memory temporary tables (`CREATE TEMP TABLE`) and standard SQL transformations (`REGEXP_REPLACE`, `SELECT DISTINCT`, `JOIN`). This removes I/O bottlenecks, simplifies data lineage, and leverages BigQuery's distributed processing power.
*   **Parameter Handling:** Command-line parameters from the KSH script are directly translated into typed input parameters for the BigQuery Stored Procedure, ensuring type safety and clear interface definition.
*   **Trade-offs:**
    *   **Initial Development Effort:** Migrating shell scripting logic and file operations to BigQuery SQL requires a complete rewrite, which can be more involved than a direct "lift and shift."
    *   **Dependency on BigQuery:** The solution is now tightly coupled with BigQuery, which is the desired outcome for a BigQuery-native migration but means less portability to other SQL engines.
    *   **Placeholder for Core Logic:** The biggest trade-off is the current placeholder for `d_ausd_bp_ta_apn_vertrag_core_logic`. Its actual implementation will dictate the final complexity and performance.

## 4. Manual steps before go-live

1.  **BigQuery Dataset Creation:** Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it:
    ```bash
    bq mk --location=US --default_table_expiration 365 `project.dataset`
    ```
    (Adjust location and expiration as needed).

2.  **IAM Permissions:**
    *   The service account or user executing these BigQuery procedures must have:
        *   `BigQuery Data Editor` role on `project.dataset` (to create/update tables and procedures).
        *   `BigQuery Job User` role (to run queries and jobs).
        *   `BigQuery Data Viewer` role on any source datasets from which data is read.

3.  **Deploy Generated Artifacts:**
    *   Execute each `.sql` file in the order specified in the "Generated artifacts" section (DDL first, then procedures).
    *   For example, using `bq query`:
        ```bash
        bq query --project_id=<your-gcp-project-id> --use_legacy_sql=false < project.dataset.job_log.sql
        bq query --project_id=<your-gcp-project-id> --use_legacy_sql=false < project.dataset.d_ausd_bp_ta_apn_vertrag_core_logic.sql
        bq query --project_id=<your-gcp-project-id> --use_legacy_sql=false < project.dataset.r_ausd_bp_ta_apn_vertrag.sql
        bq query --project_id=<your-gcp-project-id> --use_legacy_sql=false < project.dataset.cibasis_data24_clean.sql
        # ... and so on for all generated SQL files
        ```

4.  **Populate Raw Data Tables:**
    *   The post-processing steps (`cibasis_data24_clean`, `cibasis_data96_clean`, `cibasis_fax_clean`) rely on raw input tables (e.g., `project.dataset.cibasis_data24_raw`). These tables must be created and populated with the corresponding raw data before the cleaning procedures can run successfully. Define their schemas based on the original file formats.

5.  **Implement Core SQL Logic:**
    *   **CRITICAL:** The `project.dataset.d_ausd_bp_ta_apn_vertrag_core_logic` procedure currently contains placeholder logic. It must be updated with the actual, migrated SQL from the original `d_ausd_bp_ta_apn_vertrag.sql` file. This involves:
        *   Identifying source tables and their schemas.
        *   Translating any legacy SQL dialect to BigQuery SQL.
        *   Implementing the full transformation, aggregation, and loading logic.
        *   Ensuring the procedure correctly sets the `p_records_processed` OUT parameter.

6.  **External Orchestration (if applicable):**
    *   If this job is part of a larger workflow managed by Cloud Composer (Airflow), a new Airflow DAG must be developed and deployed. This DAG will be responsible for calling the `project.dataset.r_ausd_bp_ta_apn_vertrag` BigQuery Stored Procedure with the appropriate parameters.

## 5. Known gaps & unresolved references

The following items were identified as gaps or risks during the migration design and require further attention:

*   **Core SQL (`d_ausd_bp_ta_apn_vertrag.sql`) Content:** The actual content of the main SQL script is currently unknown. This is the most significant gap. The `project.dataset.d_ausd_bp_ta_apn_vertrag_core_logic` procedure contains placeholder logic and **must be fully implemented** based on a detailed analysis of the original `d_ausd_bp_ta_apn_vertrag.sql`. This includes identifying its source tables, target tables, and specific transformation rules.
*   **Detailed Logic of Included KSH Scripts:** While the purpose of utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, etc.) was inferred, their exact implementation details were not fully available. The migration assumes standard functionality for date validation and parameter checks; any highly specific or complex logic within these utilities might require further refinement in the BigQuery implementation.
*   **Commented-out Job Management System Calls:** The original script contained commented lines like `FOSJobDeaktivate` and `FOSJobErzeugeEintrag`. It's unclear if these functionalities are still required or were part of a deprecated system. The current migration replaces this with a generic `job_log` table. If specific integration with a job management system is needed, this will require further design and implementation.
*   **`p_wiederanlaufWert` Usage:** The `p_wiederanlaufWert` parameter is passed through to the BigQuery stored procedures but its specific usage or impact within the original `d_ausd_bp_ta_apn_vertrag.sql` or subsequent steps is unknown. Its purpose should be clarified during the implementation of the core logic.
*   **Source and Target Tables for Core Logic:** The exact input and output tables for the `d_ausd_bp_ta_apn_vertrag.sql` logic are not defined. These need to be identified and their schemas mapped to BigQuery tables during the implementation of `d_ausd_bp_ta_apn_vertrag_core_logic`.
*   **Raw Data Table Schemas:** The schemas for the raw input tables (`cibasis_data24_raw`, `cibasis_data96_raw`, `cibasis_fax_raw`) are assumed based on typical file structures (e.g., `column_1`, `column_a`, `fax_number_column`). These placeholder column names must be replaced with actual column names and data types corresponding to the source files.
*   **Join Keys for Post-processing:** The join conditions in `cibasis_24_96.sql` and `cibasisprodukt.sql` are based on assumed key columns (e.g., `key_column_cleaned`). These must be verified and adjusted based on the actual data relationships.

## 6. Validation

Validation of the migrated job involves several stages:

1.  **Unit Testing:**
    *   **`project.dataset.job_log`:** Verify table creation and correct insertion of log entries for various job states (RUNNING, SUCCESS, FAILED).
    *   **`project.dataset.r_ausd_bp_ta_apn_vertrag` (Orchestration):**
        *   Test with valid parameters: Ensure successful execution, correct date derivations, and proper logging.
        *   Test with invalid parameters (missing `JobKennung`, `EintragsNr`, `Stichtag`): Verify that appropriate error messages are raised and logged, and the procedure terminates gracefully.
        *   Test with invalid `Stichtag` format (e.g., `YYYY-MM-DD` instead of `DDMMYYYY`, or non-existent dates like `31022023`): Confirm correct error handling.
    *   **`project.dataset.d_ausd_bp_ta_apn_vertrag_core_logic` (Core Logic - once implemented):**
        *   Test with various input data scenarios to ensure the transformation logic produces the expected output.
        *   Verify the `p_records_processed` OUT parameter accurately reflects the number of records processed.
    *   **Post-processing SQLs (`cibasis_data24_clean`, `cibasis_24_96`, etc.):**
        *   Provide sample raw data and verify that the cleaning, deduplication, and join operations produce the expected results in the output tables.

2.  **Integration Testing:**
    *   Execute the main `project.dataset.r_ausd_bp_ta_apn_vertrag` procedure with a full set of valid parameters and representative input data (including raw data for post-processing).
    *   Verify that all intermediate and final output tables (`cibasisprodukt`, etc.) are created/updated correctly.
    *   Check the `project.dataset.job_log` table for a `SUCCESS` entry corresponding to the run, with the correct `records_processed` count.

3.  **Data Validation / Regression Testing:**
    *   **Comparison with Legacy Output:** If possible, run the legacy `k_ausd_bp_ta_apn_vertrag.ksh` script and the migrated BigQuery job with the *exact same input data* and compare their final outputs (e.g., `cibasisprodukt.csv` vs. `project.dataset.cibasisprodukt` table content). This is the most robust form of validation.
    *   **Schema Validation:** Ensure the schema of the BigQuery output tables matches the expected schema, including data types and nullability.
    *   **Record Counts:** Compare the total number of records in the final output tables with the legacy system's output.
    *   **Data Integrity:** Spot-check data values for accuracy and consistency.

**"Passing" means:**
*   The `project.dataset.r_ausd_bp_ta_apn_vertrag` stored procedure executes without raising unhandled errors.
*   A `SUCCESS` entry is recorded in `project.dataset.job_log` for the specific job run.
*   The `records_processed` count in the `job_log` (and returned by the procedure) matches the expected number of records processed by the core logic.
*   All target tables (including `cibasisprodukt` and intermediate tables) are created/updated with the correct data, matching the output of the legacy system for the same inputs.
*   All parameter validations and error handling mechanisms function as expected.

## 7. Rollback procedure

In case of critical issues identified after go-live, the following rollback procedure can be followed:

1.  **Stop New Executions:** Immediately halt any scheduled or manual executions of the `project.dataset.r_ausd_bp_ta_apn_vertrag` BigQuery Stored Procedure. If using an orchestrator like Cloud Composer, pause or disable the corresponding DAG.

2.  **Revert to Legacy System:** Re-enable the scheduling and execution of the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh` script. Ensure its environment and dependencies are fully functional.

3.  **Data Cleanup (Optional but Recommended):**
    *   If the BigQuery job has produced output tables that conflict with the legacy system's operation or contain erroneous data, consider truncating or dropping these tables (e.g., `project.dataset.cibasisprodukt`, `project.dataset.cibasis_24_96`, etc.). This prevents downstream systems from consuming incorrect data.
    *   Example: `TRUNCATE TABLE `project.dataset.cibasisprodukt`;`

4.  **Monitor Legacy System:** Closely monitor the re-enabled legacy job to ensure it is running correctly and producing expected outputs.

5.  **Root Cause Analysis:** Investigate the issues that necessitated the rollback. This may involve reviewing BigQuery job logs, examining data in intermediate tables, and debugging the BigQuery SQL code.

6.  **Plan for Re-migration/Fix:** Once the root cause is identified and a fix or re-migration strategy is developed, repeat the build, deployment, and validation steps.