# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the `r_ausd_bp_ta_msisdn.ksh` job, originally a KornShell-based process, to Google Cloud Platform (GCP).

**What was migrated:**
The original job, comprising the orchestrator script `r_ausd_bp_ta_msisdn.ksh`, the core logic script `k_ausd_bp_ta_msisdn.ksh`, and the SQL data extraction script `d_ausd_bp_ta_msisdn.sql`, has been refactored. This job's primary function is to generate a snapshot extraction of contract cache data for a specific cutoff date (`Stichtag`) to support a downstream Forderungsscoring (FOS) process, including restart/resume capabilities.

**To which target platform:**
The job has been migrated to leverage Google Cloud Platform services:
*   **BigQuery:** For data storage, transformation, and execution of the core business logic via Stored Procedures.
*   **Cloud Composer (Apache Airflow):** For scheduling, orchestration, and monitoring of the BigQuery job.

## 2. Generated Artifacts

The migration produced the following artifacts:

*   **`bq_schema_definition.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for all BigQuery tables required by the migrated job. This includes the source table (`sof_ta_msisdn_his`), the target table (`sof_ta_msisdn`), and new audit/logging tables (`job_audit`, `job_result_counts`, `dwtk_meldungen`).
    *   **Purpose:** Ensures the necessary data structures are in place within BigQuery before job execution.

*   **`bq_utility_procedures.sql`**
    *   **Role:** Contains helper BigQuery Stored Procedures (BQSPs) that encapsulate common utility functions. Currently, this includes `validate_ddmmyyyy`, which validates and converts date strings.
    *   **Purpose:** Replaces functionalities previously handled by sourced KornShell utility scripts (e.g., `h_alis_date.ksh`) with BigQuery-native, reusable components.

*   **`bq_d_ausd_bp_ta_msisdn_logic.sql`**
    *   **Role:** This file contains the core data extraction and transformation logic, translated from the original `d_ausd_bp_ta_msisdn.sql` script. It performs the actual data manipulation, including truncating the target table and inserting processed records.
    *   **Purpose:** To centralize the data processing logic within BigQuery Standard SQL, making it directly executable within the main orchestrating stored procedure.

*   **`bq_r_ausd_bp_ta_msisdn_sp.sql`**
    *   **Role:** This is the main orchestrating BigQuery Stored Procedure (`r_ausd_bp_ta_msisdn`). It combines the logic of the original `r_ausd_bp_ta_msisdn.ksh` and `k_ausd_bp_ta_msisdn.ksh` scripts. It handles parameter parsing, defaulting, date validation, error handling, logging to audit tables, and executes the core data transformation logic.
    *   **Purpose:** To provide a single, self-contained, and executable unit within BigQuery that manages the entire job flow.

*   **`cloud_composer_dag.py`**
    *   **Role:** An Apache Airflow Directed Acyclic Graph (DAG) definition written in Python. It is responsible for scheduling and executing the `r_ausd_bp_ta_msisdn` BigQuery Stored Procedure, passing necessary parameters, and monitoring its execution.
    *   **Purpose:** To replace the legacy scheduling mechanism (e.g., cron) with a robust, cloud-native orchestration tool, enabling advanced scheduling, monitoring, and dependency management.

## 3. Key Design Decisions

The migration involved several key design decisions to leverage GCP capabilities and improve maintainability:

*   **Consolidation into BigQuery Stored Procedures:**
    *   **Decision:** The logic from the multiple KornShell scripts (`r_ausd_bp_ta_msisdn.ksh`, `k_ausd_bp_ta_msisdn.ksh`) and the SQL script (`d_ausd_bp_ta_msisdn.sql`) was consolidated into a single main BigQuery Stored Procedure (`bq_r_ausd_bp_ta_msisdn_sp.sql`), with helper procedures for common utilities.
    *   **Rationale:** This approach simplifies deployment, reduces inter-script dependencies, and allows the entire data processing flow to execute natively within BigQuery, benefiting from its performance and scalability.
    *   **Trade-offs:** Increases the complexity of a single BQSP, potentially making isolated debugging of sub-components more challenging compared to separate shell scripts.

*   **Leveraging BigQuery Native Features for Logic:**
    *   **Decision:** Custom shell utilities for date handling (`h_alis_date.ksh`), parameter validation, and error logging (`f_alis_msgerr.ksh`) were replaced with BigQuery's built-in functions (`PARSE_DATE`, `CURRENT_DATE()`, `SAFE.PARSE_DATE`) and error handling constructs (`BEGIN...EXCEPTION...END`, `SIGNAL SQLSTATE`).
    *   **Rationale:** Utilizes robust, scalable, and cloud-native features, reducing the need for custom code and improving reliability.
    *   **Trade-offs:** Requires careful translation of shell-specific logic and error handling patterns into BigQuery SQL.

*   **Structured Audit and Logging:**
    *   **Decision:** Dedicated BigQuery tables (`job_audit`, `job_result_counts`) were introduced to capture job execution details, status, errors, and record counts.
    *   **Rationale:** Replaces file-based logging and custom internal frameworks, providing a structured, queryable, and centralized audit trail for all job runs. This enhances observability and simplifies troubleshooting.
    *   **Trade-offs:** Requires explicit `INSERT` and `UPDATE` statements within the BQSP for logging, adding to the procedure's code.

*   **Cloud Composer for Orchestration:**
    *   **Decision:** Apache Airflow via Cloud Composer was chosen for scheduling and orchestrating the BigQuery Stored Procedure.
    *   **Rationale:** Provides a managed, scalable, and feature-rich orchestration platform that replaces legacy cron-based scheduling. It offers robust scheduling capabilities, dependency management, monitoring, and alerting.
    *   **Trade-offs:** Introduces a new technology stack (Python/Airflow) for orchestration, requiring Airflow expertise for development and maintenance.

*   **Direct Data Model Mapping:**
    *   **Decision:** The source and target data models were directly mapped to BigQuery tables (`sof_ta_msisdn_his`, `sof_ta_msisdn`).
    *   **Rationale:** Preserves the existing data structure and integrity, minimizing changes to the data contract.
    *   **Trade-offs:** Assumes direct compatibility of data types between the original database and BigQuery; potential for schema drift if the source system's schema evolves without corresponding updates in BigQuery.

## 4. Manual Steps Before Go-Live

The following manual steps must be completed before the migrated job can be put into production:

1.  **GCP Project and BigQuery Dataset Setup:**
    *   Ensure the target GCP project (`my-gcp-project`) and BigQuery dataset (`isbert_dataset`) exist. If not, create them.

2.  **IAM Permissions Configuration:**
    *   Grant the necessary BigQuery permissions (e.g., `BigQuery Data Editor`, `BigQuery Job User`) to the service accounts that will:
        *   Execute the BigQuery Stored Procedures (if called directly).
        *   Run the Cloud Composer environment's worker and scheduler.
    *   Ensure the service account used by Cloud Composer has permissions to create/update BigQuery tables and execute stored procedures.

3.  **BigQuery Schema Deployment:**
    *   Execute the `bq_schema_definition.sql` script in the target BigQuery dataset to create the `sof_ta_msisdn_his`, `sof_ta_msisdn`, `job_audit`, `job_result_counts`, and `dwtk_meldungen` tables.
    *   **Note:** The `sof_ta_msisdn_his` and `dwtk_meldungen` tables must be populated with source data before the job can run successfully. This typically involves a separate data ingestion pipeline.

4.  **BigQuery Stored Procedure Deployment:**
    *   Execute the `bq_utility_procedures.sql` script to create the helper stored procedures (e.g., `validate_ddmmyyyy`).
    *   Execute the `bq_r_ausd_bp_ta_msisdn_sp.sql` script to create the main orchestrating stored procedure (`r_ausd_bp_ta_msisdn`).

5.  **Cloud Composer Environment Setup:**
    *   If not already present, set up a Cloud Composer environment.

6.  **Airflow DAG Deployment:**
    *   Upload the `cloud_composer_dag.py` file to the DAGs folder of the Cloud Composer environment.
    *   Verify that the DAG appears in the Airflow UI and is parsed without errors.

7.  **Connection Strings / Secrets:**
    *   No explicit connection strings are required for BigQuery Stored Procedures as they operate natively within BigQuery.
    *   Ensure the Cloud Composer environment's service account has the necessary permissions to interact with BigQuery. No additional secrets are typically needed for this direct interaction.

8.  **Scheduling Activation:**
    *   Once deployed and validated, enable the `r_ausd_bp_ta_msisdn_dag` in the Airflow UI.

## 5. Known Gaps & Unresolved References

The following items were identified as gaps or require further attention:

*   **Missing `d_ausd_bp_ta_msisdn.sql` Content (Critical B4 Item):**
    *   **Description:** The actual SQL code within the original `d_ausd_bp_ta_msisdn.sql` was *not available* during the design and generation phase. The `bq_d_ausd_bp_ta_msisdn_logic.sql` and the embedded logic in `bq_r_ausd_bp_ta_msisdn_sp.sql` are based on *inferred* logic from the design document.
    *   **Impact:** The generated core data transformation logic (`INSERT` statement) might not fully replicate the original job's behavior.
    *   **Resolution:** **Urgent manual review and translation of the original `d_ausd_bp_ta_msisdn.sql` is required.** The generated SQL should be replaced or augmented with the precise logic from the source file. This is a **B4 (Redesign/Manual Intervention Required)** item.

*   **`p_wiederanlaufwert` Logic Implementation:**
    *   **Description:** The design document mentions that `p_wiederanlaufwert` (restart value) is used to filter `DWH_VERTRAG_ID`. However, the generated `bq_d_ausd_bp_ta_msisdn_logic.sql` does not explicitly incorporate this filtering logic.
    *   **Impact:** The restart/resume capability might not be fully functional as intended by the original job.
    *   **Resolution:** The core data extraction logic within `bq_r_ausd_bp_ta_msisdn_sp.sql` (or `bq_d_ausd_bp_ta_msisdn_logic.sql`) needs to be updated to correctly apply the `p_wiederanlaufwert` parameter for filtering.

*   **Oracle-Specific SQL Constructs:**
    *   **Description:** If the original `d_ausd_bp_ta_msisdn.sql` contained Oracle-specific SQL (e.g., `DECODE`, `ROWNUM`, specific date functions), these would require careful manual translation to BigQuery Standard SQL. This cannot be fully assessed without the original SQL content.
    *   **Impact:** Potential for incorrect data transformation if Oracle-specific nuances are not correctly translated.
    *   **Resolution:** Part of the manual review of `d_ausd_bp_ta_msisdn.sql` content.

*   **Full Parity with Legacy Job Framework:**
    *   **Description:** The original job used a custom messaging and job control system (`DWMSG_*`, `FOSJob*`, `f_alis_msgerr.ksh`). While BigQuery audit tables provide structured logging, full functional parity with all nuances of the original framework (e.g., specific message types, historical data retention policies) might not be achieved.
    *   **Impact:** Minor differences in logging detail or historical context might exist.
    *   **Resolution:** If specific legacy framework features are critical, further investigation into `f_alis_msgerr.ksh` and related scripts is needed to determine if additional BigQuery logging or custom UDFs are required.

*   **`p_eintrags_nr` Parameter Usage:**
    *   **Description:** The `p_eintrags_nr` parameter is passed to the main BQSP, but its specific usage or impact on the data transformation logic is not clear from the provided design document or the inferred SQL.
    *   **Impact:** If this parameter had a functional role in the original job, that role might be missing in the migrated version.
    *   **Resolution:** Clarify the intended use of `p_eintrags_nr` from the original source code or business users and implement the corresponding logic in the BQSP if necessary.

## 6. Validation

Validation ensures the migrated job functions correctly and produces accurate results.

**How to Run Tests:**

1.  **Unit Testing BigQuery Stored Procedures:**
    *   **`bq_utility_procedures.sql`:**
        *   Execute `CALL \`my-gcp-project.isbert_dataset.validate_ddmmyyyy\`('01012023', @output_date);` and verify `@output_date` is `2023-01-01`.
        *   Execute `CALL \`my-gcp-project.isbert_dataset.validate_ddmmyyyy\`('32012023', @output_date);` and verify it raises an error.
    *   **`bq_d_ausd_bp_ta_msisdn_logic.sql` (or embedded logic):**
        *   Populate `my-gcp-project.isbert_dataset.sof_ta_msisdn_his` with a small, controlled set of test data.
        *   Manually execute the `TRUNCATE` and `INSERT` statements from `bq_d_ausd_bp_ta_msisdn_logic.sql` (or the relevant section of the main SP).
        *   Query `my-gcp-project.isbert_dataset.sof_ta_msisdn` to verify the output.
    *   **`bq_r_ausd_bp_ta_msisdn_sp.sql`:**
        *   Call the main stored procedure with various parameter combinations:
            *   `CALL \`my-gcp-project.isbert_dataset.r_ausd_bp_ta_msisdn\`(NULL, NULL, 'TEST_JOB', NULL);` (defaults)
            *   `CALL \`my-gcp-project.isbert_dataset.r_ausd_bp_ta_msisdn\`('01012023', 12345, 'TEST_JOB_SPECIFIC', 'ENTRY1');` (specific parameters)
            *   `CALL \`my-gcp-project.isbert_dataset.r_ausd_bp_ta_msisdn\`('32012023', NULL, 'TEST_JOB_INVALID_DATE', NULL);` (invalid date to test error handling)
        *   After each call, query `my-gcp-project.isbert_dataset.job_audit` and `my-gcp-project.isbert_dataset.job_result_counts` to check logging.
        *   Query `my-gcp-project.isbert_dataset.sof_ta_msisdn` to check the data output.

2.  **Integration Testing with Cloud Composer:**
    *   Trigger the `r_ausd_bp_ta_msisdn_dag` manually from the Airflow UI.
    *   Monitor the DAG run in the Airflow UI for task status and logs.
    *   After completion, verify the entries in `my-gcp-project.isbert_dataset.job_audit` and `my-gcp-project.isbert_dataset.job_result_counts`.
    *   Inspect the data in `my-gcp-project.isbert_dataset.sof_ta_msisdn`.

**What "Passing" Means:**

*   **Airflow DAG:** The `r_ausd_bp_ta_msisdn_dag` completes successfully without any failed tasks.
*   **BigQuery Audit:**
    *   The `my-gcp-project.isbert_dataset.job_audit` table contains an entry for the specific run with `status = 'SUCCESS'`.
    *   For runs designed to fail (e.g., invalid date), the `status` should be `'FAILED'` and `error_message` should contain relevant details.
*   **Record Counts:** The `my-gcp-project.isbert_dataset.job_result_counts` table contains an entry for the run, and the `record_count` matches the expected number of records processed/inserted.
*   **Data Accuracy:** The data in the target table `my-gcp-project.isbert_dataset.sof_ta_msisdn` is identical to the expected output based on the original `d_ausd_bp_ta_msisdn.sql` logic, given the same input data. This is the most critical validation step and may require comparing output with the legacy system for a controlled dataset.
*   **No Unexpected Errors:** No unexpected errors are observed in BigQuery job logs, Cloud Composer logs, or Stackdriver Logging.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure should be followed:

1.  **Immediate Action (Pause New Job):**
    *   **Pause the Airflow DAG:** In the Cloud Composer Airflow UI, locate the `r_ausd_bp_ta_msisdn_dag` and toggle it off to prevent further executions.

2.  **Data Rollback (If Data Corruption/Incorrectness Occurred):**
    *   **BigQuery Time Travel:** If the `sof_ta_msisdn` table was corrupted or populated incorrectly, use BigQuery's time travel feature to restore the table to a state before the problematic job run.
        ```sql
        -- Example: Restore table to a state 1 hour ago
        CREATE OR REPLACE TABLE `my-gcp-project.isbert_dataset.sof_ta_msisdn` AS
        SELECT * FROM `my-gcp-project.isbert_dataset.sof_ta_msisdn` FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
        ```
    *   **Backup Restore:** If BigQuery time travel window is insufficient, restore `sof_ta_msisdn` from the most recent backup.

3.  **Code Rollback (If Code Issues):**
    *   **BigQuery Stored Procedures:**
        *   If previous versions of the BQSPs (`r_ausd_bp_ta_msisdn`, `validate_ddmmyyyy`) are available (e.g., in version control), deploy the last known good versions.
        *   Alternatively, drop the problematic procedures: `DROP PROCEDURE IF EXISTS \`my-gcp-project.isbert_dataset.r_ausd_bp_ta_msisdn\`;`
    *   **Airflow DAG:**
        *   Remove the `cloud_composer_dag.py` file from the Cloud Composer DAGs folder.

4.  **Re-enable Original Job:**
    *   Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh` job in the legacy environment. Ensure it can run successfully and pick up from where it left off or reprocess as needed.

5.  **Analysis and Remediation:**
    *   Thoroughly investigate the root cause of the failure using BigQuery job logs, Cloud Composer logs, and Stackdriver Logging.
    *   Address the identified issues in the BigQuery Stored Procedures or Airflow DAG.
    *   Re-test the corrected migration artifacts in a staging environment before attempting another go-live.