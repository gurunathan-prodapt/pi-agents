# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the `r_ausd_v_ta_notice.ksh` job. The original job comprised KornShell scripts (`r_ausd_v_ta_notice.ksh`, `k_ausd_v_ta_notice.ksh`) responsible for orchestration, parameter handling, and logging, which invoked an Oracle SQL*Plus script (`d_ausd_v_ta_notice.sql`) for core data transformation.

The job has been migrated to Google Cloud Platform, leveraging:
*   **Cloud Composer (Airflow)** for orchestration, replacing the KornShell wrapper and control scripts.
*   **BigQuery Standard SQL Stored Procedures** for data transformation, replacing the Oracle SQL*Plus script.
*   **BigQuery** for all data storage (source, target, and audit tables).
*   **Cloud Logging** for centralized logging and monitoring.

The migration aims to achieve functional equivalence, improved scalability, and integration with GCP's data warehousing ecosystem.

## 2. Generated artifacts

The migration process generated the following key artifacts:

*   **`sp_process_ta_notice.sql`**
    *   **Role**: BigQuery Stored Procedure. This file contains the translated core data transformation logic from `d_ausd_v_ta_notice.sql`, including `v_datum` derivation, `TRUNCATE` and `INSERT` statements, and error handling. It encapsulates the business logic previously spread across the KornShell and Oracle SQL scripts.
*   **`audit_tables.sql`**
    *   **Role**: BigQuery DDL (Data Definition Language) script. This file defines the schema for BigQuery tables used for job execution logging and error tracking (e.g., `job_audit`, `job_error_log`). These tables replace the file-based logging and status management of the legacy system.
*   **`r_ausd_v_ta_notice_dag.py`**
    *   **Role**: Cloud Composer (Airflow) DAG definition. This Python script orchestrates the entire job workflow. It handles environment setup, parameter passing, invokes the `sp_process_ta_notice` BigQuery Stored Procedure, and manages overall job status and logging within the Airflow environment. It replaces the functionality of `r_ausd_v_ta_notice.ksh` and `k_ausd_v_ta_notice.ksh`.
*   **`config.yaml` (or similar configuration file)**
    *   **Role**: Configuration management. This file (or a similar mechanism like Airflow Variables) stores environment-specific parameters such as BigQuery project IDs, dataset names, table names, and other runtime variables required by the DAG and Stored Procedure.

## 3. Key design decisions

The following key design decisions were made during the migration:

*   **Cloud Composer for Orchestration**: Cloud Composer (managed Airflow) was chosen to replace the KornShell orchestration scripts. This provides a robust, scalable, and feature-rich platform for scheduling, monitoring, and managing complex ETL workflows, leveraging Python for task definition and integration with GCP services. This decision centralizes orchestration and standardizes it across migrated jobs.
*   **BigQuery Stored Procedures for Data Transformation**: The core Oracle SQL*Plus logic was translated into a BigQuery Standard SQL Stored Procedure. This approach keeps the data transformation logic close to the data in BigQuery, leveraging its native performance and scalability, and simplifies the orchestration by allowing a single BigQuery task to execute the entire transformation.
*   **BigQuery as Unified Data Platform**: All source, target, and audit tables are consolidated within BigQuery. This eliminates cross-database communication overhead (like Oracle DB links) and provides a single, highly performant, and cost-effective data warehousing solution.
*   **Decoupling from Oracle `pcrs1` via Data Ingestion**: The dependency on the `cds$ta_notice@pcrs1` Oracle database link was resolved by establishing a separate data ingestion pipeline to bring `cds_ta_notice` data into BigQuery. This decouples the ETL job from the operational Oracle system, improving reliability and performance, but introduces a new upstream dependency for data availability in BigQuery.
*   **Reimplementation of Oracle Package Functionality**: Specific functionalities from the `isbert_schema.DWPA_UTIL_SKRIPT` Oracle package (e.g., `TRUNCATE TABLE`) were reimplemented directly using BigQuery DDL statements within the Stored Procedure. For more complex logic, Python operators within the Airflow DAG could be used if necessary, avoiding proprietary Oracle features.
*   **Standardized Logging and Monitoring**: Cloud Logging and BigQuery audit tables replace the legacy file-based logging and `SQL*Plus` spooling. This provides structured, queryable logs and metrics, integrating seamlessly with GCP's monitoring tools.

**Notable Trade-offs:**

*   **Data Ingestion Complexity**: While decoupling from Oracle improves runtime, it necessitates a separate, robust data ingestion pipeline for `cds_ta_notice`, which adds an initial setup and maintenance overhead.
*   **Performance Re-tuning**: Oracle-specific query hints and execution plans do not directly translate to BigQuery. The migrated BigQuery SQL may require specific optimization and tuning to achieve or surpass the performance of the legacy system.
*   **Implicit Transactions**: BigQuery DML statements are transactional by default. This simplifies the `COMMIT` handling but requires careful consideration if the original Oracle logic relied on explicit, multi-statement transaction blocks that need to be replicated.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Create the necessary BigQuery datasets (e.g., `project.dataset`) to house the source, target, and audit tables.
2.  **BigQuery Schema Creation**:
    *   Execute `audit_tables.sql` to create the `job_audit` and `job_error_log` tables in BigQuery.
    *   Create the schemas for the target tables (`sof_ta_notice`, `via`) and source tables (`dwtk_meldungen`, `cds_ta_notice`) in BigQuery, ensuring data type compatibility with the original Oracle schemas.
3.  **Data Ingestion Pipeline Setup**:
    *   Establish and configure the data ingestion pipeline to continuously load data from the Oracle `pcrs1` system's `cds$ta_notice` table into the BigQuery table `project.dataset.cds_ta_notice`. This pipeline must be operational and verified before the job runs.
4.  **IAM Permissions Configuration**:
    *   Grant the Cloud Composer Service Account (or the service account used by the Airflow environment) the necessary IAM roles for BigQuery:
        *   `BigQuery Data Editor` (or more granular roles for specific datasets/tables) to read from source tables and write to target and audit tables.
        *   `BigQuery Job User` to run BigQuery queries and stored procedures.
        *   `Logging Writer` to write logs to Cloud Logging.
5.  **Secrets Management**:
    *   If the data ingestion pipeline or any part of the DAG requires credentials for external systems (e.g., Oracle database access), ensure these are securely stored in Google Secret Manager and accessible by the relevant service accounts.
6.  **BigQuery Stored Procedure Deployment**:
    *   Deploy the `sp_process_ta_notice.sql` BigQuery Stored Procedure to the target BigQuery dataset.
7.  **Cloud Composer DAG Deployment**:
    *   Upload the `r_ausd_v_ta_notice_dag.py` file to the DAGs folder of the Cloud Composer environment.
    *   Configure any necessary Airflow Variables or Connections as defined in `config.yaml` or directly in the DAG.
8.  **Scheduling Configuration**:
    *   Verify that the schedule defined in `r_ausd_v_ta_notice_dag.py` matches the required execution frequency of the legacy job.

## 5. Known gaps & unresolved references

The following items have been identified as known gaps or unresolved references that require further attention:

*   **Complexity Assessment**: The initial assessment of "Medium" complexity and "Semi-Auto" migration bucket was based on manual code review due to the absence of automated complexity metrics. There might be hidden complexities or edge cases not fully captured, which could impact the migration effort or require further refinement.
*   **`DWPA_UTIL_SKRIPT` Package Logic**: The exact implementation details of `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` and any other operations performed by this Oracle package (especially concerning the `VIA` table mentioned in the lineage) need further investigation. While `TRUNCATE TABLE` is straightforward, any more complex logic (e.g., conditional DML, specific merge operations) must be fully understood and accurately translated to BigQuery SQL or Python.
*   **Data Type Mismatches**: Subtle differences in data type behavior between Oracle and BigQuery (e.g., `NUMBER` precision, `DATE` vs. `TIMESTAMP` handling) could lead to data loss or unexpected behavior. A thorough review of all column data types and their BigQuery equivalents is required, potentially involving casting or transformation logic.
*   **Performance Differences**: Oracle-specific performance optimizations (e.g., hints, indexing strategies) do not directly translate to BigQuery. The migrated BigQuery Stored Procedure might require dedicated performance tuning, including query optimization, partitioning, and clustering strategies, to meet or exceed the performance of the legacy job.
*   **`pcrs1` Data Ingestion Strategy**: The detailed strategy for ingesting `cds$ta_notice` data from the remote Oracle `pcrs1` system into BigQuery is a critical upstream dependency. This includes the chosen ingestion tool (e.g., Cloud Data Fusion, Dataflow, database migration service), frequency, error handling, and monitoring. This sub-project must be fully defined and implemented.
*   **Unused Parameters `s:` and `l:`**: The original `r_ausd_v_ta_notice.ksh` script accepts parameters `-s` and `-l` but does not explicitly use them in the provided code snippets. Their intended purpose, if any, needs to be clarified to ensure no functionality is inadvertently lost during migration.

## 6. Validation

Validation of the migrated job involves ensuring functional equivalence, data accuracy, and operational robustness.

**How to run the tests:**

1.  **Deploy Artifacts**: Ensure all generated artifacts (`sp_process_ta_notice.sql`, `audit_tables.sql`, `r_ausd_v_ta_notice_dag.py`) are deployed to their respective GCP environments (BigQuery, Cloud Composer).
2.  **Prepare Source Data**: Ensure the `project.dataset.cds_ta_notice` and `project.dataset.dwtk_meldungen` tables in BigQuery contain representative source data, ideally a snapshot identical to what the legacy job would process.
3.  **Trigger Execution**:
    *   For initial testing, manually trigger the `r_ausd_v_ta_notice_dag` in Cloud Composer.
    *   For scheduled testing, allow the DAG to run according to its defined schedule.
4.  **Monitor Execution**:
    *   Monitor the Cloud Composer UI for DAG and task status.
    *   Check Cloud Logging for detailed job logs and any errors.
    *   Query the BigQuery `job_audit` and `job_error_log` tables for job status and error messages.
5.  **Data Comparison**:
    *   After the BigQuery job completes, compare the data in the target `project.dataset.sof_ta_notice` table with the output of the legacy Oracle job (`sof$ta_notice`) for the same processing period and source data. This can be done using `SELECT * EXCEPT(load_timestamp) FROM ...` or data diff tools.

**What "passing" means:**

*   **Functional Equivalence**: The data in the BigQuery target table (`project.dataset.sof_ta_notice`) is identical to the data produced by the legacy Oracle job for the same input parameters and source data. All filtering, date comparisons, and `is_production` logic must yield the same results.
*   **Data Accuracy**: All transformed columns and derived values (e.g., `v_datum`) are correct and match the legacy system's output.
*   **Job Completion**: The Cloud Composer DAG completes successfully without any task failures.
*   **Logging and Auditing**:
    *   Cloud Logging contains comprehensive logs for the job execution, including start/end times, parameters, and any informational messages.
    *   The BigQuery `job_audit` table accurately reflects the job's execution status (success/failure), start/end times, and any relevant metrics (e.g., rows processed).
    *   In case of simulated errors, the `job_error_log` table correctly captures the error details.
*   **Performance**: The job completes within acceptable performance thresholds, ideally matching or improving upon the execution time of the legacy Oracle job.

## 7. Rollback procedure

In the event that the migrated job fails validation or encounters critical issues in production, the following rollback procedure should be followed:

1.  **Immediate Action**:
    *   **Stop New Job**: Immediately pause or disable the `r_ausd_v_ta_notice_dag` schedule in Cloud Composer to prevent further execution of the migrated job.
2.  **Revert to Legacy System**:
    *   **Re-enable Legacy Job**: Re-enable the scheduling and execution of the original `r_ausd_v_ta_notice.ksh` job on the legacy platform.
3.  **Data State (if necessary)**:
    *   Since the migrated job writes to a new BigQuery target table (`project.dataset.sof_ta_notice`), the data in this table can typically be ignored or truncated if the rollback is initiated. The legacy job will continue populating its own Oracle target table (`sof$ta_notice`).
    *   If, however, the BigQuery target table is intended to be a shared or primary source for downstream systems *immediately* after go-live, and a rollback occurs, a data restoration might be necessary for that BigQuery table to a known good state, or downstream systems must temporarily revert to consuming from the legacy Oracle target.
4.  **Investigation and Remediation**:
    *   Analyze the root cause of the failure using Cloud Logging, BigQuery audit tables, and any available debugging tools.
    *   Address the identified issues in the migrated code or configuration.
    *   Re-test thoroughly in a non-production environment.
5.  **Re-deployment**:
    *   Once the issues are resolved and re-validated, follow the "Manual steps before go-live" and "Validation" procedures to re-deploy and re-activate the migrated job.