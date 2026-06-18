# MIGRATION_NOTES.md

## Migration Job: `finaltestingrepo`

**Date:** 2024-01-23
**Version:** 1.0

---

### 1. Migration Summary

This document details the migration of the `finaltestingrepo` ETL pipeline, a medium-complexity, enterprise-wide data processing system, from its legacy on-premises environment to Google Cloud Platform (GCP) with Google BigQuery as the primary data warehouse.

**Purpose of Migration:**
The primary objective is to modernize the data platform, improve scalability, reduce operational overhead, and leverage GCP's managed services for data processing, storage, and orchestration. The `finaltestingrepo` pipeline supports critical business functions across Customer, Finance, and Retail (Sales) domains, including customer scoring and segmentation, financial GL reconciliation and reporting, and retail sales analytics.

**Legacy Environment Overview:**
The existing pipeline utilizes a diverse and complex technology stack including Oracle 19c databases, Ab Initio graphs, Oracle PL/SQL packages, SQL*Plus scripts, Python scripts, Scala/PySpark jobs, and KornShell scripts, all orchestrated by Automic (UC4) workflows.

**Target Environment Overview:**
The migrated pipeline will leverage GCP services:
*   **Data Warehouse:** Google BigQuery
*   **Data Lake/Staging:** Google Cloud Storage (GCS)
*   **ETL/Transformation:** Dataproc (for Spark/PySpark/Scala Spark), BigQuery SQL, Cloud Data Fusion/Dataflow (for ingestion), Python (Cloud Functions/Cloud Run/Composer).
*   **Orchestration:** Cloud Composer (Apache Airflow)
*   **Configuration/Secrets:** Cloud Composer Variables, Google Secret Manager

**Complexity:** Medium
**Estimated Effort:** This is a significant undertaking involving re-platforming and rewriting logic across multiple technologies. The initial estimate of 0.0h is a placeholder and should be disregarded. A realistic estimate would be several person-months, depending on the team size and experience.

---

### 2. Key Design Decisions

The migration strategy focuses on leveraging GCP's managed services for scalability, cost-efficiency, and reduced operational burden.

**2.1. Target Architecture Principles:**
*   **Cloud-Native First:** Prioritize GCP managed services over self-managed solutions.
*   **Serverless/Managed where possible:** Utilize services like BigQuery, Cloud Composer, and Cloud Storage to minimize infrastructure management.
*   **Cost Optimization:** Employ BigQuery's columnar storage and query pricing, and Dataproc's ephemeral clusters.
*   **Scalability & Performance:** Design for horizontal scalability using Spark on Dataproc and BigQuery's distributed query engine.
*   **Observability:** Integrate with Cloud Logging and Cloud Monitoring.

**2.2. Data Storage and Schema Design:**
*   **Primary Data Warehouse:** Google BigQuery will house all dimensions, facts, and aggregated data.
*   **Staging Layer:** GCS will serve as a landing zone for raw data, and BigQuery staging datasets (`stg_*`) will hold data prior to transformation into the data warehouse.
*   **Schema Conversion:** Oracle table and column names will largely be retained for consistency.
*   **Data Type Mapping:** Standard Oracle data types will be mapped to their BigQuery equivalents (e.g., `NUMBER(P,S)` to `NUMERIC`/`BIGNUMERIC`, `VARCHAR2` to `STRING`, `DATE` to `DATE`, `TIMESTAMP` to `TIMESTAMP`).
*   **Key Management:**
    *   BigQuery does not have native sequences. Surrogate keys will be generated during ETL using techniques like `GENERATE_UUID()` or `ROW_NUMBER()` for new records, while retaining existing Oracle surrogate keys for historical data.
    *   SCD Type 2 versioning will utilize `ROW_NUMBER()` during `MERGE` operations.
    *   Batch IDs can be generated using `FARM_FINGERPRINT()` or `UNIX_MILLIS()`.
*   **Partitioning and Clustering:** All large fact tables and frequently queried dimension tables will be partitioned by a date column (e.g., `TRANSACTION_DATE`, `LOAD_DATE`) and clustered by frequently used join/filter columns to optimize query performance and cost.
*   **Access Control:** Oracle cross-schema grants will be translated to BigQuery dataset-level IAM roles and permissions.

**2.3. Transformation Logic Migration Strategy:**

*   **Oracle PL/SQL Packages (`pkg_*.sql`):**
    *   **SCD Type 2 Logic:** Rewritten using **BigQuery `MERGE` statements** for efficient upsert and history management.
    *   **Dynamic SQL:** Complex dynamic SQL logic will be rewritten in **Python** scripts. These scripts will read metadata from BigQuery configuration tables, construct BigQuery-compliant SQL, and execute it via the `google-cloud-bigquery` client library.
    *   **Iterative Logic/Loops:** Translated to BigQuery stored procedures (for simple cases) or Python scripts orchestrating BigQuery DML.
    *   **Aggregations:** Translated to BigQuery SQL `MERGE` or `INSERT` statements.
    *   **Orchestration Procedures:** Replaced by Cloud Composer DAGs.

*   **SQL*Plus Scripts (`*.sql`):**
    *   SQL queries will be translated to **BigQuery SQL**.
    *   Shell scripting logic (parameter passing, logging, error handling) will be absorbed into **Python operators** within Cloud Composer DAGs.

*   **Ab Initio Graphs (`.mp`, `.xfr`, `.pdl`):**
    *   All Ab Initio graphs will be migrated to **Spark jobs (PySpark or Scala Spark)** running on **Dataproc**.
    *   Common Ab Initio components (e.g., `input_table`, `reformat`, `filter`, `join`, `rollup`, `output_table`) will be mapped to Spark DataFrame API operations and BigQuery connector usage.

*   **Existing Spark Jobs (Scala/PySpark):**
    *   These jobs will be migrated to **Dataproc** with minimal code changes.
    *   Oracle JDBC connections will be replaced with the **Spark BigQuery connector**.
    *   Dynamic SQL within these jobs will be updated to generate BigQuery SQL syntax.

*   **KornShell Scripts (`.ksh`):**
    *   All KornShell scripts will be rewritten in **Python** and orchestrated by **Cloud Composer**.
    *   Orchestration logic, parameter parsing, logging, and error handling will be implemented in Python.
    *   External command calls will be replaced by BigQuery client library calls, `DataprocSubmitJobOperator`, or `PythonOperator` in Airflow.
    *   Retry logic will leverage Airflow's native retry mechanisms or Python libraries.

**2.4. Data Flow and Orchestration:**
*   **Cloud Composer (Apache Airflow)** will be the central orchestrator, replacing Automic (UC4) and KornShell.
*   **Ingestion:** Initial bulk load via Cloud Data Fusion/DMS. Incremental loads via Python scripts (replacing SQL*Plus extracts) or CDC solutions (e.g., Debezium) to GCS, then to BigQuery staging.
*   **Staging:** Data lands in GCS (raw zone) and BigQuery staging datasets (`stg_*`).
*   **Transformation:** Spark jobs on Dataproc and BigQuery SQL transformations process data from staging to the data warehouse.
*   **Data Warehouse:** Final, curated data resides in BigQuery DW datasets (`dw_customer`, `dw_finance`, `dw_retail`).
*   **Cross-Domain Dependencies:** Explicit event-based dependencies (e.g., `FINANCE_GL_CLOSE_COMPLETE`) will be managed using Airflow sensors (e.g., `ExternalTaskSensor` or custom BigQuery table sensors).

**2.5. Configuration Management:**
*   Environment-specific parameters (e.g., from `conf/env_retail.properties`) will be managed using Cloud Composer Variables, Google Secret Manager, or BigQuery configuration tables.

---

### 3. Generated Files

The migration process will generate a suite of new artifacts for the GCP environment. These files will replace the legacy scripts and configurations.

**3.1. BigQuery DDL Scripts (`.sql`)**
*   **Purpose:** Define the schema for all staging (`stg_*`), data warehouse (`dw_*`), and audit (`audit_etl`) tables in BigQuery.
*   **Examples:**
    *   `bq_ddl/stg_customer/stg_customer_profile.sql`
    *   `bq_ddl/dw_customer/dim_customer_crm.sql`
    *   `bq_ddl/dw_finance/fact_gl_balances.sql`
    *   `bq_ddl/audit_etl/etl_job_audit.sql`
    *   (All DDLs provided in the "Target Schema Design" section of the Migration Design Document)

**3.2. PySpark / Scala Spark Transformation Scripts (`.py`, `.scala`)**
*   **Purpose:** Implement the data transformation logic previously handled by Ab Initio graphs, Oracle PL/SQL (complex dynamic parts), and existing Spark jobs. These will run on Dataproc.
*   **Examples:**
    *   `dataproc_jobs/customer/crm_customer_scoring_spark.py` (replaces `crm_customer_scoring.mp`)
    *   `dataproc_jobs/finance/gl_period_close_spark.py` (replaces `gl_period_close.mp`)
    *   `dataproc_jobs/finance/account_processor.scala` (migrated existing Scala)
    *   `dataproc_jobs/sales/retail_daily_sales_spark.py` (replaces `retail_daily_sales.mp`)
    *   `dataproc_jobs/data_quality/retail_data_quality.py` (migrated existing PySpark)
    *   `dataproc_jobs/utils/scd2_processor.py` (reusable SCD Type 2 logic)
    *   `dataproc_jobs/utils/bigquery_connector.py` (Spark-BigQuery interaction utilities)
    *   (The `finaltestingrepo_gcp_migration.py` summary provided in the artifacts represents a modular approach to these PySpark scripts.)

**3.3. BigQuery SQL Transformation Scripts (`.sql`)**
*   **Purpose:** Implement simpler data transformations, aggregations, and SCD Type 2 `MERGE` operations directly within BigQuery.
*   **Examples:**
    *   `bq_sql/dw_customer/merge_dim_customer_crm.sql`
    *   `bq_sql/dw_finance/update_fact_gl_balances.sql`
    *   `bq_sql/dw_retail/insert_fact_daily_sales.sql`
    *   `bq_sql/audit_etl/insert_etl_job_audit.sql`

**3.4. Cloud Composer DAGs (`.py`)**
*   **Purpose:** Orchestrate the end-to-end ETL pipeline, replacing Automic (UC4) workflows and KornShell scripts.
*   **Examples:**
    *   `dags/finance_daily_workflow.py`
    *   `dags/retail_daily_workflow.py`
    *   `dags/crm_weekly_workflow.py`
    *   `dags/common/oracle_extractor_template.py` (reusable Oracle extraction logic)

**3.5. Python Utility Scripts (`.py`)**
*   **Purpose:** Support various tasks such as Oracle data extraction, dynamic SQL generation, metadata management, and custom orchestration logic not directly handled by Airflow operators.
*   **Examples:**
    *   `python_scripts/oracle_extractors/extract_gl_transactions.py`
    *   `python_scripts/dynamic_sql_generator/build_gl_balances_sql.py`
    *   `python_scripts/metadata/crm_lineage_tracker.py` (migrated existing Python)
    *   `python_scripts/config/env_loader.py` (replaces `conf/env_retail.properties` parsing)

**3.6. Configuration Files**
*   **Purpose:** Define parameters for Dataproc clusters, BigQuery connections, and other GCP services.
*   **Examples:**
    *   `configs/dataproc_cluster_templates.yaml`
    *   `configs/bq_connection_details.json`
    *   `configs/airflow_variables.json` (for importing into Cloud Composer)

---

### 4. Test Coverage

Comprehensive testing is crucial for a successful migration. The following categories of tests will be implemented:

**4.1. Unit Tests (PySpark Functions)**
*   **Scope:** Individual PySpark functions and utility modules (e.g., `prepare_scd2_source`, `standardize_string_cols`, `safe_coalesce`).
*   **Framework:** `pytest` with a local SparkSession fixture.
*   **Objective:** Verify the correctness of isolated logic, data transformations, and utility functions.
*   **Key Test Cases (from summary):**
    *   `test_standardize_string_cols_basic`: Ensures trimming and upper-casing.
    *   `test_scd2_merge_sql_new_record`: Verifies correct `INSERT` for new records.
    *   `test_scd2_merge_sql_update_record`: Verifies `UPDATE` of `valid_to` and `is_current`, and `INSERT` of new version.
    *   `test_derive_period_dimension_valid_date`: Checks correct period derivation.
    *   `test_build_dim_account_source_schema`: Validates schema generation.
    *   `test_calculate_gl_variance_basic`: Checks variance calculation.

**4.2. Integration Tests (Spark Jobs, BigQuery SQL, DAGs)**
*   **Scope:** End-to-end execution of individual Spark jobs, BigQuery SQL scripts, and full Airflow DAGs.
*   **Environment:** Dedicated GCP test environment.
*   **Objective:** Verify that components interact correctly, data flows as expected, and transformations produce the desired output when chained together.
*   **Key Test Cases (from summary):**
    *   `test_finance_daily_workflow_full_run`: Executes the entire DAG, verifying all tasks complete successfully.
    *   `test_retail_daily_sales_spark_job_bq_io`: Verifies Spark job reads from and writes to BigQuery correctly.
    *   `test_crm_weekly_workflow_dependency_resolution`: Checks Airflow sensors for external dependencies.
    *   `test_dynamic_sql_generation_and_execution`: Verifies Python script generates and executes correct BigQuery SQL.

**4.3. Data Validation Tests**
*   **Scope:** Comparison of data between the legacy system and the new BigQuery environment.
*   **Tools:** SQL queries, data comparison tools (e.g., `data-diff`), custom Python scripts.
*   **Objective:** Ensure data integrity, completeness, and accuracy post-migration.
*   **Key Test Cases (from summary):**
    *   `test_row_counts_match_after_full_load`: Compares row counts for all tables.
    *   `test_checksums_match_for_key_columns`: Verifies data integrity using checksums on critical columns.
    *   `test_aggregate_values_match_for_facts`: Compares sums, averages, min/max for financial and sales facts.
    *   `test_scd2_history_correctness`: Validates `valid_from`, `valid_to`, `is_current` logic for SCD Type 2 dimensions.
    *   `test_data_quality_rules_applied_correctly`: Verifies DQ results in `audit_etl.dq_results`.
    *   `test_cross_domain_data_consistency`: Validates referential integrity and data consistency across domains.

**4.4. Performance Tests**
*   **Scope:** Execution time and resource utilization of critical ETL jobs and queries.
*   **Tools:** Cloud Monitoring, BigQuery query history, Dataproc logs.
*   **Objective:** Ensure the migrated pipeline meets or exceeds performance SLAs and operates within cost targets.
*   **Key Test Cases (from summary):**
    *   `test_daily_sales_load_within_sla`: Measures execution time of `retail_daily_workflow`.
    *   `test_gl_period_close_resource_utilization`: Monitors Dataproc cluster usage during finance jobs.
    *   `test_bigquery_query_performance_for_reporting`: Benchmarks key analytical queries.

**4.5. Security and Compliance Tests**
*   **Scope:** IAM roles, data encryption, network access, audit logging.
*   **Tools:** GCP IAM policies, Cloud Audit Logs, Security Command Center.
*   **Objective:** Verify adherence to security policies and compliance requirements.
*   **Key Test Cases (from summary):**
    *   `test_iam_roles_least_privilege`: Confirms service accounts have only necessary permissions.
    *   `test_data_encryption_at_rest_and_in_transit`: Verifies encryption settings.
    *   `test_audit_logging_enabled_for_bq_and_gcs`: Checks for proper logging.

**4.6. Operational Readiness Tests**
*   **Scope:** Monitoring, alerting, error handling, restartability, logging.
*   **Tools:** Cloud Monitoring, Cloud Logging, Airflow UI.
*   **Objective:** Ensure the pipeline is robust, observable, and easily manageable in production.
*   **Key Test Cases (from summary):**
    *   `test_alerting_on_failed_dag_runs`: Verifies notifications for failures.
    *   `test_job_restartability_from_failure`: Checks if DAGs can resume from a failed task.
    *   `test_logging_granularity_and_accessibility`: Ensures logs are detailed and easily retrievable.

---

### 5. Validation Steps

The following steps outline the process for validating the migrated `finaltestingrepo` pipeline.

1.  **Pre-Migration Data Profiling (Legacy System):**
    *   Profile all source and target tables in the legacy Oracle environment.
    *   Capture row counts, distinct counts, min/max values, sums, averages, and data distributions for key columns.
    *   Document data types, nullability, and primary/foreign key relationships.
    *   Identify and document any known data quality issues or anomalies.

2.  **Schema Validation (BigQuery DDL):**
    *   Deploy all generated BigQuery DDL scripts to a dedicated test environment.
    *   Verify that all tables, columns, data types, partitioning, and clustering are correctly applied in BigQuery, matching the design document.
    *   Confirm that primary key constraints (logical, as BigQuery doesn't enforce them) and foreign key relationships are understood and handled in the ETL logic.

3.  **Initial Data Load Validation (Historical Data):**
    *   Perform a one-time historical data load from Oracle to BigQuery.
    *   **Row Count Comparison:** Compare total row counts for each table in BigQuery against the corresponding Oracle tables.
    *   **Data Sample Verification:** Randomly sample records from both systems and visually inspect for data accuracy.
    *   **Aggregate Value Comparison:** Compare aggregate values (SUM, AVG, MIN, MAX) for numerical columns and distinct counts for categorical columns.
    *   **Checksum Validation:** For critical tables, calculate and compare checksums (e.g., MD5 hashes of concatenated column values) to ensure data integrity.

4.  **Incremental Data Load Validation (Daily/Weekly Runs):**
    *   Execute the migrated Cloud Composer DAGs for a full cycle (e.g., daily, weekly).
    *   **Source-to-Staging:** Verify that data extracted from Oracle sources lands correctly in GCS and BigQuery staging tables.
    *   **Staging-to-DW:** Validate that transformations (SCD Type 2, aggregations, business logic) are applied correctly, and data moves from staging to the data warehouse.
    *   **Data Comparison:** Perform daily/weekly data comparisons (row counts, aggregates, sample data) between the legacy and new systems for newly processed data.
    *   **SCD Type 2 Logic:** Specifically validate that `valid_from`, `valid_to`, `is_current`, and `version_num` columns are correctly managed in dimension tables.
    *   **Lineage Tracking:** Verify that the `crm_lineage_tracker.py` script correctly records lineage information in BigQuery.

5.  **Functional Validation (Business Logic):**
    *   **Report Reconciliation:** Compare key business reports and dashboards generated from the legacy system with those generated from the new BigQuery environment.
    *   **Business Rule Verification:** Confirm that all business rules (e.g., customer scoring algorithms, GL reconciliation logic, sales aggregation rules) are correctly implemented in the migrated ETL.
    *   **Edge Case Testing:** Test with known edge cases, null values, and invalid data to ensure robust error handling.

6.  **Performance Validation:**
    *   Monitor the execution times of all DAGs and individual tasks in Cloud Composer.
    *   Analyze BigQuery query performance and Dataproc job metrics (CPU, memory, I/O).
    *   Compare performance against established SLAs and legacy system benchmarks.
    *   Identify and optimize any performance bottlenecks.

7.  **Operational Validation:**
    *   Verify that Cloud Composer DAGs are scheduled correctly and handle retries as expected.
    *   Confirm that logging (Cloud Logging) and monitoring (Cloud Monitoring) are functioning, and alerts are triggered for failures.
    *   Test the ability to pause, resume, and backfill DAGs.
    *   Review `audit_etl.etl_job_audit` for accurate job status and metrics.

---

### 6. Rollback Approach

A robust rollback strategy is essential to minimize business disruption in case of unforeseen issues during or after the migration.

**6.1. Principle: Parallel Run and Cutover**
*   The legacy ETL pipeline will remain fully operational and continue processing data during the initial deployment and validation phases of the GCP pipeline.
*   Data will be processed in parallel by both systems for a defined period (e.g., 2-4 weeks) to allow for thorough comparison and validation.
*   A formal cutover will only occur once the GCP pipeline is fully validated and deemed stable.

**6.2. Rollback Triggers:**
*   Significant data discrepancies detected during validation.
*   Failure to meet performance SLAs.
*   Critical business reports showing incorrect data.
*   Repeated or unresolvable pipeline failures.
*   Unacceptable operational overhead or cost overruns.

**6.3. Rollback Steps:**

1.  **Stop New Data Ingestion to GCP (if applicable):** Immediately halt any new data feeds or incremental loads directed towards the GCP BigQuery environment.
2.  **Revert Reporting/Consumption:**
    *   Direct all downstream reporting, analytics, and data consumption systems back to the legacy Oracle data warehouse.
    *   This is the primary and fastest rollback mechanism, as the legacy system remains operational.
3.  **Isolate GCP Environment:**
    *   Disable or pause all Cloud Composer DAGs related to the `finaltestingrepo` pipeline in GCP.
    *   If necessary, revert any changes made to source system configurations that redirected data to GCP.
4.  **Data State:**
    *   The legacy Oracle database will contain the authoritative data up to the point of rollback.
    *   The BigQuery environment will contain the partially migrated or incorrect data. This data can be archived or deleted after analysis of the rollback cause.
5.  **Root Cause Analysis:**
    *   Conduct a thorough investigation to identify the root cause of the migration failure.
    *   Develop a revised migration plan addressing the identified issues.
6.  **Re-plan and Re-execute:**
    *   Once the issues are resolved and the plan updated, the migration process can be re-initiated.

**6.4. Key Considerations for Rollback:**
*   **Data Divergence:** During parallel run, ensure mechanisms are in place to quickly identify and quantify any data divergence between the legacy and new systems.
*   **Minimal Impact:** The rollback strategy aims to minimize impact on business operations by quickly reverting to the known-good legacy system.
*   **Communication:** Clear communication with stakeholders is crucial during a rollback event.

---

### 7. Reviewer Checklist

This checklist is for reviewers to ensure all aspects of the `finaltestingrepo` migration to GCP are thoroughly evaluated.

**7.1. Design & Architecture Review:**
*   [ ] Has the target GCP architecture been clearly defined and documented?
*   [ ] Are the chosen GCP services appropriate for the workload (BigQuery, Dataproc, Composer, GCS)?
*   [ ] Is the BigQuery schema design (datasets, tables, data types, partitioning, clustering) optimized for performance and cost?
*   [ ] Is the strategy for handling surrogate keys and SCD Type 2 dimensions clear and robust?
*   [ ] Are cross-domain dependencies and data sharing mechanisms well-defined?
*   [ ] Is the approach for migrating each legacy technology (PL/SQL, Ab Initio, SQL*Plus, KornShell) clearly articulated and justified?
*   [ ] Are security considerations (IAM, encryption, network) addressed in the design?
*   [ ] Is the configuration management strategy sound (Composer Variables, Secret Manager)?

**7.2. Code Review:**
*   [ ] Are all generated BigQuery DDL scripts accurate and complete?
*   [ ] Do the PySpark/Scala Spark scripts correctly implement the transformed logic from Ab Initio and existing Spark jobs?
*   [ ] Are BigQuery SQL scripts efficient and BigQuery-idiomatic?
*   [ ] Are Cloud Composer DAGs well-structured, idempotent, and follow Airflow best practices?
*   [ ] Is the Python code for Oracle extraction and dynamic SQL generation robust and secure?
*   [ ] Is error handling and logging implemented consistently across all code artifacts?
*   [ ] Is the code well-commented and readable?
*   [ ] Are there any hardcoded values that should be externalized to configuration?

**7.3. Test Coverage Review:**
*   [ ] Is there adequate unit test coverage for core transformation logic and utility functions?
*   [ ] Are integration tests in place to validate the end-to-end data flow and component interactions?
*   [ ] Are data validation tests comprehensive, covering row counts, aggregates, checksums, and data accuracy?
*   [ ] Are performance tests planned for critical ETL jobs and queries?
*   [ ] Are security and operational readiness tests included in the test plan?
*   [ ] Is the test data representative of production data characteristics?

**7.4. Validation Steps Review:**
*   [ ] Is the pre-migration data profiling plan sufficient to establish a baseline?
*   [ ] Are the post-migration data comparison methods robust and automated where possible?
*   [ ] Is there a clear plan for functional validation by business users?
*   [ ] Are performance monitoring and benchmarking activities defined?
*   [ ] Is the operational readiness checklist complete?

**7.5. Rollback Plan Review:**
*   [ ] Is the rollback strategy clearly defined and understood?
*   [ ] Does the plan ensure the legacy system remains operational and can be reverted to quickly?
*   [ ] Are the triggers for initiating a rollback clearly stated?
*   [ ] Is the impact of a rollback on data consistency and downstream systems understood?
*   [ ] Are communication protocols for a rollback event established?

**7.6. Documentation & Handover:**
*   [ ] Is the `MIGRATION_NOTES.md` document complete, clear, and easy to understand?
*   [ ] Is there a plan for updating runbooks and operational documentation?
*   [ ] Is there a training plan for the operations team on the new GCP environment?
*   [ ] Are all dependencies (internal and external) clearly identified and managed?

---