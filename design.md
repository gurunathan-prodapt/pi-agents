# Migration Design — vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2

## 1. Purpose & Scope

The KornShell script `r_exis_v2` acts as a "Next Generation Exporter" framework for a Data Warehouse (DWH). Its primary purpose is to orchestrate the extraction of data from an Oracle database, apply various transformations, and then export this data into files. It supports complex functionalities such as configuration-driven execution, partitioning (both file and SQL-based), parallel processing, incremental loading, and diverse file distribution methods (SCP, SFTP, compression, email). The script is designed for flexibility and performance in data export operations for both cube and normal exporter requirements.

## 2. Source Inventory

The job consists of a single primary source file:

*   **File:** `vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2`
    *   **Technology:** KornShell script (ksh_dwh)
    *   **Complexity Tier:** complex
    *   **Automation Bucket:** manual
    *   **Summary:** This script serves as a unified framework for exporting data from Oracle to files. It dynamically constructs and executes SQL queries via `sqlplus`, processes data streams using standard Unix utilities (`nawk`, `sed`, `perl`), and manages file distribution through `scp`, `sftp`, and `mailx`. It includes intricate logic for parameter parsing, timestamp handling, incremental data processing, and robust error handling integrated with DWH monitoring routines. The script heavily relies on dynamic evaluation (`eval`) and shell piping for its control flow and data manipulation.

## 3. Target Architecture

The migration targets a Google Cloud Platform (GCP) architecture leveraging BigQuery for data processing and storage, and Cloud Composer (Apache Airflow) for orchestration.

*   **Orchestration:** **Cloud Composer (Apache Airflow)** will manage the end-to-end workflow. This includes reading configuration, scheduling tasks, handling parallel execution, and integrating with other GCP services. The complex orchestration logic, dynamic parameter resolution, and error handling of the original shell script will be re-engineered into Python-based Airflow DAGs.
*   **Data Storage & Processing:** **BigQuery** will be the primary data warehouse for all extracted and transformed data.
    *   **SQL Transformations:** All `OUTPUT_SQL`, `PRE_SQL`, `POST_SQL` and other SQL-based data extraction and transformation logic will be re-written as BigQuery SQL.
    *   **Configuration & Metadata:** Configuration elements (originally in config files) will be stored in BigQuery control tables. Metadata related to job execution, partitioning, and status (e.g., `dwh$ta_k_meldungen`) will also reside in BigQuery.
    *   **UDFs/Stored Procedures:** Helper functions from the shell script (e.g., `handletimestamps`, `fillattribs`) will be re-implemented as BigQuery User-Defined Functions (UDFs) or Stored Procedures where applicable.
*   **Data Transformation (Complex Text Processing):** For non-SQL data manipulations currently performed by `nawk`, `sed`, `perl` that are too complex for direct BigQuery SQL functions, **Dataflow** jobs (using Apache Beam in Python) will be utilized.
*   **File Staging & Archiving:** **Google Cloud Storage (GCS)** will serve as the landing zone for exported files and the source for external distribution. Lifecycle policies will be applied for archiving (`compress`) and retention (`rm`) of files.
*   **External File Distribution:**
    *   **Cloud Storage Transfer Service:** For scheduled, high-volume transfers to/from external SFTP/SCP.
    *   **Cloud Run:** For custom logic to push files from GCS to external SFTP/SCP servers or send emails based on events triggered by Airflow.
*   **Logging & Monitoring:** **Cloud Logging** and **Cloud Monitoring** will capture and provide insights into the execution of Airflow DAGs, BigQuery jobs, and Dataflow pipelines.

## 4. Data Flow & Lineage

The core data flow involves extracting data from an Oracle source, processing it, and outputting it to files, potentially for further distribution.

**Original (Legacy) Flow:**

1.  **Configuration Loading:** The script starts by loading configuration from `p_ConfigFile` and `p_DefaultFile`.
2.  **Parameter Resolution:** Command-line parameters and placeholders within the configuration are resolved.
3.  **Timestamp & Partitioning Logic:** `handletimestamps`, `getsubintervalls`, `getsqlsplits` determine execution date ranges and partitioning strategies (both SQL and file partitions). Oracle metadata views (`DBA_TAB_PARTITIONS`, `ALL_TABLES`, `ALL_SYNONYMS`) are queried for this.
4.  **SQL Execution & Extraction:**
    *   `PRE_SQL` is executed.
    *   `OUTPUT_SQL` is executed via `sqlplusspool` (wrapped `sqlplus -S $DBCONNECT`), performing the main data extraction from `DWH$TA_K_MELDUNGEN` or other Oracle tables.
    *   `POST_SQL` is executed.
    *   This SQL execution can be parallelized (`PARALLELSQL`).
5.  **Data Processing & File Creation:**
    *   The output of `sqlplus` is piped through various `nawk`, `sed`, `perl` commands for reformatting, filtering, and incremental merging.
    *   The processed data is written to a `DESTINATION` file (e.g., `/path/to/output.csv`).
    *   File creation can be parallelized (`PARALLELFILE`).
6.  **Metadata & Ready Files:** `collectmetadata` is called, and `DESTINATION_RDY` ready files are created, containing metadata.
7.  **Distribution:** The output file(s) are distributed using `scp`, `sftp`, compressed (`compress`), moved (`mv`), copied (`cp`), or emailed (`mailx`).
8.  **Logging & Status:** Execution status and detailed logs are written to `$LogDatei` and `dwh$ta_k_meldungen` via `DWMSG_` functions.

**Target (GCP) Flow:**

1.  **Airflow DAG Trigger:** An Airflow DAG in Cloud Composer is triggered (on schedule or manually).
2.  **Configuration & Parameter Loading:** The Airflow DAG loads configuration from BigQuery control tables and resolves job parameters.
3.  **BigQuery SQL Orchestration (Dataform):**
    *   Airflow tasks trigger Dataform jobs or execute BigQuery Stored Procedures for `PRE_SQL`, `OUTPUT_SQL`, and `POST_SQL`.
    *   Dataform manages the BigQuery SQL assets, including dynamic SQL generation (parameterized queries).
    *   Partitioning logic will be directly integrated into BigQuery SQL queries or handled by Airflow tasks orchestrating parameterized queries.
    *   Oracle source tables would have been migrated to BigQuery.
4.  **Data Processing (BigQuery/Dataflow):**
    *   The output of BigQuery SQL queries (replacing Oracle `sqlplus` extraction) is either:
        *   Further processed by BigQuery SQL (for simple `nawk`/`sed`-like transformations).
        *   Exported to a temporary GCS bucket, then processed by a Dataflow job (for complex `nawk`/`sed`/`perl` logic), and finally loaded back into BigQuery or saved to GCS.
5.  **File Generation (GCS):** Final output data is exported from BigQuery to GCS using `EXPORT DATA` commands, creating files in the designated `DESTINATION` (GCS bucket/path).
6.  **Metadata & Ready Files:** BigQuery Stored Procedures or Python tasks update metadata tables in BigQuery for "ready file" information.
7.  **External Distribution:**
    *   Airflow tasks enqueue messages to a Pub/Sub topic or directly call Cloud Run services to handle `scp`/`sftp` transfers to external systems, compression, or email notifications.
    *   Cloud Storage lifecycle policies replace `compress` and `rm` for long-term storage management.
8.  **Logging & Monitoring:** All Airflow tasks, BigQuery jobs, and Dataflow jobs emit logs to Cloud Logging. Metrics are pushed to Cloud Monitoring.

## 5. Transformation Logic

The KornShell script employs various transformation patterns that need to be re-engineered for BigQuery and other GCP services:

*   **Dynamic SQL Substitution (`fillattribs`, `handletimestamps`):**
    *   **Legacy:** Shell script dynamically constructs SQL queries and commands based on configuration values and runtime variables (e.g., `SYSDATE`, `FROM`, `TO`, `SEQ`). Heavy use of `eval`.
    *   **Target:** BigQuery parameterized queries managed by Dataform. Airflow will pass parameters to Dataform. `eval` will be eliminated. Complex variable substitution logic will be re-implemented in Python within Airflow or as BigQuery UDFs/Stored Procedures.
*   **Shell AWK/SED/Perl Reformatting (`nawk`, `sed`, `perl` pipes):**
    *   **Legacy:** Extensive use of `nawk`, `sed`, `perl` for string manipulation, reformatting, filtering, and column reshaping within shell pipes.
    *   **Target:** For simple cases, BigQuery SQL functions like `REGEXP_REPLACE`, `REGEXP_EXTRACT`, `SPLIT`, `SUBSTR`, `TRIM`, etc. For complex, multi-stage text processing, **Dataflow** (Python/Apache Beam) will be used to ensure maintainability and scalability.
*   **Oracle SQL Extraction (`sqlplusspool`, `executeSQLfromnode`):**
    *   **Legacy:** Direct `sqlplus` calls to extract data from Oracle. Includes specific Oracle functions and metadata queries.
    *   **Target:** Re-write as native BigQuery SQL queries. If Oracle-specific functions are used, equivalent BigQuery functions must be identified or custom UDFs created. Dataform will manage these SQL assets.
*   **Incremental Merge Logic:**
    *   **Legacy:** Uses shell logic (`nawk`, `grep`) to filter records based on timestamps (`TIMESTAMP_CSV`, `DELETE_OLDER`, `DELETE_NEWER`) from existing files for incremental updates.
    *   **Target:** BigQuery `MERGE` statements using staging tables. An Airflow task will orchestrate the `MERGE` operation.
*   **Parallel Processing (`merger`, `filepartition`, `sqlpartition`, `PARALLELFILE`, `PARALLELSQL`):**
    *   **Legacy:** Custom shell functions and background processes (`&`) combined with named pipes (`mkfifo`) for parallel execution and merging of streams.
    *   **Target:** Airflow's native parallelism features (e.g., `KubernetesPodOperator`, `BashOperator`, `PythonOperator` with `ThreadPoolExecutor` or `ProcessPoolExecutor`). Airflow tasks will be designed to run concurrently as needed, often triggering independent Dataform or Dataflow jobs.
*   **File Distribution (`scp`, `sftp`, `compress`, `mailx`, `mv`, `cp`, `rm`):**
    *   **Legacy:** Direct use of Unix commands for file operations.
    *   **Target:** Replaced by GCP services:
        *   `scp`/`sftp`: Cloud Storage Transfer Service or custom Cloud Run service for external interactions.
        *   `compress`: GCS object lifecycle management for archiving.
        *   `mv`/`cp`/`rm`: GCS object operations, either via Airflow GCS operators or GCS object lifecycle management.
        *   `mailx`: Airflow email operators, potentially integrating with Cloud Functions and SendGrid for robust email notifications.
*   **Configuration Parsing (`parser_getnode`, `parser_filattrib`, `parser_filfile`):**
    *   **Legacy:** Custom shell functions to parse and extract values from structured configuration files.
    *   **Target:** Configuration will be stored in BigQuery tables or Airflow Variables. Python functions within Airflow will be responsible for reading and interpreting this configuration.

## 6. External Dependencies

*   **Oracle Database:**
    *   **Legacy Use:** Source data extraction (tables: `DWH$TA_K_MELDUNGEN`, `DBA_TAB_PARTITIONS`, `ALL_TABLES`, `ALL_SYNONYMS`).
    *   **Target Replacement:** Initial bulk migration of historical data to BigQuery. Ongoing incremental data ingestion from Oracle to BigQuery using BigQuery Data Transfer Service, Datastream, or custom Dataflow pipelines. Oracle metadata lookups will either be re-implemented against BigQuery system views or against replicated Oracle metadata tables in BigQuery.
*   **SFTP/SCP Servers:**
    *   **Legacy Use:** Distribution of exported files to external systems.
    *   **Target Replacement:** Files will be exported to Google Cloud Storage. Cloud Storage Transfer Service will be used for transferring files to/from external SFTP/SCP endpoints. For more complex logic or immediate push, Cloud Run services can be developed to initiate transfers from GCS.
*   **Email System (via `mailx`):**
    *   **Legacy Use:** Sending email notifications upon job completion or failure.
    *   **Target Replacement:** Airflow email operators integrated with an email service (e.g., SendGrid, Mailgun) through Cloud Functions or directly from Airflow. Cloud Monitoring alerts can also trigger notifications.
*   **Local Filesystem (for logs, temporary files, configs):**
    *   **Legacy Use:** `$LogDatei`, temporary `.working` files, named pipes (`mkfifo`), config files (`k_exis_v2_defaults.cfg`).
    *   **Target Replacement:**
        *   Logs: Cloud Logging.
        *   Temporary files: Google Cloud Storage for transient data.
        *   Config files: BigQuery control tables or Airflow Variables, potentially stored as objects in GCS.
        *   Named pipes and in-memory streams: Replaced by BigQuery tables, Dataflow pipelines, or Airflow's XComs and intermediate GCS files for passing data between tasks.

## 7. Unresolved / Risks

*   **Complex Shell Logic and Dynamic `eval`:** The script makes extensive use of `eval` and complex shell piping for dynamic command construction. Replicating this exact behavior in a modern, maintainable GCP environment is challenging and often undesirable due to security and complexity. This requires a complete re-engineering of the logic into structured Python and BigQuery components.
*   **Custom `ksh_dwh` Utilities:** The script relies on custom sourcing of several KornShell utility scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_parser.ksh`). The functionalities of these helper scripts (error reporting, parameter parsing, date utilities) must be fully understood and re-implemented in Python for Airflow or BigQuery for specific data transformations.
*   **Performance Characteristics of Parallelism:** The shell script's custom parallel processing (`merger`, `filepartition`, `sqlpartition`) might have specific performance characteristics due to its low-level control. The new Airflow/BigQuery/Dataflow architecture needs careful design and testing to match or improve upon this performance.
*   **Configuration Migration Complexity:** The configuration is deeply intertwined with the script's execution logic. Migrating this to BigQuery tables requires a clear schema definition for storing node-based configurations and a robust parsing mechanism in Python.
*   **Legacy `DWMSG_*` Routines:** The custom DWH messaging and status update routines need to be mapped to GCP's logging and monitoring systems (Cloud Logging, Cloud Monitoring, BigQuery audit tables) to ensure comparable visibility and operational control.
*   **Oracle `dbms_application_info`:** The call to `dbms_application_info.set_module` is Oracle-specific. Its purpose needs to be re-evaluated; if it's for auditing/tracking, similar functionality should be implemented in BigQuery audit tables.
*   **`manual` Migration Bucket:** The classification as "manual" and "complex" indicates a significant re-engineering effort, not a direct automated conversion. This necessitates thorough manual analysis, design, development, and testing.

## 8. Build Plan

This migration will be a re-engineering effort, building components primarily in Python (for Airflow and Dataflow) and BigQuery SQL (managed by Dataform).

1.  **Foundational Setup (Phase 1: Shared Services)**
    *   **BigQuery Control & Audit Tables:** Define and implement BigQuery schemas for:
        *   `exporter_config`: To store migrated job configurations (nodes like `OUTPUT_SQL`, `META`, `DISTRIBUTION`).
        *   `exporter_log`: To store detailed job logs and status messages.
        *   `exporter_status`: To track overall job status, last run details, and error messages.
        *   `exporter_distribution_queue`: A queue table for external distribution tasks.
    *   **BigQuery Helper Functions:** Develop BigQuery UDFs and Stored Procedures for common logic:
        *   `handletimestamps_bq`: Date calculation and formatting.
        *   `fillattribs_bq`: Attribute filling/placeholder resolution.
        *   `get_file_partitions_bq`, `get_sql_splits_bq`: Partitioning logic.
        *   `execute_sql_node_bq`: Generic SQL execution from config nodes.
        *   Functions for evaluating `INITIAL_IF`, `INITIAL_IF_DIFF`, `INITIAL_IF_CHANGE_TABLE`.
    *   **GCS Bucket Setup:** Create GCS buckets for:
        *   Landing exported data files.
        *   Temporary staging for Dataflow.
        *   Logs (optional, if not using Cloud Logging directly).

2.  **Configuration Migration (Phase 2: Data Migration)**
    *   **Job Configuration:** Manually extract and load the `r_exis_v2` configuration (from `k_exis_v2_defaults.cfg` and any specific job config files) into the `exporter_config` BigQuery table. Ensure all nodes like `OUTPUT_SQL`, `PRE_SQL`, `POST_SQL`, `DISTRIBUTION`, `META`, etc., are correctly structured.
    *   **Oracle Source Data Migration:** Plan and execute the migration of historical data from Oracle tables (`DWH$TA_K_MELDUNGEN`, source tables for `OUTPUT_SQL`) to BigQuery. Set up ongoing incremental data transfer.

3.  **Core ETL Logic Re-engineering (Phase 3: Development)**
    *   **Dataform Project:** Create a Dataform project for `r_exis_v2`.
    *   **BigQuery SQL Assets:** Translate `PRE_SQL`, `OUTPUT_SQL`, `POST_SQL` from the configuration into Dataform SQL models or BigQuery Stored Procedures. Parameterize these queries to accept `FROM`, `TO`, `SQLSPLIT`, etc., as inputs.
    *   **Complex Transformation Dataflow:** For intricate `nawk`/`sed`/`perl` logic, develop Python-based Apache Beam pipelines in Dataflow.
    *   **Incremental Merge:** Implement the incremental merge logic using BigQuery `MERGE` statements within Dataform.

4.  **Orchestration (Phase 4: Airflow DAG Development)**
    *   **Airflow DAG:** Develop an Airflow DAG (`r_exis_v2_dag.py`) using Python.
    *   **Tasks:**
        *   **`PythonOperator`:** For initial parameter loading, configuration parsing from BigQuery, and dynamic task generation.
        *   **`DataformOperator` (or `BigQueryOperator` with `EXECUTE IMMEDIATE`):** To run Dataform jobs or BigQuery Stored Procedures for SQL extraction and transformation.
        *   **`DataflowTemplatedJobOperator`:** To submit Dataflow jobs for complex text processing.
        *   **`GCSToGCSOperator` / `GCSDeleteOperator`:** For managing files in GCS.
        *   **`PubSubPublishOperator`:** To publish messages to a Pub/Sub topic for external distribution.
        *   **`CloudRunOperator` (Custom):** To invoke Cloud Run services for SFTP/SCP or email.
        *   **`BigQueryInsertJobOperator`:** To update `exporter_log` and `exporter_status` tables.
        *   **Branching/Looping:** Use Airflow's native control flow for file and SQL partitioning.

5.  **External Integration (Phase 5: External Services)**
    *   **Cloud Run Services:** Develop small Python-based Cloud Run services for:
        *   SFTP/SCP file push (reading from GCS, pushing to external SFTP/SCP).
        *   Email notification (reading from Pub/Sub, sending email).
    *   **Cloud Storage Transfer Jobs:** Configure necessary Cloud Storage Transfer Service jobs for any required external transfers.

6.  **Testing & Deployment (Phase 6)**
    *   **Unit Tests:** For all Python functions, BigQuery UDFs, and Stored Procedures.
    *   **Integration Tests:** Test the end-to-end Airflow DAG, including interactions with BigQuery, GCS, Dataflow, and Cloud Run.
    *   **User Acceptance Testing (UAT):** Validate output data against legacy system output.
    *   **Deployment:** Deploy Dataform project, Airflow DAG, and Cloud Run services to respective GCP environments.

**Build Output:**

The migration effort will produce the following artifacts:

*   `exporter_config.sql`, `exporter_log.sql`, `exporter_status.sql`, `exporter_distribution_queue.sql`: BigQuery DDLs for control and audit tables.
*   `handletimestamps_bq.sql`, `fillattribs_bq.sql`, `get_file_partitions_bq.sql`, etc.: BigQuery UDFs and Stored Procedure DDLs.
*   `r_exis_v2_sql_model_output.sql`, `r_exis_v2_sql_model_pre.sql`, `r_exis_v2_sql_model_post.sql`: Dataform SQL models/BigQuery SQL scripts for the main ETL logic.
*   `r_exis_v2_dataflow_pipeline.py`: Python code for Dataflow if complex text processing is required.
*   `r_exis_v2_dag.py`: Python code for the Airflow DAG.
*   `external_distributor_cloud_run.py`, `sftp_config.yaml`: Python code and configuration for Cloud Run services handling external file distribution.