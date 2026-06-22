# Migration Design — vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2

## 1. Purpose & Scope
This KornShell script (`r_exis_v2`) serves as a unified exporter framework within the Data Warehouse (DWH) environment. Its primary objective is to facilitate the export of data for both general export requirements and cube-specific data needs. The script is designed to provide flexibility, improve performance, and offer comprehensive control over the data export process. This includes parsing configuration, resolving timestamps, managing initial versus incremental runs, executing SQL queries against an Oracle database, creating output files (potentially partitioned and in parallel), generating ready-files, performing post-processing steps, and distributing the final output through various mechanisms (SCP, SFTP, email, compression, move, copy, delete).

The scope of this migration is to re-implement the functionality of `r_exis_v2` on Google Cloud Platform, leveraging BigQuery for data processing and storage, and cloud-native services for orchestration, external interactions, and file management.

## 2. Source Inventory
*   **File**: `vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2`
*   **Technology**: KornShell Script
*   **Complexity Tier**: complex
*   **Automation Bucket**: manual
*   **Notes**: The script is a highly parameterized and modular KornShell script, making extensive use of shell functions, `nawk`, `perl`, `sed`, and `sqlplus` for dynamic SQL execution and data manipulation. It orchestrates complex data extraction, partitioning, and distribution logic. The `loc-escalated` flag indicates a large codebase that might require careful, segmented migration.

## 3. Target Architecture
The target architecture in BigQuery will involve:

*   **BigQuery Stored Procedures**: To encapsulate the core orchestration logic, parameter handling, conditional branching, and looping. Each major function within the original KornShell script (e.g., `exportcore`, `filepartition`, `sqlpartition`) will likely translate into a distinct BigQuery Stored Procedure or a set of modular procedures.
*   **BigQuery Tables**:
    *   **Configuration Tables**: To store job-specific and default configuration parameters currently defined in external config files. Examples: `dataset.config_kv`.
    *   **Job History / Audit Tables**: To track job execution metadata, status, incremental run information, and sequence numbers. Examples: `dataset.job_history`, `dataset.export_audit`.
    *   **Control Tables**: For managing dynamic aspects like sequence numbers, initial conditions, and change detection snapshots. Examples: `dataset.export_control`.
    *   **Staging Tables**: For intermediate data storage if required during complex transformations or partitioning.
    *   **Destination/Ready-File Tracking Tables**: To manage the metadata of generated output files and ready-files. Examples: `dataset.export_distribution`, `dataset.export_readyfiles`.
*   **Cloud Storage**: For staging exported data files before distribution, and as the primary destination for data that would historically be written to the local filesystem or transferred via SCP/SFTP. This will replace direct filesystem operations.
*   **Cloud Run / Cloud Functions / Cloud Workflows**: For handling external dependencies and non-SQL operations:
    *   **File transfer**: Replacing SCP and SFTP for moving data to external systems.
    *   **File manipulation**: Replacing `compress`, `move`, `copy`, `delete` commands on files within Cloud Storage.
    *   **Email notifications**: Replacing `mailx` for sending email attachments.
    *   **External system calls**: If any specific external system interactions remain outside of direct BigQuery capabilities.
*   **BigQuery SQL**: For all data extraction, transformation, and loading (ETL) logic, replacing the `sqlplus` calls and embedded SQL.
*   **Python UDFs (Optional)**: For highly specialized string or date parsing logic if not directly replicable in BigQuery SQL, though this should be minimized.

## 4. Data Flow & Lineage
The original data flow is highly dynamic and depends on runtime parameters and configuration files. In the migrated architecture, this flow will be orchestrated primarily by BigQuery Stored Procedures, with external services handling file distribution.

1.  **Job Initiation**: A scheduling system (e.g., Cloud Composer/Airflow) invokes the main BigQuery Stored Procedure (e.g., `dataset.r_exis_v2`), passing job-specific parameters.
2.  **Configuration & Initialization**:
    *   The main procedure reads configuration from `dataset.config_kv` and potentially `dataset.job_history`.
    *   It resolves runtime parameters, timestamps (`TOTAL_FROM`, `TOTAL_TO`, `INITIAL_FROM`), and determines the `INITIAL` mode based on various conditions.
    *   It sets up logging in `dataset.export_audit`.
3.  **Execution Planning**:
    *   Based on `TOTAL_FROM`, `TOTAL_TO`, `FILE_PARTITION`, and `SQL_PARTITION` parameters, the procedure calculates the necessary file and SQL partitions. This logic will be implemented using BigQuery SQL queries and potentially helper UDFs.
    *   The execution plan (list of file partitions and their SQL sub-partitions) is either generated dynamically or stored in a temporary table.
4.  **Data Export Core (`exportcore` equivalent)**:
    *   Executes `PRE_EXPORT` SQL (if defined in config) against the source Oracle database (via a federated query or data transfer service) or directly in BigQuery if data is already migrated.
    *   Iterates through the planned file partitions:
        *   **File Partition Execution (`filepartition` equivalent)**:
            *   Sets up partition-specific variables.
            *   Executes `PRE_FILE` SQL (if defined).
            *   If incremental mode, it will perform merge/upsert operations in BigQuery based on timestamps, replacing the shell-based incremental logic and file preprocessing.
            *   **SQL Partition Execution (`sqlpartition` equivalent)**:
                *   Executes `PRE_SQL`, `OUTPUT_SQL`, and `POST_SQL` for each SQL sub-partition. These SQL statements, originally for Oracle, will be converted to BigQuery SQL. Data extraction will likely happen via BigQuery's data transfer service or federated queries to Oracle.
                *   The output of `OUTPUT_SQL` will be written to a temporary BigQuery table or directly to Cloud Storage.
            *   Executes `POSTFILEPROCESSING` (if defined).
            *   Generates metadata for the output file.
            *   If ready-file generation is enabled, creates entries in `dataset.export_readyfiles`.
    *   Executes `POST_EXPORT` SQL (if defined).
5.  **File Distribution**:
    *   A Cloud Function or Cloud Workflow is triggered for each generated output file in Cloud Storage.
    *   This service reads the distribution instructions from the configuration (e.g., from `dataset.export_distribution`) and performs actions like SCP, SFTP, compression, moving, copying, deleting, or emailing the file.
6.  **Error Handling**: BigQuery Stored Procedures will use `EXCEPTION` blocks, and external orchestrators will handle retries and alerts for failures in Cloud Functions/Workflows.

## 5. Transformation Logic
The transformation logic is highly dynamic, driven by configuration files and the shell script's functions.

*   **Parameter and Attribute Filling (`fillattribs`, `parsformatstring`, `parser_filattrib`, `parser_filfile`)**: This logic will be implemented within BigQuery Stored Procedures. Configuration values will be stored in a `dataset.config_kv` table. Placeholder substitution will be handled via string functions (`REPLACE`, `REGEXP_REPLACE`) or by dynamically constructing SQL queries/statements within the procedures.
*   **Timestamp Handling (`handletimestamps`, `getsubintervalls`)**: BigQuery's rich set of date and timestamp functions (`CURRENT_TIMESTAMP()`, `FORMAT_TIMESTAMP`, `PARSE_TIMESTAMP`, `DATE_ADD`, `DATE_SUB`, etc.) will replace the shell-based date calculations. Interval splitting can be achieved using `GENERATE_DATE_ARRAY` or similar constructs.
*   **SQL Execution (`sqlplusspool`, `executeSQLfromnode`)**: All Oracle SQL will be translated into BigQuery SQL. Data extraction from Oracle will occur through:
    *   **BigQuery Data Transfer Service**: For scheduled, batch transfers of Oracle tables.
    *   **Federated Queries**: For ad-hoc querying of Oracle data if real-time access is required, using BigQuery's support for external data sources.
    *   **ELT Approach**: Load raw Oracle data into BigQuery, then perform transformations within BigQuery SQL.
*   **Partitioning (`getsqlsplits`, `sqlpartition`, `filepartition`)**: The dynamic SQL partitioning based on `dba_tab_partitions` and other logic will be replicated in BigQuery SQL queries that inspect BigQuery table metadata (e.g., `INFORMATION_SCHEMA`) or use explicit partition clauses. File partitioning logic will define how data is grouped before being written to distinct BigQuery tables or Cloud Storage objects.
*   **Parallel Execution (`merger`, `PARALLELFILE`, `PARALLELSQL`)**:
    *   For parallel file creation, BigQuery can leverage its inherent parallel processing for `INSERT ... SELECT` statements or by spawning multiple BigQuery jobs/procedures orchestrated by Cloud Workflows.
    *   Parallel SQL execution could mean either concurrent BigQuery queries or sub-procedures.
*   **Incremental Logic**: The script's incremental mode, which involved reading existing files and deleting/appending records, will be replaced by BigQuery's `MERGE` statements on partitioned tables or by managing incremental loads into new partitions.

## 6. External Dependencies
The original script has several external dependencies:

*   **Oracle Database (via `sqlplus` and `DBCONNECT`)**:
    *   **Impact**: This is the primary data source. All Oracle SQL queries and PL/SQL blocks need to be converted to BigQuery SQL.
    *   **Migration**: Data will be ingested into BigQuery using the BigQuery Data Transfer Service (for scheduled batch) or by setting up federated queries to the Oracle source (for near real-time needs). Oracle metadata tables (`dwh$ta_k_meldungen`, `all_tables`, `all_synonyms`, `dba_tab_partitions`) will be migrated to dedicated BigQuery tables or replaced by BigQuery's `INFORMATION_SCHEMA`.
*   **Local Filesystem (for config files, output files, temporary files)**:
    *   **Impact**: The script heavily relies on local file paths for configuration, intermediate processing, and final output.
    *   **Migration**: Local configuration files (`p_ConfigFile`, `p_DefaultFile`) will be migrated to BigQuery configuration tables (e.g., `dataset.config_kv`). Output data files and temporary files will be written to and managed in Cloud Storage buckets.
*   **Shell Utilities (`nawk`, `perl`, `sed`, `grep`, `cut`, `basename`, `wc`, `expr`, `mkfifo`, `rm`, `mv`, `tail`, `tee`, etc.)**:
    *   **Impact**: These are used for string manipulation, data processing, flow control, and file operations. Many are low-level OS commands.
    *   **Migration**: Most string and data processing logic will be rewritten using BigQuery SQL functions. File operations will be replaced by Cloud Storage API calls, orchestrated by Cloud Functions/Workflows. `mkfifo` and `tee`-based pipe logic will be replaced by BigQuery's internal data streaming/staging mechanisms or Cloud Pub/Sub if inter-process communication is required.
*   **Sourced KornShell Scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parser.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`)**:
    *   **Impact**: These provide utility functions for environment setup, error reporting, parsing, parameter handling, and date calculations.
    *   **Migration**: Their functionalities will be re-implemented as BigQuery UDFs (if simple and stateless), helper BigQuery Stored Procedures, or Python modules within Cloud Functions/Workflows.
*   **External Parser/Date Routines (`parser_getnode`, `parser_filattrib`, `parser_filfile`, `pruefeParameterGesetzt`, `DWMSG_*`, `Zeitraumschleife`, `SubtrahiereZeitbereich`)**:
    *   **Impact**: These are custom functions/scripts for parsing configuration, attribute substitution, and date range calculations.
    *   **Migration**: These will be converted into BigQuery Stored Procedures or UDFs, or Python functions within Cloud Functions, depending on their complexity and state requirements.
*   **SCP / SFTP / Mail (`scp`, `sftp`, `mailx`, `uuencode`)**:
    *   **Impact**: Used for distributing the final output files to external systems or users.
    *   **Migration**: These will be replaced by Cloud Functions or Cloud Workflows that interact with target systems via their respective APIs (e.g., SFTP gateway, object storage APIs, email APIs).
*   **UC4 Job Tracking (`DWH$TA_K_MELDUNGEN`)**:
    *   **Impact**: The script updates a DWH job tracking table, likely in Oracle.
    *   **Migration**: This table will be migrated to BigQuery (e.g., `dataset.job_history`), and updates will be performed by BigQuery DML statements within the stored procedures.

## 7. Unresolved / Risks
*   **Complexity of Shell Script Logic**: The script is marked as "complex" and uses many nested shell commands, pipes, and `eval` statements. Translating all intricacies, especially `nawk` and `sed` scripts, directly to BigQuery SQL or Python requires careful analysis and testing. This is the primary reason for the "manual" automation bucket and the "loc-escalated" migration flag.
*   **Dynamic SQL Generation**: The script dynamically constructs SQL queries based on configuration. This will need to be re-engineered in BigQuery to use parameterized queries within stored procedures or careful string manipulation for dynamic query construction, respecting BigQuery's SQL dialect.
*   **Parallel Execution Orchestration**: The shell-based `merger` function and parallel file/SQL processing logic need to be re-thought for a cloud-native parallel execution model. BigQuery's inherent parallelism helps, but complex inter-process communication patterns (like `mkfifo` and `controlledpipeelem`) will need entirely different solutions using Cloud Pub/Sub, Cloud Storage object notifications, or Cloud Workflows.
*   **External System Integration**: While `SCP`, `SFTP`, and `Mail` have cloud-native equivalents (Cloud Functions/Workflows), the exact parameters and authentication mechanisms for these external systems need to be thoroughly analyzed and secured in the new environment. The details of `EXT:DATABASE` are vague and might require further investigation to determine the exact Oracle database being connected to.
*   **Configuration Management**: The current `parser_getnode`, `parser_filattrib`, `parser_filfile` logic for reading configuration from files needs to be fully translated to interacting with a BigQuery configuration table. The structure and retrieval methods must be robust.
*   **Error Handling and Traps**: The shell `trap` mechanism and custom error messaging (`DWMSG_*`) need to be adapted to BigQuery's error handling (`EXCEPTION` blocks) and cloud-native logging/monitoring (Cloud Logging, Cloud Monitoring).
*   **`DW_DIR_ROOT` and `HOME` variables**: These are environment-specific paths. Their usage will need to be replaced by Cloud Storage paths or environment variables/parameters managed by the orchestration layer.

## 8. Build Plan
The migration will primarily involve translating the KornShell script into BigQuery Stored Procedures and leveraging Google Cloud services for orchestration and external interactions.

**Phase 1: Foundation & Data Ingestion**
1.  **Create BigQuery Dataset**: `dwh_exporter` (or similar) to house all migrated assets.
2.  **Migrate Configuration**:
    *   Design and create `dataset.config_kv` table to store all parameters from `p_ConfigFile` and `p_DefaultFile`.
    *   Develop a Python script (Cloud Function) to parse existing config files and load data into `dataset.config_kv`.
3.  **Migrate Job History**: Create `dataset.job_history` and `dataset.export_audit` tables in BigQuery to store job tracking information (`DWH$TA_K_MELDUNGEN` equivalent).
4.  **Data Ingestion from Oracle**:
    *   Configure BigQuery Data Transfer Service jobs to regularly ingest source Oracle tables (identified from SQL queries in config) into BigQuery.
    *   Alternatively, establish BigQuery Federated Queries for direct Oracle access if required.

**Phase 2: Core Logic Translation - BigQuery Stored Procedures**
5.  **Translate Utility Functions**:
    *   Create BigQuery UDFs or helper Stored Procedures for date/time manipulation (`handletimestamps`, `getsubintervalls`), string parsing (`parsformatstring`, `fillattribs` logic), and sequence generation.
    *   Example: `CREATE OR REPLACE FUNCTION dataset.resolve_timestamp(...) RETURNS STRING AS (...)`.
6.  **Translate SQL Execution Logic**:
    *   For each `OUTPUT_SQL`, `PRE_SQL`, `POST_SQL`, `PRE_FILE`, `POST_FILE`, `PRE_EXPORT`, `POST_EXPORT` node in the configuration, create corresponding BigQuery SQL statements. These might be part of larger procedures or dynamically constructed.
    *   Implement the `sqlplusspool` equivalent by executing these BigQuery SQL statements.
7.  **Implement Partitioning Logic**:
    *   Develop BigQuery Stored Procedures for `getsqlsplits` equivalent, leveraging `INFORMATION_SCHEMA` or dedicated control tables to determine partitioning strategies.
    *   The `sqlpartition` and `filepartition` logic will be implemented as nested BigQuery Stored Procedures, handling the iteration over partitions and execution of corresponding SQL.
8.  **Main Orchestration Procedure**:
    *   Create the main BigQuery Stored Procedure, `dataset.r_exis_v2`, to encapsulate the `exportcore` logic.
    *   This procedure will handle parameter validation, configuration loading, execution planning, and calling sub-procedures for file and SQL partitioning.

**Phase 3: External Interactions & Orchestration**
9.  **Cloud Storage Integration**:
    *   Modify BigQuery SQL to write results directly to Cloud Storage as CSV/JSON files.
    *   Set up Cloud Storage buckets for raw, processed, and distributed data.
10. **Distribution Services (Cloud Functions / Cloud Workflows)**:
    *   Develop Cloud Functions/Workflows for each distribution method:
        *   **SCP/SFTP**: A Cloud Function triggered by new files in a specific Cloud Storage bucket, to transfer files to external SFTP/SCP servers. (Requires secure credential management).
        *   **Mail Attachment**: A Cloud Function to send emails with attachments from Cloud Storage.
        *   **Compression/Move/Copy/Delete**: Cloud Functions to perform these operations on objects in Cloud Storage.
    *   These will be invoked from the main BigQuery Stored Procedure (via `CALL ...`) or by Cloud Storage event triggers.
11. **Logging and Monitoring**:
    *   Integrate BigQuery Stored Procedure execution logs with Cloud Logging.
    *   Set up Cloud Monitoring alerts for job failures or performance issues.

**Phase 4: Testing & Deployment**
12. **Unit Testing**: Test individual BigQuery Stored Procedures and Cloud Functions.
13. **Integration Testing**: Test the end-to-end data flow, including data ingestion, processing, and distribution.
14. **Performance Testing**: Benchmark the new BigQuery solution against the legacy script.
15. **Deployment**: Deploy BigQuery assets, Cloud Functions/Workflows, and schedule the job using Cloud Composer/Airflow.

**Language Choices for Build Plan Items:**

*   **BigQuery DDL/DML/Procedures**: For all data-related tasks and core orchestration.
*   **Python**: For Cloud Functions/Workflows handling external integrations (SFTP, SCP, Mail) and potentially complex config parsing or specific UDFs.
*   **GCP CLI/Terraform**: For infrastructure setup (datasets, buckets, service accounts).