# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh

## 1. Purpose & Scope
The purpose of this job is to prepare a snapshot extraction ("Stichtags-Abzug") of a contract cache/base table in DWH and make it available for the BERT report, specifically for "Forderungsscoring" (claims scoring). It acts as a wrapper script, handling parameter parsing, environment setup, and error logging, before invoking a core script that performs the actual data extraction and staging. The job is scoped to generate a time-based snapshot of contract data, applying a restart mechanism if specified.

## 2. Source Inventory
The job is primarily defined by a single KornShell script.
*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh`
*   **Technology:** KornShell (ksh)
*   **Complexity Tier:** medium
*   **Automation Bucket:** semi_auto
*   **Role:** Orchestrator / Wrapper Script

This script depends on and sources several other shell utility scripts for common functions, and crucially, invokes a core processing script:
*   `$HOME/.dw_init` (for environment initialization)
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (for error messaging framework)
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (for parameter parsing utilities)
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (for date handling utilities)
*   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_austausch.ksh` (the core business logic script, invoked by this wrapper)

## 3. Target Architecture
The migration target platform is Google Cloud's BigQuery.
*   **Orchestration & Control Flow:** The wrapper script's logic (parameter handling, conditional execution, basic error management) will be primarily migrated to a BigQuery Stored Procedure. For more complex external interactions, parameter validation, or advanced scheduling, a Python-based orchestration layer (e.g., a Cloud Composer (Airflow) DAG, Cloud Functions, or Cloud Run service) could wrap the BigQuery Stored Procedure call.
*   **Data Transformation:** The implied data extraction and staging logic will be implemented as BigQuery SQL statements within the aforementioned BigQuery Stored Procedure.
*   **Logging & Monitoring:** The custom shell-based logging framework will be replaced with BigQuery audit tables and integrated with Cloud Logging for centralized log management and alerting.
*   **Data Storage:** Source and target tables will reside in BigQuery.

## 4. Data Flow & Lineage
The original script outlines the following data flow and execution sequence:

1.  **Input Parameters:** The script accepts `-s DDMMYYYY` (Stichtag/snapshot date) and `-l <value>` (Wiederanlaufwert/restart value) as command-line arguments. If `-s` is not provided, the `Stichtag` defaults to the current system date. If `-l` is not provided, it defaults to `0`.
2.  **Environment Initialization:** Sources `$HOME/.dw_init` and several `BERT_DIR_ROOT` utility scripts to set up the environment and functions.
3.  **Parameter Processing & Validation:** Parses inputs, defaults missing values, and validates the `Stichtag`.
4.  **Logging Setup:** Initializes a custom logging framework (`DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`) and sets up shell `trap` commands for error handling.
5.  **Core Logic Invocation:** Executes `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_austausch.ksh`, passing the resolved parameters (`JobKennung`, `Stichtag`, `DW_EintragsNr`, `Wiederanlaufwert`) to it.
6.  **Data Extraction (Implied from Comments):** The core script (`k_ausd_austausch.ksh`) is expected to:
    *   Delete records in the target FOS table where `DWH_VERTRAG_ID >= Wiederanlaufwert` (restart logic).
    *   Insert new records into the FOS table by selecting from a `contract_cache_source` table.
    *   Filtering conditions for extraction are: `Gueltig_von <= Stichtag`, `Stichtag < Gueltig_bis`, and `LADEDATUM < Stichtag`.
    *   Additional filtering `DWH_VERTRAG_ID > Wiederanlaufwert` is applied during insertion.
7.  **Status Reporting:** Logs the job status (`DWMSG_SetzeStatusOK`) to the custom logging system upon successful completion.

## 5. Transformation Logic
The transformation logic involves re-implementing the shell script's control flow and data filtering in a BigQuery-native context.

**Legacy Component: `r_ausd_austausch.ksh` (Wrapper/Orchestrator)**
*   **Parameter Parsing (`getopts`):** Replaced by BigQuery Stored Procedure input parameters (`p_stichtag_string STRING`, `p_wiederanlaufWert INT64`) or Python function arguments.
*   **Defaulting Parameters:** `IFNULL(p_wiederanlaufWert, 0)` for restart value. If `p_stichtag_string` is NULL or empty, use `CURRENT_DATE()` for `v_stichtag`.
*   **Date Determination:** Shell's `DWDate_Gib_Zeitraum` and `sysdate` logic will be `CURRENT_DATE()` in BigQuery. Date string `DDMMYYYY` will be parsed using `PARSE_DATE('%d%m%Y', p_stichtag_string)`.
*   **Parameter Validation:** `IF v_stichtag IS NULL THEN RAISE USING MESSAGE = 'Stichtag is missing or invalid'; END IF;`
*   **Error Handling (`set -e`, `trap`):** Replaced by BigQuery Stored Procedure's `EXCEPTION WHEN ERROR` block and structured error logging to an audit table.
*   **Logging Framework (`DWMSG_*`):** Replaced by `INSERT` statements into a BigQuery `job_audit_log` table (recording job start, end, status, errors) and leveraging Cloud Logging for runtime messages.
*   **Core Script Invocation:** This will translate to calling another BigQuery Stored Procedure (the migrated `k_ausd_austausch.ksh`) using `CALL` statement within the orchestrating Stored Procedure, passing the relevant parameters.

**Legacy Component: `k_ausd_austausch.ksh` (Core Logic - Implied)**
Based on the comments in the wrapper script, the core logic for data manipulation is assumed to perform:
*   **Restart Logic:** A `DELETE` statement on the target FOS table based on `DWH_VERTRAG_ID >= Wiederanlaufwert`.
*   **Data Extraction & Insertion:** An `INSERT INTO ... SELECT FROM` statement with `WHERE` clauses applying the `Stichtag`, `Gueltig_von`, `Gueltig_bis`, `LADEDATUM`, and `DWH_VERTRAG_ID` filters.

The `k_ausd_austausch.ksh` script's specific SQL queries will need to be analyzed and translated into BigQuery SQL, potentially forming another BigQuery Stored Procedure or a set of SQL statements.

## 6. External Dependencies
The `lineage_assembled_jobs` analysis indicated no direct external system dependencies (e.g., Oracle, SFTP, S3). All referenced components appear to be internal to the legacy DW environment.

*   **Legacy Dependency: `$HOME/.dw_init`:** This environment initialization file sets up paths and variables. In BigQuery, this configuration will be managed implicitly by the project and dataset context, or explicitly via environment variables in an external orchestration tool (e.g., Cloud Composer).
*   **Legacy Dependencies: Utility Scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`):** The functionality of these scripts will be absorbed into the BigQuery Stored Procedure logic or Python orchestration. Error handling, parameter parsing, and date functions are standard features in both environments.
*   **Legacy Dependency: `k_ausd_austausch.ksh` (Core Script):** This is a critical internal dependency. Its functionality must be migrated to a BigQuery-native component (e.g., a separate BigQuery Stored Procedure) that the migrated `r_ausd_austausch.ksh` orchestration will invoke.

## 7. Unresolved / Risks
*   **Content of `k_ausd_austausch.ksh` is Unknown:** The actual data transformation logic in the core script is not available. This is the biggest unresolved item. Without it, the full scope of BigQuery SQL transformation cannot be finalized. The current design assumes simple SQL transformations based on comments, but complex logic could require different approaches (e.g., Dataflow/Spark).
*   **Specifics of `.dw_init` and `BERT_DIR_ROOT`:** The exact contents and configuration defined by `$HOME/.dw_init` and the full path resolution of `${BERT_DIR_ROOT}` are not known. These need to be identified and mapped to appropriate BigQuery project/dataset names, GCS paths, or environmental configurations in the target.
*   **Custom Logging Framework Details:** While the approach is to replace it with BigQuery audit tables, any specific logging details or custom reporting that needs to be preserved from the `DWMSG_*` functions would require careful mapping.
*   **Performance of Restart Logic:** The `DELETE` then `INSERT` pattern for restart logic can be inefficient for very large tables in BigQuery. Optimized strategies (e.g., `MERGE` statement, or partitioning/clustering strategies) should be considered during implementation, especially if `DWH_VERTRAG_ID` is not the clustering key.
*   **Data Volume and Latency Requirements:** The scale of the "contract cache" and "FOS table" and the required data refresh frequency will influence the choice between a simple BigQuery Stored Procedure and a more robust orchestration solution like Cloud Composer.

## 8. Build Plan
1.  **Analyze `k_ausd_austausch.ksh`:** Obtain and perform a detailed static analysis of the core script (`${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_austausch.ksh`) to understand its complete data transformation and loading logic. This step is critical and must precede the detailed design of the data transformation.
2.  **Schema Definition for BigQuery:**
    *   Create `contract_cache_source` (source table), `fos_table` (target table), and `job_audit_log` (logging table) in BigQuery with appropriate schemas, data types, partitioning, and clustering keys.
    *   Map legacy data types to BigQuery equivalents.
3.  **Develop BigQuery Stored Procedure (`k_ausd_austausch_sp`):** Based on the analysis from step 1, implement the core data extraction, transformation, and load (ETL) logic into a BigQuery Stored Procedure.
4.  **Develop BigQuery Stored Procedure (`r_ausd_austausch_sp`):**
    *   Translate the `r_ausd_austausch.ksh` wrapper logic into a BigQuery Stored Procedure.
    *   Implement parameter parsing, defaulting, and validation.
    *   Integrate the call to the `k_ausd_austausch_sp` stored procedure.
    *   Implement comprehensive audit logging to the `job_audit_log` table.
    *   Handle errors using `EXCEPTION WHEN ERROR` blocks.
5.  **Implement External Orchestration (Optional but Recommended):**
    *   If dynamic external parameters or complex scheduling are required, create a Python script or Cloud Composer DAG.
    *   This script will parse command-line/Airflow parameters and invoke `r_ausd_austausch_sp` with the correct arguments.
6.  **Testing:**
    *   **Unit Tests:** Test individual BigQuery SQL components and Stored Procedures.
    *   **Integration Tests:** Test the `r_ausd_austausch_sp` calling `k_ausd_austausch_sp` end-to-end.
    *   **Data Validation:** Verify data integrity and accuracy in the target `fos_table`.
7.  **Deployment:** Deploy the BigQuery Stored Procedures, tables, and any external orchestration components to the target BigQuery environment.
8.  **Decommissioning:** Retire the legacy KornShell scripts and associated utility files.