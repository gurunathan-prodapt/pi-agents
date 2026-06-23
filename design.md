# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh

## 1. Purpose & Scope

This job, `r_ausd_bp_ta_msisdn.ksh`, serves as an orchestration script for the initial provision of selected basic products ("Basisprodukte") for the BERT system, which is likely involved in credit scoring or similar analytical processes. Its primary function is to prepare and make available a snapshot of contract cache data from a Data Warehouse (DWH) for a Forderungsscoring (scoring for receivables) system.

The script handles:
*   **Parameter Parsing**: Reads command-line arguments for cutoff date (`-s`) and restart value (`-l`).
*   **Date Determination**: Establishes a system date and defaults the cutoff date if not provided.
*   **Error Handling and Logging**: Implements a robust error concept with logging to a file and setting up traps for runtime errors.
*   **Orchestration**: Invokes a core "kernel script" (`k_ausd_bp_ta_msisdn.ksh`) to perform the actual data preparation and loading.

The scope of this migration focuses on translating this KornShell orchestration logic to Google BigQuery stored procedures and associated orchestration mechanisms, while identifying the need for separate analysis and migration of the delegated core business logic.

## 2. Source Inventory

The job is composed of a single KornShell script.

*   **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh`
*   **Technology**: KornShell (ksh)
*   **Category**: Shell Script
*   **Tier**: Not explicitly found in `file_complexity` table. Inferred as **Medium** due to its role as an orchestrator, parameter handling, and error management, despite delegating core logic.
*   **Automation Bucket**: `semi_auto`

## 3. Target Architecture

The migration targets Google Cloud Platform, leveraging BigQuery for data processing and storage, and Cloud Composer (Apache Airflow), Google Workflows, or Scheduled Queries for orchestration.

*   **Data Processing**: BigQuery Stored Procedures will implement the orchestration logic, parameter handling, and logging. A separate BigQuery Stored Procedure will serve as a placeholder for the core business logic currently in `k_ausd_bp_ta_msisdn.ksh`.
*   **Audit and Logging**: Dedicated BigQuery tables (e.g., `job_audit_log`, `job_run_info`) will replace filesystem-based logging and status tracking.
*   **Orchestration**: Cloud Composer (Airflow DAGs), Google Workflows, or BigQuery Scheduled Queries will manage the scheduling and execution of the BigQuery stored procedures. Cloud Logging can integrate with the BQ stored procedure logs.
*   **Data Storage**: Existing DWH data sources will be migrated to BigQuery tables. The target `FOS-Tabelle` will also reside in BigQuery.

## 4. Data Flow & Lineage

The original lineage query for `lineage_edges` did not return explicit invocation edges for this script. However, through code analysis, the data flow is understood as follows:

1.  **Inputs**:
    *   Command-line parameters: `p_stichtag` (cutoff date DDMMYYYY), `p_wiederanlaufWert` (restart value).
    *   Environment variables/sourced scripts: `$HOME/.dw_init`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`.
    *   Implicit Data Source: `DWH` (Data Warehouse), specifically referencing a table like `DWH\$TA_C_VERTRAG` as indicated in commented-out code.
2.  **Orchestration Logic (`r_ausd_bp_ta_msisdn.ksh` - this script)**:
    *   Parses input parameters.
    *   Defaults restart value and cutoff date if not provided.
    *   Performs parameter validation.
    *   Initializes job metadata and logging entries.
    *   Invokes the core kernel script (`k_ausd_bp_ta_msisdn.ksh`).
    *   Updates job status (OK/ERROR) and logs completion message.
3.  **Core Business Logic (`k_ausd_bp_ta_msisdn.ksh`)**:
    *   (Delegated) This script is expected to perform the actual extraction of contract cache data from DWH based on the `p_stichtag` and `p_wiederanlaufWert`.
    *   It will transform and load this data into the `FOS-Tabelle` (Forderungsscoring table).
4.  **Outputs**:
    *   **Audit/Log Data**: `job_audit_log`, `job_run_info` tables in BigQuery.
    *   **Target Data**: `FOS-Tabelle` (e.g., `project.dataset.target_table`) in BigQuery.
    *   **Downstream Consumption**: The data in the `FOS-Tabelle` is made available for the "Forderungsscoring" system.

**Execution Order:**
1.  Environment setup and utility script sourcing.
2.  Parameter parsing and validation.
3.  Job initialization and logging.
4.  Call to `k_ausd_bp_ta_msisdn.ksh` (represented by a BQ Stored Procedure).
5.  Post-execution logging and status update.

## 5. Transformation Logic

The migration will transform the KornShell orchestration logic into a BigQuery Stored Procedure, and abstract the core business logic into a separate BigQuery Stored Procedure.

**Original Script Logic vs. BigQuery Implementation:**

*   **Parameter Handling**: The shell's `getopts` and variable assignments will be translated to `IN` parameters of a BigQuery Stored Procedure (e.g., `p_stichtag_string STRING`, `p_wiederanlaufWert INT64`).
*   **Date Determination**: Shell date commands (`DWDate_Gib_Zeitraum`) and conditional logic will be replaced with BigQuery SQL functions like `CURRENT_DATE()`, `PARSE_DATE('%d%m%Y', ...)`, and `IF/CASE` statements.
*   **Restart Logic**: The conditional logic for `p_wiederanlaufWert` will be translated directly. The `DWH_VERTRAG_ID > Wiederanlaufwert` filter and potential `DELETE` operations based on this value will be part of the delegated kernel stored procedure.
*   **Error Handling**: Shell `trap` mechanisms and `DWMSG_MeldeFehler` calls will be replaced with BigQuery's `EXCEPTION WHEN ERROR THEN` blocks, `SIGNAL SQLSTATE`, and `INSERT` statements into an audit log table.
*   **Logging**: `print` statements and redirection to log files (`LogDatei`) will be replaced by `INSERT` statements into `job_audit_log` and `job_run_info` tables.
*   **Core Logic Invocation**: The shell command `${Name_Kernskript} -j ...` will be replaced by a BigQuery `CALL` statement to the `k_ausd_bp_ta_msisdn` BigQuery Stored Procedure.

**BigQuery SQL Pseudocode (from CM MCP tool):**

```sql
-- BigQuery Stored Procedure: ausd_bp_ta_msisdn_wrapper (Orchestration)
CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_msisdn_wrapper`(
  IN p_stichtag_string STRING,
  IN p_wiederanlaufWert INT64
)
BEGIN
  -- Variable declarations and initializations
  DECLARE v_sysdate DATE;
  DECLARE v_stichtag DATE;
  DECLARE v_wiederanlaufWert INT64 DEFAULT 0;
  DECLARE v_job_kennung STRING DEFAULT 'ausd_bp_ta_msisdn';
  DECLARE v_job_nr INT64;
  DECLARE v_log_dateiname STRING;
  DECLARE v_err_nr INT64 DEFAULT 0;
  DECLARE v_err_arg STRING DEFAULT '';
  DECLARE v_status STRING DEFAULT 'INIT';

  -- Date determination and defaulting
  SET v_sysdate = CURRENT_DATE();
  IF p_wiederanlaufWert IS NULL THEN SET v_wiederanlaufWert = 0; ELSE SET v_wiederanlaufWert = p_wiederanlaufWert; END IF;
  IF p_stichtag_string IS NULL OR TRIM(p_stichtag_string) = '' THEN SET v_stichtag = v_sysdate; ELSE SET v_stichtag = PARSE_DATE('%d%m%Y', p_stichtag_string); END IF;

  -- Parameter validation
  IF v_stichtag IS NULL THEN SET v_err_nr = 193; SET v_err_arg = 'Stichtag'; END IF;
  IF v_err_nr <> 0 THEN
    INSERT INTO `project.dataset.job_audit_log` (job_kennung, job_nr, log_ts, status, err_nr, err_arg, message)
    VALUES (v_job_kennung, NULL, CURRENT_TIMESTAMP(), 'ERROR', v_err_nr, v_err_arg, 'Required parameter missing or invalid');
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Parameter validation failed';
  END IF;

  -- Job initialization and logging (DWMSG_ErmittleNr, DWMSG_Logdateiname, DWMSG_ErzeugeEintrag, DWMSG_SetzeStichtagInfo equivalents)
  SET v_job_nr = (SELECT IFNULL(MAX(job_nr), 0) + 1 FROM `project.dataset.job_audit_log` WHERE job_kennung = v_job_kennung);
  SET v_log_dateiname = CONCAT(v_job_kennung, '_', CAST(v_job_nr AS STRING), '.log');
  INSERT INTO `project.dataset.job_audit_log` (job_kennung, job_nr, log_ts, status, stichtag, sysdate, log_file, message)
  VALUES (v_job_kennung, v_job_nr, CURRENT_TIMESTAMP(), 'STARTED', v_stichtag, v_sysdate, v_log_dateiname, 'Job started');
  INSERT INTO `project.dataset.job_run_info` (job_kennung, job_nr, stichtag, sysdate, created_ts)
  VALUES (v_job_kennung, v_job_nr, v_stichtag, v_sysdate, CURRENT_TIMESTAMP());

  -- Core business logic placeholder: Call the downstream procedure
  CALL `project.dataset.k_ausd_bp_ta_msisdn`(v_job_kennung, v_stichtag, v_job_nr, v_wiederanlaufWert);

  -- Success handling
  INSERT INTO `project.dataset.job_audit_log` (job_kennung, job_nr, log_ts, status, message)
  VALUES (v_job_kennung, v_job_nr, CURRENT_TIMESTAMP(), 'OK', 'Die Abarbeitung wurde ohne erkennbare Fehler beendet');
  SET v_status = 'OK';

EXCEPTION WHEN ERROR THEN
  INSERT INTO `project.dataset.job_audit_log` (job_kennung, job_nr, log_ts, status, message)
  VALUES (v_job_kennung, v_job_nr, CURRENT_TIMESTAMP(), 'ERROR', 'AppError: Abbruch');
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Job failed';
END;
```

```sql
-- Optional downstream procedure placeholder: k_ausd_bp_ta_msisdn (Core Logic)
CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_bp_ta_msisdn`(
  IN p_job_kennung STRING,
  IN p_stichtag DATE,
  IN p_job_nr INT64,
  IN p_wiederanlaufWert INT64
)
BEGIN
  -- Placeholder for the actual business logic from the original k_ausd_bp_ta_msisdn.ksh.
  -- This will likely involve:
  -- 1) DELETE FROM `project.dataset.target_table` WHERE DWH_VERTRAG_ID >= p_wiederanlaufWert; (if p_wiederanlaufWert > 0)
  -- 2) INSERT INTO `project.dataset.target_table` (...)
  --    SELECT ...
  --    FROM `project.dataset.source_table`
  --    WHERE Gueltig_von <= p_stichtag AND p_stichtag < Gueltig_bis AND LADEDATUM < p_stichtag AND DWH_VERTRAG_ID > p_wiederanlaufWert;
  -- 3) Log status/row counts to job_audit_log.
  INSERT INTO `project.dataset.job_audit_log` (job_kennung, job_nr, log_ts, status, message)
  VALUES (p_job_kennung, p_job_nr, CURRENT_TIMESTAMP(), 'INFO', 'Kernel procedure executed');
END;
```

## 6. External Dependencies

The `lineage_assembled_jobs` record indicated no explicit external systems (`external_systems: []`). However, analysis of the source code reveals the following potential dependencies:

*   **DWH (Data Warehouse)**: The script comments reference `DWH\$TA_C_VERTRAG` and a `FOSHoleLadedatum` function, implying a dependency on an upstream Data Warehouse system for contract data. The specific technology of this DWH (e.g., Oracle, Teradata) is not specified and requires further investigation. This DWH will be migrated to BigQuery.
*   **Forderungsscoring System**: The output data (FOS-Tabelle) is explicitly provided for a "Forderungsscoring" system, indicating a downstream consumer of the prepared data. This downstream system's integration method will need to be re-evaluated for BigQuery.
*   **Environment Variables/Configuration**: Sourcing of `$HOME/.dw_init` and usage of `${BERT_DIR_ROOT}` suggests reliance on a specific environment setup. These configurations will need to be translated into BigQuery runtime parameters or part of the orchestration configuration (e.g., Airflow variables).
*   **Utility Scripts**: The script sources several utility scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`). These functions need to be replicated in BigQuery SQL or Python.

**Replacement Strategy:**
*   **DWH**: Source DWH tables will be ingested into BigQuery.
*   **Forderungsscoring**: Integration with the downstream system will be re-established, potentially through BigQuery views, exports to Cloud Storage, or direct API access if applicable.
*   **Environment/Utilities**: Common functions will be reimplemented as BigQuery UDFs or part of the stored procedures. Environment-specific settings will be managed via orchestration (e.g., Airflow variables, GCP Secrets Manager).

## 7. Unresolved / Risks

*   **Core Business Logic**: The most significant unresolved item is the exact SQL logic within `k_ausd_bp_ta_msisdn.ksh`. This script needs to be separately analyzed and migrated to a BigQuery Stored Procedure. Without its content, the full data transformation details remain unknown.
*   **`DWH$TA_C_VERTRAG` Details**: The schema, data types, and access patterns for `DWH$TA_C_VERTRAG` are unknown. This is crucial for defining the source table in BigQuery.
*   **Missing Lineage Edges**: The lack of explicit `INVOKES` or `READS`/`WRITES` edges for this script in the `lineage_edges` table highlights a gap in automated dependency discovery for this specific type of shell orchestration, requiring manual code analysis.
*   **Shell-specific Constructs**: Shell `trap` commands for error handling and `print`/`tee` for file-based logging have no direct BigQuery equivalents and require re-architecting into BigQuery's `EXCEPTION` handling and audit tables.
*   **Helper Script Functionality**: The exact functionality of `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` needs to be fully understood to accurately translate their behaviors into BigQuery.
*   **`file_complexity` Information Gap**: The `file_complexity` table returned no rows, meaning the tier and migration flags were not automatically assessed. The manual assessment of "Medium" tier might need refinement once the kernel script is analyzed.

## 8. Build Plan

The migration will proceed in an iterative fashion, prioritizing the core data transformation and then building the orchestration around it.

1.  **Analyze Kernel Script**:
    *   **Action**: Analyze `k_ausd_bp_ta_msisdn.ksh` to extract its full data transformation and loading logic.
    *   **Output**: Detailed design for `k_ausd_bp_ta_msisdn` as a BigQuery Stored Procedure, including source and target table definitions.
    *   **Language**: Manual analysis, potentially `shellscript_to_bqsql_design` for the kernel script.

2.  **Define BigQuery Audit Tables**:
    *   **Action**: Create DDL for `project.dataset.job_audit_log` and `project.dataset.job_run_info` tables in BigQuery.
    *   **Output**: `job_audit_log_ddl.sql`, `job_run_info_ddl.sql`.
    *   **Language**: BigQuery SQL.

3.  **Develop Core Logic BigQuery Stored Procedure**:
    *   **Action**: Implement the `project.dataset.k_ausd_bp_ta_msisdn` BigQuery Stored Procedure based on the analysis from Step 1.
    *   **Output**: `k_ausd_bp_ta_msisdn_sp.sql`.
    *   **Language**: BigQuery SQL.

4.  **Develop Orchestration BigQuery Stored Procedure**:
    *   **Action**: Implement the `project.dataset.ausd_bp_ta_msisdn_wrapper` BigQuery Stored Procedure as designed in Section 5.
    *   **Output**: `ausd_bp_ta_msisdn_wrapper_sp.sql`.
    *   **Language**: BigQuery SQL.

5.  **Develop Orchestration Mechanism**:
    *   **Action**: Create a Cloud Composer DAG, Google Workflow, or BigQuery Scheduled Query to trigger `project.dataset.ausd_bp_ta_msisdn_wrapper`.
    *   **Output**: `ausd_bp_ta_msisdn_dag.py` (for Cloud Composer), or `ausd_bp_ta_msisdn_workflow.yaml`, or Scheduled Query configuration.
    *   **Language**: Python (for Airflow), YAML (for Workflows), or JSON (for Scheduled Query API).

6.  **Data Ingestion for DWH Sources**:
    *   **Action**: Set up continuous or batch ingestion for `DWH$TA_C_VERTRAG` and any other relevant source tables into BigQuery.
    *   **Output**: Data pipelines (e.g., Dataflow, Cloud Data Fusion, Storage Transfer Service).
    *   **Language**: Varies by tool.

7.  **Testing and Validation**:
    *   **Action**: Thoroughly test the BigQuery stored procedures and orchestration, validating data output against the legacy system.
    *   **Output**: Test reports.

8.  **Deprovisioning**:
    *   **Action**: Decommission the legacy KornShell script and associated environment.