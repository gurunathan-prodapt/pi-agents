# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh

## 1. Purpose & Scope
This KornShell script, `r_ausd_bp_ta_bpr_beschr.ksh`, is an orchestration layer for the initial provisioning of selected basic products for the BERT system. Its primary purpose is to generate a snapshot of contract cache data from the Data Warehouse (DWH) and make it available for a downstream Forderungsscoring (FOS) process. The script handles parameter parsing, date determination, error handling, and invokes a core script (`k_ausd_bp_ta_bpr_beschr.ksh`) that performs the actual data processing. It supports a restart mechanism (`p_wiederanlaufWert`) to manage incremental or recovery loads.

## 2. Source Inventory
The job consists of a single KornShell script.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh`
    *   **Technology:** KornShell
    *   **Complexity Tier:** Medium
    *   **Automation Bucket:** Semi-Auto
    *   **Summary:** Orchestrates the initial provision of selected basic products for the BERT system, generating a snapshot of contract cache data from the DWH for Forderungsscoring.
    *   **Purpose:** ETL orchestrator/wrapper script.
    *   **Dependencies (sourced scripts):**
        *   `. $HOME/.dw_init` (environment initialization)
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error handling)
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter parsing helper)
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date handling helper)
    *   **Invokes:** `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_beschr.ksh` (core business logic script)
    *   **Implicitly references (from summary/comments/references_out):**
        *   `DWH$TA_C_VERTRAG` (DWH contract cache table)
        *   `FOS_TABLE` (target table for Forderungsscoring)

## 3. Target Architecture
The KornShell orchestration script will be migrated to a BigQuery Stored Procedure. This stored procedure will encapsulate the parameter handling, date logic, and invocation of the core data processing, as well as the error handling and logging. The actual data transformation logic, which currently resides in `k_ausd_bp_ta_bpr_beschr.ksh`, will also need to be migrated into a separate BigQuery Stored Procedure or SQL script, which the main stored procedure will then call.

*   **Orchestration Component:** BigQuery Stored Procedure (`ausd_bp_ta_bpr_beschr`)
*   **Data Processing Component:** Separate BigQuery Stored Procedure(s) or SQL scripts (migrated from `k_ausd_bp_ta_bpr_beschr.ksh`)
*   **Logging/Auditing:** Dedicated BigQuery logging tables (e.g., `project.dataset.job_log`, `project.dataset.job_audit`).
*   **Data Storage:** BigQuery tables for source (e.g., `DWH.TA_C_VERTRAG`) and target (e.g., `FOS.FOS_TABLE`).

## 4. Data Flow & Lineage
The original script `r_ausd_bp_ta_bpr_beschr.ksh` acts as a wrapper.
1.  **Parameter Input:** The script accepts `p_stichtag` (processing date) and `p_wiederanlaufWert` (restart value).
2.  **Date/Parameter Handling:** It determines the effective processing date, defaulting to the system date if not provided. It also initializes the restart value.
3.  **Error Handling & Logging Setup:** Initializes error handling utilities and sets up logging.
4.  **Core Script Invocation:** It invokes the `k_ausd_bp_ta_bpr_beschr.ksh` script, passing the parsed parameters. This core script is responsible for:
    *   Reading data from `DWH$TA_C_VERTRAG` (and potentially other DWH tables).
    *   Applying filtering logic: `Gueltig_von <= Stichtag < Gueltig_bis AND LADEDATUM < Stichtag`.
    *   Handling restart logic: potentially deleting rows from the target table (`FOS_TABLE`) based on `DWH_VERTRAG_ID` and `p_wiederanlaufWert`, then inserting new/updated data.
    *   Writing the processed data to `FOS_TABLE`.
5.  **Status Reporting:** Logs the job status and exits.

In BigQuery:
*   The `ausd_bp_ta_bpr_beschr` stored procedure will receive `p_stichtag` and `p_wiederanlaufWert`.
*   It will perform date defaulting and parameter validation.
*   It will record job start in an audit table.
*   It will execute a sub-procedure or direct SQL statements (derived from `k_ausd_bp_ta_bpr_beschr.ksh`) to:
    *   Perform an optional `DELETE` from the target table if `p_wiederanlaufWert` is active.
    *   Perform an `INSERT INTO ... SELECT ...` from the DWH source table(s) to the FOS target table, applying the date and restart filters.
*   It will update the job status in the audit table upon completion or error.

## 5. Transformation Logic

**Original Script (`r_ausd_bp_ta_bpr_beschr.ksh`) Logic:**
*   **Parameter Parsing:** Uses `getopts` to parse `-s` (stichtag) and `-l` (wiederanlaufWert).
*   **Defaulting `p_wiederanlaufWert`:** Defaults to 0 if not set.
*   **System Date Retrieval:** Calls `DWDate_Gib_Zeitraum` to get `v_sysdate` in `DDMMYYYY`.
*   **Defaulting `p_stichtag`:** If `p_stichtag` is not provided, it defaults to `v_sysdate`. (Note: commented-out code suggests a more complex `MIN(sysdate, maxladedatum)` logic was considered but not implemented).
*   **Parameter Validation:** Calls `pruefeParameterGesetzt`.
*   **Logging Initialization:** Sets up `JobKennung`, `DW_EintragsNr`, `LogDatei`.
*   **Error Trapping:** `trap` commands are used for `INT`, `STOP`, `CONT`, and `ERR` signals to ensure error handling (`DWMSG_Fehlerbehandlung`).
*   **Core Script Invocation:** `"${Name_Kernskript}" -j $JobKennung -s $p_stichtag -f ${DW_EintragsNr} -l ${p_wiederanlaufWert}`

**Target BigQuery Stored Procedure (`ausd_bp_ta_bpr_beschr`) Logic:**

```sql
CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_bpr_beschr`(
  IN p_stichtag_in STRING,
  IN p_wiederanlaufWert_in INT64
)
BEGIN
  DECLARE v_sysdate STRING;
  DECLARE v_stichtag STRING;
  DECLARE v_restart_value INT64 DEFAULT 0;
  DECLARE v_job_kennung STRING DEFAULT 'AUSD_BP_TA_BPR_BESCHR';
  DECLARE v_job_nr INT64;

  -- 1. Initialize restart value
  SET v_restart_value = IFNULL(p_wiederanlaufWert_in, 0);

  -- 2. Current system date in DDMMYYYY
  SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

  -- 3. Default processing date
  SET v_stichtag = IFNULL(NULLIF(p_stichtag_in, ''), v_sysdate);

  -- 4. Parameter validation
  IF v_stichtag IS NULL OR v_stichtag = '' THEN
    INSERT INTO `project.dataset.job_log`
    (job_kennung, status, message, created_at)
    VALUES
    (v_job_kennung, 'ERROR', 'Stichtag missing', CURRENT_TIMESTAMP());
    RAISE USING MESSAGE = 'Parameter validation failed: Stichtag missing';
  END IF;

  -- 5. Create job entry / audit record (simplified for example)
  -- In a real scenario, v_job_nr would be generated, and a more robust audit table used.
  INSERT INTO `project.dataset.job_audit`
  (job_kennung, stichtag, restart_value, status, created_at)
  VALUES
  (v_job_kennung, v_stichtag, v_restart_value, 'STARTED', CURRENT_TIMESTAMP());

  -- Optional: Get a job_nr for logging further steps
  SET v_job_nr = (SELECT COUNT(*) FROM `project.dataset.job_audit` WHERE job_kennung = v_job_kennung); -- Placeholder, usually sequence or hash

  -- 6. Core data processing (this part would be the migration of k_ausd_bp_ta_bpr_beschr.ksh)
  -- This section assumes k_ausd_bp_ta_bpr_beschr.ksh's logic involves selecting from DWH$TA_C_VERTRAG
  -- and inserting into FOS_TABLE with specific date and ID filters.

  -- Optional restart cleanup: Delete from target table based on restart_value
  IF v_restart_value > 0 THEN
    DELETE FROM `project.dataset.FOS_TABLE`
    WHERE DWH_VERTRAG_ID >= v_restart_value;
  END IF;

  -- Insert/Merge into target table based on criteria
  INSERT INTO `project.dataset.FOS_TABLE` (column1, column2, ..., DWH_VERTRAG_ID)
  SELECT
    src.column1, src.column2, ..., src.DWH_VERTRAG_ID
  FROM `project.dataset.DWH_TA_C_VERTRAG` src -- Assuming DWH$TA_C_VERTRAG is mapped to DWH_TA_C_VERTRAG
  WHERE src.Gueltig_von <= PARSE_DATE('%d%m%Y', v_stichtag)
    AND PARSE_DATE('%d%m%Y', v_stichtag) < src.Gueltig_bis
    AND src.Ladedatum < PARSE_DATE('%d%m%Y', v_stichtag)
    AND src.DWH_VERTRAG_ID > v_restart_value;

  -- 7. Mark success
  UPDATE `project.dataset.job_audit`
  SET status = 'OK',
      finished_at = CURRENT_TIMESTAMP()
  WHERE job_kennung = v_job_kennung
    AND stichtag = v_stichtag
    AND restart_value = v_restart_value
    AND status = 'STARTED';

EXCEPTION WHEN ERROR THEN
  -- 8. Error handling: Log error and re-raise
  INSERT INTO `project.dataset.job_audit`
  (job_kennung, stichtag, restart_value, status, message, created_at)
  VALUES
  (v_job_kennung, v_stichtag, v_restart_value, 'ERROR', @@error.message, CURRENT_TIMESTAMP());
  RAISE;
END;
```

## 6. External Dependencies
The original script does not directly interact with external systems like Oracle, SFTP, or S3 based on the `lineage_external_systems` analysis (which was empty). Its dependencies are primarily other shell scripts and potentially the DWH database (implied by `DWH$TA_C_VERTRAG`).

*   **DWH Database (`DWH$TA_C_VERTRAG`):** This source table will be migrated to a BigQuery table (e.g., `project.dataset.DWH_TA_C_VERTRAG`).
*   **FOS Target Table (`FOS_TABLE`):** This target table will be migrated to a BigQuery table (e.g., `project.dataset.FOS_TABLE`).
*   **Helper Scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`):** The functionalities of these scripts will be re-implemented directly within the BigQuery Stored Procedure using SQL constructs (e.g., `IF`, `DECLARE`, `FORMAT_DATE`, `PARSE_DATE`) and dedicated logging tables.
*   **Core Script (`k_ausd_bp_ta_bpr_beschr.ksh`):** This script contains the primary business logic for data extraction, transformation, and loading. It will need to be separately analyzed and migrated into a BigQuery Stored Procedure or a set of SQL statements that can be called by the main orchestration procedure.

## 7. Unresolved / Risks
*   **Missing `k_ausd_bp_ta_bpr_beschr.ksh` analysis:** The core data transformation logic in `k_ausd_bp_ta_bpr_beschr.ksh` was not part of this analysis, as it is an invoked script. Its migration design will be critical and needs a separate analysis to fully define the ETL process.
*   **Detailed Logging:** The current logging in the KornShell script is file-based. While the BigQuery pseudocode includes basic audit table inserts, a more comprehensive logging strategy, including detailed error messages and execution traces, should be implemented for BigQuery.
*   **Environment Variables:** The script uses `BERT_DIR_ROOT` and potentially other environment variables. These will need to be handled in BigQuery either through configuration tables, procedure parameters, or by embedding values directly if they are static.
*   **`MIN(sysdate, maxladedatum)` Logic:** The commented-out section for `MIN(sysdate, maxladedatum)` suggests an intended logic that was not fully implemented. If this logic is a business requirement, it needs to be explicitly re-introduced in the BigQuery migration.
*   **Permissions and Access Control:** Ensure appropriate BigQuery IAM roles and permissions are configured for the stored procedures and tables.

## 8. Build Plan
The migration will primarily involve writing BigQuery SQL for stored procedures and DDL for tables.

1.  **Create Target Tables (DDL - BigQuery SQL):**
    *   `project.dataset.DWH_TA_C_VERTRAG` (if not already existing as a migrated DWH table)
    *   `project.dataset.FOS_TABLE`
    *   `project.dataset.job_log` (for general job logging)
    *   `project.dataset.job_audit` (for specific job execution audit)

2.  **Migrate Core Business Logic (`k_ausd_bp_ta_bpr_beschr.ksh`) (BigQuery SQL Stored Procedure):**
    *   Analyze `k_ausd_bp_ta_bpr_beschr.ksh` to extract the exact SQL queries, transformations, and control flow.
    *   Create a BigQuery Stored Procedure (e.g., `project.dataset.process_contract_cache_data`) that takes the necessary parameters (`stichtag`, `restart_value`, `job_nr`) and performs the data manipulation (DELETE/INSERT).

3.  **Create Orchestration Stored Procedure (`ausd_bp_ta_bpr_beschr`) (BigQuery SQL Stored Procedure):**
    *   Implement the `CREATE OR REPLACE PROCEDURE` statement for `project.dataset.ausd_bp_ta_bpr_beschr` as detailed in Section 5.
    *   Ensure calls to the core business logic stored procedure (from step 2) are correctly integrated.

4.  **Develop Parameter Handling and Date Utilities (BigQuery SQL):**
    *   Implement `FORMAT_DATE`, `PARSE_DATE`, `IFNULL`, etc., for date and parameter handling. These are standard BigQuery functions.

5.  **Implement Error Handling and Logging (BigQuery SQL):**
    *   Ensure the `EXCEPTION WHEN ERROR THEN` block in the stored procedure correctly logs errors to `project.dataset.job_audit` and re-raises them.
    *   Define the schema for `project.dataset.job_log` and `project.dataset.job_audit` to capture all necessary audit and error information.

6.  **Orchestration (Cloud Composer/Workflows):**
    *   If this job is part of a larger workflow, create an Airflow DAG (using Python) in Cloud Composer to call the `project.dataset.ausd_bp_ta_bpr_beschr` stored procedure, passing parameters as needed.

7.  **Testing:**
    *   Develop unit and integration tests for the BigQuery stored procedures to ensure functional parity with the original KornShell script.

This detailed plan will enable a semi-automated migration to BigQuery, addressing the `medium` complexity and `semi_auto` bucket rating.