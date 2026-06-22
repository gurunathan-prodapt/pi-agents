# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn_his.ksh

## 1. Purpose & Scope

This KornShell (ksh) script, `r_ausd_bp_ta_msisdn_his.ksh`, serves as an orchestration or wrapper script. Its primary purpose is to prepare parameters, initialize a job logging and error handling framework, and then invoke a core processing script, `k_ausd_bp_ta_msisdn_his.ksh`. This core script is responsible for providing selected basic products from a Data Warehouse (DWH) contract cache for a 'Forderungsscoring' (FOS) system, based on a specific cut-off date.

The script handles:
- Parsing command-line arguments for a cutoff date (`-s`) and a restart value (`-l`).
- Defaulting the cutoff date to the system date if not provided.
- Initializing a messaging/error handling framework (`DWMSG_...` functions).
- Setting up `trap` mechanisms for error management.
- Orchestrating the execution of the core script, passing along the derived parameters.
- Logging job status and messages to a file.

The scope of this migration document focuses on translating the orchestration logic of this wrapper script to BigQuery. The logic of the invoked core script (`k_ausd_bp_ta_msisdn_his.ksh`) is assumed to be handled separately or will become a called BigQuery Stored Procedure.

## 2. Source Inventory

| File Path                                                             | Technology  | Purpose            | Complexity Tier | Automation Bucket |
| :-------------------------------------------------------------------- | :---------- | :----------------- | :-------------- | :---------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn_his.ksh` | KornShell | Orchestrator/ETL | *Not Found*     | *Not Found*       |

**Note:** Complexity tier and automation bucket information were not found in the database for this file. Based on its role as a parameter-passing and orchestration script, it is likely of `simple` to `medium` complexity for migration of its wrapper logic.

## 3. Target Architecture

The target architecture for this script will involve BigQuery Stored Procedures for the orchestration logic.

- **Orchestration Layer:** A BigQuery Stored Procedure, `project.dataset.ausd_bp_ta_msisdn_his_wrapper`, will replace the `r_ausd_bp_ta_msisdn_his.ksh` script. This stored procedure will manage parameter handling, validation, logging, and the invocation of the core logic.
- **Logging/Auditing:** A BigQuery table, e.g., `project.dataset.dwmsg_job_audit`, will be created to capture job status, error messages, and log details, replacing the file-based logging of the source script.
- **Core Logic:** The core processing script, `k_ausd_bp_ta_msisdn_his.ksh`, is expected to be migrated into a separate BigQuery Stored Procedure (e.g., `project.dataset.k_ausd_bp_ta_msisdn_his`) or a data transformation pipeline orchestrated by Cloud Composer or Cloud Workflows.
- **External Orchestration (if applicable):** If the original UC4 invocation (`DW.BERT_AUSD_BP_TA_MSISDN_HIS.xml INVOKES SCRIPT:R_AUSD_BP_TA_MSISDN_HIS.KSH`) represents a larger workflow, Cloud Composer (Apache Airflow) or Cloud Workflows can be used to manage the scheduling and execution of the BigQuery Stored Procedures.

## 4. Data Flow & Lineage

The current script acts as an entry point and orchestrator.

**Inputs:**
- Command-line parameters: `-s Stichtag` (cutoff date DDMMYYYY), `-l Wiederanlaufwert` (restart value).
- Environment variables sourced from `$HOME/.dw_init`.
- System date (for defaulting `Stichtag`).

**Processing (within `r_ausd_bp_ta_msisdn_his.ksh`):**
1. Environment setup and helper script sourcing (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`).
2. Parameter parsing and validation.
3. Defaulting of `p_wiederanlaufWert` to 0 if not provided.
4. Determination of `p_stichtag` (defaults to system date if not provided).
5. Initialization of job identifier (`JobKennung`, `DW_EintragsNr`) and log file (`LogDatei`).
6. Setup of shell `trap` for error handling.
7. Invocation of the core script: `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_msisdn_his.ksh` with parameters.
8. Logging of job status and messages.

**Outputs:**
- Log entries written to a file (`$LogDatei`).
- Job status updates via `DWMSG_SetzeStatusOK`.
- Potential email notifications via `DWMSG_ERMITTLENR` on error.
- The core script (`k_ausd_bp_ta_msisdn_his.ksh`) is expected to perform the actual data processing, potentially reading from `DWH$TA_C_VERTRAG` and writing to `FOS-Tabelle`. (Though not explicitly captured in `lineage_edges` for this file, `file_analysis` indicates these as referenced objects).

**Execution Order:**
1. External scheduler (UC4, based on lineage) triggers `r_ausd_bp_ta_msisdn_his.ksh`.
2. `r_ausd_bp_ta_msisdn_his.ksh` performs setup and invokes `k_ausd_bp_ta_msisdn_his.ksh`.
3. `k_ausd_bp_ta_msisdn_his.ksh` executes core data processing.
4. Control returns to `r_ausd_bp_ta_msisdn_his.ksh` for final logging and exit.

## 5. Transformation Logic

The KornShell wrapper script will be migrated to a BigQuery Stored Procedure.

**Original KornShell Constructs and BigQuery Equivalents:**

- **Environment variables (`p_stichtag`, `p_wiederanlaufWert`, `JobKennung`, `DW_EintragsNr`, `LogDatei`):** These will be mapped to BigQuery Stored Procedure `IN` parameters and `DECLARE`d local variables.
- **Parameter parsing (`getopts`):** Replaced by BigQuery Stored Procedure parameters. No direct CLI parsing in SQL.
- **Conditional logic (`if`, `[[ ]]`):** Mapped to BigQuery procedural `IF ... THEN ... ELSE ... END IF;` statements.
- **Date functions (`DWDate_Gib_Zeitraum`):** Replaced by BigQuery's `CURRENT_DATE()`, `FORMAT_DATE()`, and other date functions.
- **Utility script sourcing (`. $HOME/.dw_init`, `. ${BERT_DIR_ROOT}/...`):**
    - Environment initialization (`. $HOME/.dw_init`): Relevant environment variables should be passed as parameters or configured in the BigQuery environment (e.g., through a configuration table).
    - Helper functions (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`): Their functionalities (`DWMSG_...`, `pruefeParameterGesetzt`) will be directly implemented as SQL logic within the stored procedure or as separate, smaller utility stored procedures.
- **Error handling (`set -e`, `trap`, `DWMSG_MeldeFehler`):** Replaced by BigQuery's `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` blocks for error trapping. Error messages will be logged to the audit table.
- **Logging (`print`, `tee -a $LogDatei`, redirection `>> $LogDatei 2>&1`):** Replaced by `INSERT` statements into a BigQuery audit/log table (`project.dataset.dwmsg_job_audit`).
- **Invocation of core script (`${Name_Kernskript} ...`):** Replaced by a `CALL` statement to the migrated BigQuery Stored Procedure representing `k_ausd_bp_ta_msisdn_his.ksh`.

**BigQuery SQL Pseudocode for the Wrapper Procedure:**

```sql
CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_msisdn_his_wrapper`(
  IN p_stichtag_in STRING,         -- Input Stichtag (DDMMYYYY)
  IN p_wiederanlaufWert_in INT64   -- Input Wiederanlaufwert
)
BEGIN
  DECLARE v_sysdate STRING;
  DECLARE v_stichtag STRING;
  DECLARE v_wiederanlaufWert INT64;
  DECLARE v_jobkennung STRING DEFAULT 'AUSD_BP_TA_MSISDN_HIS';
  DECLARE v_eintragsnr INT64;
  DECLARE v_logdatei STRING;
  -- DECLARE v_errnr INT64 DEFAULT 0; -- Error number (handled by BigQuery exceptions)
  -- DECLARE v_errarg STRING DEFAULT ''; -- Error argument (handled by BigQuery exceptions)
  DECLARE v_status STRING DEFAULT 'STARTED';

  -- Initialize Wiederanlaufwert
  SET v_wiederanlaufWert = IFNULL(p_wiederanlaufWert_in, 0);

  -- Determine system date
  SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

  -- Determine Stichtag (defaults to system date if not provided)
  SET v_stichtag = IFNULL(p_stichtag_in, v_sysdate);

  -- Parameter validation (pruefeParameterGesetzt)
  IF v_stichtag IS NULL OR v_stichtag = '' THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Required parameter Stichtag is missing';
  END IF;

  -- Determine job entry number (DWMSG_ErmittleNr)
  -- Assuming dwmsg_job_audit table exists and stores job_id and job_name
  SET v_eintragsnr = (
    SELECT IFNULL(MAX(job_id), 0) + 1
    FROM `project.dataset.dwmsg_job_audit`
    WHERE job_name = v_jobkennung
  );

  -- Generate log file name (DWMSG_Logdateiname)
  SET v_logdatei = CONCAT('job_', CAST(v_eintragsnr AS STRING), '_', v_jobkennung, '.log');

  -- Log job start (DWMSG_ErzeugeEintrag)
  INSERT INTO `project.dataset.dwmsg_job_audit` (job_id, job_name, script_name, log_file, stichtag, status, created_at)
  VALUES (v_eintragsnr, v_jobkennung, 'ausd_bp_ta_msisdn_his_wrapper', v_logdatei, v_stichtag, 'STARTED', CURRENT_TIMESTAMP());

  -- Set Stichtag Info (DWMSG_SetzeStichtagInfo) - Already captured in audit table

  -- Main logic block with exception handling (replaces 'trap' and error functions)
  BEGIN
    -- Call the core processing stored procedure
    -- This procedure would contain the logic of 'k_ausd_bp_ta_msisdn_his.ksh'
    CALL `project.dataset.k_ausd_bp_ta_msisdn_his`(
      v_jobkennung,
      v_stichtag,
      v_eintragsnr,
      v_wiederanlaufWert
    );

    -- If successful, update status (DWMSG_SetzeStatusOK)
    SET v_status = 'COMPLETED';
    INSERT INTO `project.dataset.dwmsg_job_audit` (job_id, job_name, script_name, log_file, stichtag, status, created_at)
    VALUES (v_eintragsnr, v_jobkennung, 'ausd_bp_ta_msisdn_his_wrapper', v_logdatei, v_stichtag, 'COMPLETED', CURRENT_TIMESTAMP());

  EXCEPTION WHEN ERROR THEN
    -- Handle error (DWMSG_Fehlerbehandlung, DWMSG_MeldeFehler)
    SET v_status = 'FAILED';
    INSERT INTO `project.dataset.dwmsg_job_audit` (job_id, job_name, script_name, log_file, stichtag, status, error_message, created_at)
    VALUES (v_eintragsnr, v_jobkennung, 'ausd_bp_ta_msisdn_his_wrapper', v_logdatei, v_stichtag, 'FAILED', @@error.message, CURRENT_TIMESTAMP());
    RAISE; -- Re-raise the error to propagate it
  END;

END;
```

## 6. External Dependencies

| Original Component                                      | Type        | Replacement / Handling in BigQuery                                                                    |
| :------------------------------------------------------ | :---------- | :---------------------------------------------------------------------------------------------------- |
| `$HOME/.dw_init`                                        | File        | Environment variables and configurations will be managed via BigQuery stored procedure parameters, BigQuery's native environment settings, or dedicated configuration tables. |
| `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` | Shell Script | Functionality (`DWMSG_...`) will be absorbed into the BigQuery stored procedure logic and use a BigQuery audit table. |
| `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` | Shell Script | Parameter handling will be done through BigQuery stored procedure parameters and internal SQL logic.  |
| `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` | Shell Script | Date manipulation will use BigQuery's built-in date and time functions.                               |
| `DWH$TA_C_VERTRAG`                                      | Table       | To be migrated to a BigQuery table, e.g., `project.dataset.dwh_ta_c_vertrag`.                         |
| `FOS-Tabelle`                                           | Table       | To be migrated to a BigQuery table, e.g., `project.dataset.fos_tabelle`.                              |
| `FOS-Loader`                                            | Job         | This job, which consumes the `FOS-Tabelle`, will need to be re-engineered to consume data from BigQuery. |
| UC4 Scheduler (invoking `r_ausd_bp_ta_msisdn_his.ksh`)  | Scheduler   | To be replaced by Cloud Composer (Apache Airflow), Cloud Workflows, or Cloud Scheduler.             |

## 7. Unresolved / Risks

- **Core Script (`k_ausd_bp_ta_msisdn_his.ksh`) Logic:** This document only covers the wrapper script. The full migration depends heavily on the detailed analysis and migration of the core script, which is assumed to become a BigQuery Stored Procedure or part of a data pipeline. If `k_ausd_bp_ta_msisdn_his.ksh` has complex file I/O or shell-specific operations, its migration may introduce further complexity or require solutions beyond pure BigQuery SQL (e.g., Cloud Functions, Dataflow).
- **Missing Complexity/Automation Rates:** The absence of `file_complexity` and `automation_rate` data means the effort estimation for this component is based on manual analysis, not automated tooling. This could lead to underestimation if hidden complexities exist.
- **DWMSG Framework:** The `DWMSG_...` functions are a custom logging/messaging framework. While a BigQuery audit table is proposed, full replication of all framework functionalities (e.g., custom error codes, detailed message formatting) might require more elaborate stored procedure logic or external logging solutions (e.g., Cloud Logging).
- **`BERT_DIR_ROOT`:** This environment variable indicates a dependency on a specific directory structure. Its equivalent in BigQuery will likely be hardcoded project/dataset/table names or parameters.
- **`DWH_VERTRAG_ID`:** This is a column reference. Its exact type and constraints will need to be preserved during schema migration to BigQuery.

## 8. Build Plan

1.  **Define Target Schemas:**
    *   Create `project.dataset.dwmsg_job_audit` table in BigQuery for logging and audit purposes.
    *   Define target BigQuery schemas for `DWH$TA_C_VERTRAG` (e.g., `project.dataset.dwh_ta_c_vertrag`) and `FOS-Tabelle` (e.g., `project.dataset.fos_tabelle`).
2.  **Translate Utility Functions:**
    *   Implement `pruefeParameterGesetzt` logic directly within the main wrapper stored procedure.
    *   Replace `DWDate_Gib_Zeitraum` logic with BigQuery's native date functions.
3.  **Create BigQuery Wrapper Stored Procedure:**
    *   Develop `project.dataset.ausd_bp_ta_msisdn_his_wrapper` using the provided BigQuery SQL pseudocode.
    *   Implement error handling using `BEGIN...EXCEPTION` blocks.
4.  **Migrate Core Script (`k_ausd_bp_ta_msisdn_his.ksh`):**
    *   (Parallel task) Analyze `k_ausd_bp_ta_msisdn_his.ksh` and design its migration (e.g., into `project.dataset.k_ausd_bp_ta_msisdn_his` stored procedure, or a Python/Spark job on Dataflow/Dataproc).
5.  **Update Invocation:**
    *   Modify the `ausd_bp_ta_msisdn_his_wrapper` stored procedure to `CALL` the migrated core processing logic.
6.  **Migrate Data:**
    *   Perform data migration for `DWH$TA_C_VERTRAG` and `FOS-Tabelle` from the source DWH to BigQuery.
7.  **Replace Scheduler:**
    *   Configure a Cloud Composer DAG or Cloud Workflows job to execute `project.dataset.ausd_bp_ta_msisdn_his_wrapper` at the appropriate schedule, replacing the UC4 dependency.
8.  **Integrate Downstream Systems:**
    *   Update `FOS-Loader` and any other downstream systems to consume data from the new BigQuery tables.

**Generated Files:**
*   `ausd_bp_ta_msisdn_his_wrapper.sql` (BigQuery SQL)
*   `dwmsg_job_audit_table.sql` (BigQuery DDL for audit table)
*   (Dependent on core script migration): `k_ausd_bp_ta_msisdn_his.sql` (BigQuery SQL) or Python/Spark script.
*   (Dependent on external orchestration): `airflow_dag_ausd_bp_ta_msisdn_his.py` (Python for Cloud Composer).