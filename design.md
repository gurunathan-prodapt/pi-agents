# Migration Design — DW.BERT_AUSD_BP_TA_ICCID_VERTRAG

## 1. Purpose & Scope
The job `DW.BERT_AUSD_BP_TA_ICCID_VERTRAG` is an ETL process originating from an Automic (UC4) scheduler. Its primary purpose is to prepare and load aggregated ICCID (SIM card ID) data into the `SOF$TA_ICCID_VERTRAG` table for reporting and downstream consumption by the BERT (Basisprodukte) system. It processes data from `SOF$TA_ICCID_EINZELN`, performing grouping and pivoting operations to consolidate multiple ICCID types per contract ID into a single record.

The scope of this migration is to re-platform this entire ETL workflow to Google Cloud Platform, specifically leveraging BigQuery for data storage and transformation, and Airflow (via Cloud Composer) for orchestration.

## 2. Source Inventory

| File Path                                                                                                   | Technology  | Complexity Tier | Automation Bucket | Summary                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| :---------------------------------------------------------------------------------------------------------- | :---------- | :-------------- | :---------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_ICCID_VERTRAG.xml` | UC4/Automic | Medium          | Semi-Auto         | UC4 job definition for a UNIX job named `DW.BERT_AUSD_BP_TA_ICCID_VERTRAG`, responsible for preparing instantiated base products by executing a shell script. This acts as the top-level orchestrator.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_vertrag.ksh`                 | KornShell   | Medium          | Semi-Auto         | This KornShell script acts as an orchestrator, parsing command-line arguments for a 'Stichtag' (key date) and a 'Wiederanlaufwert' (restart value), then executing a core data preparation script (`k_ausd_bp_ta_iccid_vertrag.ksh`) with these parameters and robust error handling.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_vertrag.ksh`                 | KornShell   | Medium          | Semi-Auto         | This KornShell script acts as a control script, parsing parameters, performing date validation, executing a SQL script via a wrapper, and handling post-execution steps including job status logging. It also contains commented-out sections for shell-based data reformatting and joining.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_iccid_vertrag.sql`                 | Oracle SQL  | Complex         | Retire            | This Oracle SQL script truncates the `SOF$TA_ICCID_VERTRAG` table and then populates it by aggregating ICCID (SIM card ID) data from `SOF$TA_ICCID_EINZELN`, grouping by contract ID and pivoting multiple ICCID types into columns. The complexity is due to the extensive list of columns (MS1_ICCID to MS10_VALID_TO) indicative of complex pivoting logic.                                                                                                                                                                                                                                                                                                                                                                                                           |

## 3. Target Architecture

The migrated job will run on Google Cloud Platform, leveraging the following services:

*   **Orchestration:** Apache Airflow on Cloud Composer.
*   **Data Storage & Transformation:** BigQuery.
*   **Logging & Monitoring:** Cloud Logging and Cloud Monitoring.
*   **Temporary File Storage:** Cloud Storage (for any intermediate file requirements, though minimal for this flow).

The overall architecture will involve an Airflow DAG (`dw_bert_ausd_bp_ta_iccid_vertrag`) responsible for triggering a BigQuery Stored Procedure. This stored procedure (`r_ausd_bp_ta_iccid_vertrag_sp`) will encapsulate the logic from the shell orchestrator, manage parameters, and then invoke another BigQuery Stored Procedure (`k_ausd_bp_ta_iccid_vertrag_sp`). The inner stored procedure (`k_ausd_bp_ta_iccid_vertrag_sp`) will execute the core SQL transformation, which will be migrated to BigQuery SQL, acting directly on BigQuery tables.

**BigQuery Dataset Structure:**
*   `project.source_dataset`: Contains source tables like `sof_ta_iccid_einzeln`.
*   `project.target_dataset`: Contains target tables like `sof_ta_iccid_vertrag`.
*   `project.audit_dataset`: For job logging and registry tables (`job_log`, `job_registry`).

**Naming Conventions:**
*   Airflow DAG: `dw_bert_ausd_bp_ta_iccid_vertrag`
*   BigQuery Stored Procedures:
    *   `project.dataset.r_ausd_bp_ta_iccid_vertrag_sp` (replacing `r_ausd_bp_ta_iccid_vertrag.ksh`)
    *   `project.dataset.k_ausd_bp_ta_iccid_vertrag_sp` (replacing `k_ausd_bp_ta_iccid_vertrag.ksh`)
*   BigQuery SQL transformation: Embedded within `k_ausd_bp_ta_iccid_vertrag_sp`.
*   Tables: Oracle `SOF$TA_ICCID_EINZELN` will become `source_dataset.sof_ta_iccid_einzeln`. Oracle `SOF$TA_ICCID_VERTRAG` will become `target_dataset.sof_ta_iccid_vertrag`. Schema names (e.g., `isbert_schema`) will map to BigQuery datasets.

## 4. Data Flow & Lineage

The data flow will be as follows:

1.  **Airflow DAG (`dw_bert_ausd_bp_ta_iccid_vertrag`)**:
    *   Triggered by a schedule (to be defined, as no `EVNT_TIME` was provided in source UC4).
    *   Executes a single `BigQueryStartStoredProcedureOperator` task.
    *   Calls the `project.dataset.r_ausd_bp_ta_iccid_vertrag_sp` with required parameters (e.g., `p_stichtag`, `p_wiederanlaufWert`).

2.  **BigQuery Stored Procedure (`project.dataset.r_ausd_bp_ta_iccid_vertrag_sp`)**:
    *   Receives `p_stichtag` (cutoff date) and `p_wiederanlaufWert` (restart value) as input.
    *   Initializes internal variables and logs job start to `project.audit_dataset.job_registry` and `project.audit_dataset.job_log`.
    *   Validates parameters (`p_stichtag` format, presence of required arguments).
    *   Calls `project.dataset.k_ausd_bp_ta_iccid_vertrag_sp` passing job-specific parameters (job identifier, entry number, cutoff date, restart value).
    *   Handles exceptions, logs errors, and updates job status in `project.audit_dataset.job_registry`.

3.  **BigQuery Stored Procedure (`project.dataset.k_ausd_bp_ta_iccid_vertrag_sp`)**:
    *   Receives `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert` as input.
    *   Further validates input parameters, including date format.
    *   Calculates `v_datum_heute` and `v_datum_gestern` using BigQuery date functions.
    *   Executes the core data transformation logic (derived from `d_ausd_bp_ta_iccid_vertrag.sql`):
        *   Truncates `project.target_dataset.sof_ta_iccid_vertrag`.
        *   Inserts data into `project.target_dataset.sof_ta_iccid_vertrag` by performing a `SELECT` statement that aggregates ICCID data from `project.source_dataset.sof_ta_iccid_einzeln` and groups by `cntrct_id`, pivoting various ICCID-related fields.
    *   Captures the number of records processed.
    *   Updates `project.audit_dataset.job_registry` with job completion status and processed record count.
    *   Handles errors and exceptions.

**Lineage Summary:**
`Airflow DAG` -> `r_ausd_bp_ta_iccid_vertrag_sp` -> `k_ausd_bp_ta_iccid_vertrag_sp` -> `SELECT FROM project.source_dataset.sof_ta_iccid_einzeln` and `TRUNCATE/INSERT INTO project.target_dataset.sof_ta_iccid_vertrag`.

## 5. Transformation Logic

### 5.1 UC4 Orchestration to Airflow DAG
The UC4 XML defines a simple Unix job that executes `r_ausd_bp_ta_iccid_vertrag.ksh`. This will be directly translated into an Airflow DAG:
*   **DAG ID:** `dw_bert_ausd_bp_ta_iccid_vertrag`
*   **Schedule:** Not derivable from source; will require manual configuration (e.g., `@daily`, `cron="0 0 * * *"`) or external trigger.
*   **Task:** A single `BigQueryStartStoredProcedureOperator` calling `project.dataset.r_ausd_bp_ta_iccid_vertrag_sp`.
*   **Parameters:** UC4 job variables will be mapped to Airflow task parameters or BigQuery stored procedure arguments.
    *   `&DWH_JOB_KENNUNG` will be passed as `job_kennung='AUSD_BP_TA_ICCID_VERTRAG'`.
    *   The `Stichtag` and `Wiederanlaufwert` parameters will be passed from the Airflow DAG to the `r_ausd_bp_ta_iccid_vertrag_sp`.

### 5.2 Shell Script (`r_ausd_bp_ta_iccid_vertrag.ksh`) to BigQuery Stored Procedure
This script's logic will be converted into a BigQuery Stored Procedure: `project.dataset.r_ausd_bp_ta_iccid_vertrag_sp`.
*   **Parameter Handling:** `getopts` logic will be replaced with direct BigQuery stored procedure input parameters (`p_stichtag STRING`, `p_wiederanlaufWert INT64`). Default values and checks for missing parameters will use `IF` statements and `IS NULL` checks.
*   **Date Determination:** Shell commands for `sysdate` and `min(sysdate, maxladedatum)` will be replaced by BigQuery `CURRENT_DATE()`, `FORMAT_DATE()`, `COALESCE()`, and `SAFE.PARSE_DATE()`.
*   **Error Handling:** Shell traps and `DWMSG_*` functions will be replaced by BigQuery `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks and `RAISE` statements. Logging will be directed to an audit table (`project.audit_dataset.job_log`).
*   **Core Logic Execution:** The call to `k_ausd_bp_ta_iccid_vertrag.ksh` will become a `CALL` statement to `project.dataset.k_ausd_bp_ta_iccid_vertrag_sp`.

### 5.3 Shell Script (`k_ausd_bp_ta_iccid_vertrag.ksh`) to BigQuery Stored Procedure
This script's logic will be converted into a BigQuery Stored Procedure: `project.dataset.k_ausd_bp_ta_iccid_vertrag_sp`.
*   **Parameter Handling:** `getopts` logic will be replaced with direct BigQuery stored procedure input parameters (`p_JobKennung STRING`, `p_EintragsNr STRING`, `p_Stichtag STRING`, `p_wiederanlaufWert STRING`).
*   **Date Validation:** `DWDate_Datum_Check` will be replaced with `SAFE.PARSE_DATE()` and `IF` checks for format validity.
*   **SQL Execution:** The `starteSQLSkript` wrapper function and the explicit SQL file reference will be replaced by direct execution of the BigQuery-translated SQL (`d_ausd_bp_ta_iccid_vertrag.sql`) embedded within this stored procedure.
*   **Record Count:** Reading from a temporary file (`tmpFile`) will be replaced by capturing the `ROW_COUNT()` after the `INSERT` operation or a `SELECT COUNT(*)` from the target table.
*   **Commented-out Post-processing:** The `sed`, `sort`, `join` commands in the commented-out section will be ignored as they were inactive. If this logic ever becomes active, it would require migration to BigQuery DML/SQL.

### 5.4 Oracle SQL (`d_ausd_bp_ta_iccid_vertrag.sql`) to BigQuery SQL
The core data transformation will be directly translated into BigQuery SQL.
*   **Table References:** Oracle table names like `sof$ta_iccid_vertrag` and `sof$ta_iccid_einzeln` will be converted to BigQuery dataset.table format, e.g., ``project.target_dataset.sof_ta_iccid_vertrag`` and ``project.source_dataset.sof_ta_iccid_einzeln``.
*   **`TRUNCATE TABLE`:** Oracle `TRUNCATE TABLE sof$ta_iccid_vertrag REUSE STORAGE` becomes BigQuery `TRUNCATE TABLE ``project.target_dataset.sof_ta_iccid_vertrag```.
*   **`NVL` to `COALESCE`:** Oracle `NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101')` becomes BigQuery `COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')`.
*   **Optimizer Hints:** Oracle `/*+ full(rp) parallel(rp,4) */` will be removed as BigQuery handles query optimization automatically.
*   **`COMMIT;`:** Explicit `COMMIT` statements are not needed in BigQuery as DML operations are atomic.
*   **Variable Declarations:** Oracle `DEFINE` and `COLUMN s_datum new_value v_datum noprint` will be converted to BigQuery `DECLARE` statements and variable assignments within the stored procedure.
*   **Function Mapping:** Standard SQL functions used in Oracle are expected to have direct BigQuery equivalents.
*   **Pivoting Logic:** The complex `MAX(column) ... GROUP BY cntrct_id` for multiple `MSx_ICCID`, `MSx_IMSI_*` columns implies a pivoting operation. BigQuery handles this pattern well.

## 6. External Dependencies

| Original System/Component | Type          | How it will be replaced/handled in GCP                                                                                                                                                                                                                                                                                                                                                                                                                             |
| :------------------------ | :------------ | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| UC4 Scheduler             | Orchestration | Replaced by Apache Airflow on Cloud Composer. The UC4 job will be converted into an Airflow DAG.                                                                                                                                                                                                                                                                                                                                                                 |
| Oracle Database           | Database      | Replaced by BigQuery. Source tables (`sof$ta_iccid_einzeln`) and target tables (`sof$ta_iccid_vertrag`) will be migrated to BigQuery. The `isbert_schema.dwtk_meldungen` table will also need to be available in BigQuery (e.g., `project.source_dataset.dwtk_meldungen`).                                                                                                                                                                                                |
| KornShell (ksh) scripts   | Scripting     | The orchestration and control logic within `r_ausd_bp_ta_iccid_vertrag.ksh` and `k_ausd_bp_ta_iccid_vertrag.ksh` will be migrated into BigQuery Stored Procedures. Generic shell utilities (like `print`, `getopts`) will be replaced by BigQuery scripting constructs. External utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) will be replaced by BigQuery functions, direct parameter passing, or audit tables. |
| Unix Host (`DWHDWH2P`)    | Execution Env | The execution of scripts will be handled by the Cloud Composer worker environment (for Airflow) and BigQuery (for SQL and Stored Procedures).                                                                                                                                                                                                                                                                                                                    |
| Temporary Files (`.tmp`)  | File I/O      | The use of temporary files for record counts will be replaced by BigQuery scripting variables or temporary tables. If the commented-out file processing (sed, sort, join) were ever activated, it would require reconsideration with Cloud Storage/Dataflow or BigQuery external tables.                                                                                                                                                                         |

## 7. Unresolved / Risks

*   **UC4 Schedule:** The original UC4 job's schedule was not provided. The Airflow DAG will initially be created without a defined schedule, requiring manual configuration based on business requirements.
*   **External Utility Scripts:** The exact content and dependencies of sourced shell scripts like `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, and `gestern.ksh` were not analyzed. Their functionality has been assumed to be standard environment setup, logging, date utilities, and parameter parsing. Any complex logic within these scripts that cannot be directly translated to BigQuery SQL or Python will need separate analysis and migration (e.g., custom UDFs, Cloud Functions).
*   **`DWMSG_*` Functions:** The specific implementation of `DWMSG_MeldeFehler`, `DWMSG_ErmittleNr`, etc., is currently mapped to generic audit log table inserts and error raising. The full functionality of these custom error/logging mechanisms should be reviewed to ensure no critical features are lost.
*   **Oracle `isbert_schema.dwtk_meldungen`:** This table is referenced for variable definition. Its schema and data content need to be available in BigQuery as `project.source_dataset.dwtk_meldungen`.
*   **"Retire" Bucket for SQL:** The core SQL script was flagged as `retire`. This is a significant risk, implying a potential redesign or high manual effort. While a BigQuery SQL equivalent has been designed, the "retire" flag warrants a deeper review to understand the underlying reasons (e.g., outdated logic, poor performance, complexity) and whether a functional redesign is preferable to a direct lift-and-shift. This design assumes a like-for-like translation, but the "retire" status suggests this assumption might need re-evaluation.
*   **`p_wiederanlaufWert` Logic:** The restart logic involves deleting entries `>= p_wiederanlaufWert` and then only processing `DWH_VERTRAG_ID > p_wiederanlaufWert`. This logic needs to be carefully implemented in BigQuery to ensure data integrity and atomicity, potentially requiring a transaction or careful staging.
*   **Table Naming:** The use of `SOF$TA` in Oracle table names is atypical. While translated to `sof_ta` in BigQuery, any potential conflicts or non-standard characters in other table names should be reviewed.

## 8. Build Plan

The migration will involve the following steps:

1.  **BigQuery Audit and Log Tables:**
    *   Create `project.audit_dataset.job_registry` table to store job execution metadata (job ID, name, start/end time, status, parameters).
    *   Create `project.audit_dataset.job_log` table for detailed logging of job execution.
    *   **Language:** BigQuery DDL

2.  **BigQuery Source Table Schema Migration:**
    *   Ensure the `sof$ta_iccid_einzeln` table is migrated to BigQuery as `project.source_dataset.sof_ta_iccid_einzeln` with appropriate data types.
    *   Ensure `isbert_schema.dwtk_meldungen` is migrated to BigQuery as `project.source_dataset.dwtk_meldungen`.
    *   **Language:** BigQuery DDL

3.  **BigQuery Target Table Schema Creation:**
    *   Create `project.target_dataset.sof_ta_iccid_vertrag` table based on the output schema of the Oracle SQL script. Data types will be mapped from Oracle to BigQuery.
    *   **Language:** BigQuery DDL

4.  **BigQuery Stored Procedure for `d_ausd_bp_ta_iccid_vertrag.sql`:**
    *   Develop the BigQuery SQL transformation logic (from Section 5.4) and encapsulate it within a stored procedure or as an executable script. This will be called by `k_ausd_bp_ta_iccid_vertrag_sp`.
    *   **Language:** BigQuery SQL (within a Stored Procedure)

5.  **BigQuery Stored Procedure for `k_ausd_bp_ta_iccid_vertrag.ksh`:**
    *   Develop `project.dataset.k_ausd_bp_ta_iccid_vertrag_sp` that includes parameter handling, validation, date logic, and calls the core SQL transformation logic (from step 4).
    *   **Language:** BigQuery SQL (Stored Procedure)

6.  **BigQuery Stored Procedure for `r_ausd_bp_ta_iccid_vertrag.ksh`:**
    *   Develop `project.dataset.r_ausd_bp_ta_iccid_vertrag_sp` that includes top-level parameter handling, logging, error handling, and orchestrates the call to `project.dataset.k_ausd_bp_ta_iccid_vertrag_sp`.
    *   **Language:** BigQuery SQL (Stored Procedure)

7.  **Airflow DAG (`dw_bert_ausd_bp_ta_iccid_vertrag`) Implementation:**
    *   Create a Python file for the Airflow DAG.
    *   Define the DAG properties (DAG ID, `start_date`, `catchup`, `tags`).
    *   Implement a `BigQueryStartStoredProcedureOperator` task to call `project.dataset.r_ausd_bp_ta_iccid_vertrag_sp`.
    *   Configure parameters to be passed to the stored procedure.
    *   **Language:** Python

8.  **Deployment and Testing:**
    *   Deploy BigQuery tables and stored procedures.
    *   Deploy the Airflow DAG to Cloud Composer.
    *   Thoroughly test the DAG execution, parameter passing, data transformation, logging, and error handling.
    *   **Language:** GCP Deployment (Terraform/gcloud), Airflow deployment.

9.  **Schedule Configuration:**
    *   Define the appropriate Airflow schedule for the `dw_bert_ausd_bp_ta_iccid_vertrag` DAG based on business requirements.
    *   **Language:** Airflow Configuration.