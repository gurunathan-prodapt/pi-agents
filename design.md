# Migration Design — DW.BERT_AUSD_BP_TA_RN_DA_VDA_TK

## 1. Purpose & Scope

This job, originating as a UC4 Automic `JOBS_UNIX` object, is responsible for the "Aufbereitung der instantiierten Basisprodukte" (preparation of instantiated basic products) for the BERT process. The core functionality involves selecting and transforming data related to contract IDs and MSISDNs (mobile numbers) from a source table and inserting them into a target table, based on a specific date and filtering criteria. The job orchestrates a series of KornShell scripts that manage environment setup, parameter parsing, date validation, and the execution of the main Oracle SQL transformation.

The scope of this migration is to re-platform this ETL workflow from its current UC4/KornShell/Oracle environment to a Google Cloud Platform (GCP) ecosystem, utilizing:
*   **Airflow** for workflow orchestration.
*   **Horizon Python** for wrapper and control logic.
*   **BigQuery SQL** for data transformation.

## 2. Source Inventory

The job consists of the following primary components:

*   **Orchestration (UC4 XML Job Definition)**
    *   **File:** `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_RN_DA_VDA_TK.xml`
    *   **Technology:** UC4/Automic
    *   **Tier:** Medium
    *   **Automation Bucket:** Semi-Auto
    *   **Description:** Defines a UNIX job that initiates the execution of a KornShell script. It has no explicit schedule defined in the provided XML and is treated as an on-demand or externally triggered job.

*   **Wrapper KornShell Script**
    *   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh`
    *   **Technology:** KornShell
    *   **Tier:** Medium
    *   **Automation Bucket:** Semi-Auto
    *   **Description:** This script acts as the initial entry point after the UC4 job. It handles environment setup, parses input parameters (`p_stichtag`, `p_wiederanlaufWert`), includes shared utility scripts for error handling and date operations, and then invokes the core KornShell control script.

*   **Core KornShell Control Script**
    *   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_da_vda_tk.ksh`
    *   **Technology:** KornShell
    *   **Tier:** Medium
    *   **Automation Bucket:** Semi-Auto
    *   **Description:** This script is central to the job's logic. It parses parameters passed from the wrapper script, performs validation, determines current/previous dates, and crucially, executes the main Oracle SQL transformation script. It also prepares temporary files for tracking record counts.

*   **Oracle SQL Transformation Script**
    *   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_rn_da_vda_tk.sql`
    *   **Technology:** Oracle SQL
    *   **Tier:** Complex
    *   **Automation Bucket:** Manual
    *   **Description:** Contains the core data manipulation logic. It retrieves a date from a metadata table, truncates a temporary target table, and then performs an `INSERT ... SELECT` operation to populate the table with filtered data from a source table.

## 3. Target Architecture

The migrated job will be implemented on GCP with the following architecture:

*   **Orchestration Layer:**
    *   **Airflow DAG:** The UC4 job will be converted into an Airflow DAG (`dw_bert_ausd_bp_ta_rn_da_vda_tk`).
    *   **Tasks:** A single `DataprocSubmitJobOperator` task will be used to execute the Horizon Python script that encapsulates the business logic.
    *   **Schedule:** No explicit schedule was found in the UC4 XML, so the DAG will be created with `schedule=None`, implying manual triggering or external scheduling.

*   **Transformation & Control Layer:**
    *   **Horizon Python Script:** The logic from `r_ausd_bp_ta_rn_da_vda_tk.ksh` and `k_ausd_bp_ta_rn_da_vda_tk.ksh` will be consolidated into a single Horizon Python script (e.g., `dw_bert_ausd_bp_ta_rn_da_vda_tk.py`). This script will handle:
        *   Environment variable loading.
        *   Parameter parsing and validation (`job_kennung`, `eintrags_nr`, `stichtag`, `wiederanlauf_wert`).
        *   Date handling (e.g., deriving today and yesterday).
        *   Execution of the BigQuery SQL transformation using `script.func_execute_bq`.
        *   Error logging and process status updates (replacing legacy `DWMSG` functions).
    *   **BigQuery SQL:** The `d_ausd_bp_ta_rn_da_vda_tk.sql` logic will be directly translated into BigQuery SQL. This SQL will be embedded or dynamically loaded by the Horizon Python script.

*   **Data Layer:**
    *   **BigQuery Datasets/Tables:**
        *   Source Table: `sof$ta_rn_einzeln` will be migrated to a BigQuery table (e.g., `your_dataset.sof_ta_rn_einzeln`).
        *   Target Table: `sof$ta_rn_da_vda_tk` will be migrated to a BigQuery table (e.g., `your_dataset.sof_ta_rn_da_vda_tk`). This table will be truncated and re-populated.
        *   Metadata Table: `isbert_schema.dwtk_meldungen` will be migrated to a BigQuery table (e.g., `your_metadata_dataset.dwtk_meldungen`).

## 4. Data Flow & Lineage

The migrated data flow will be as follows:

1.  **Airflow DAG `dw_bert_ausd_bp_ta_rn_da_vda_tk`:** Triggered manually or by an external system.
2.  **Airflow Task `run_bert_ausd_bp_ta_rn_da_vda_tk`:** Initiates a Dataproc job to execute the Horizon Python script (`dw_bert_ausd_bp_ta_rn_da_vda_tk.py`).
3.  **Horizon Python Script:**
    *   Parses input parameters (`job_kennung`, `eintrags_nr`, `stichtag`, `wiederanlauf_wert`).
    *   Retrieves the latest `timecreated` for `BERT_DROP_TEMP_TABLE` from `your_metadata_dataset.dwtk_meldungen`.
    *   **BigQuery SQL Execution:** Calls `script.func_execute_bq` to run the BigQuery SQL.
        *   **Action 1 (Truncate):** `TRUNCATE TABLE your_dataset.sof_ta_rn_da_vda_tk;`
        *   **Action 2 (Insert):** `INSERT INTO your_dataset.sof_ta_rn_da_vda_tk` from `your_dataset.sof_ta_rn_einzeln`.
    *   Logs execution status and record counts.

**Lineage:**
`your_metadata_dataset.dwtk_meldungen` (READ) -> `Horizon Python Script` (determines `v_datum`)
`your_dataset.sof_ta_rn_einzeln` (READ) -> `BigQuery SQL Transformation`
`BigQuery SQL Transformation` -> `your_dataset.sof_ta_rn_da_vda_tk` (TRUNCATE & WRITE)

## 5. Transformation Logic

### Oracle SQL to BigQuery SQL Conversion:

The Oracle SQL script (`d_ausd_bp_ta_rn_da_vda_tk.sql`) will be translated to the following BigQuery SQL:

```sql
-- Step 00: derive substitution date
DECLARE v_datum STRING DEFAULT (
  SELECT COALESCE(
    FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)),
    '19000101'
  )
  FROM `isbert_schema.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Step 01: truncate target table
TRUNCATE TABLE `sof$ta_rn_da_vda_tk`;

-- Step 05_c: insert filtered rows
INSERT INTO `sof$ta_rn_da_vda_tk`
(
  CNTRCT_ID,
  DA_RN_MSISDN,
  DA_RN_STATUS,
  DA_RN_VALID_TO,
  VDA_RN_MSISDN,
  VDA_RN_STATUS,
  VDA_RN_VALID_TO,
  TK_RN_MSISDN,
  TK_RN_STATUS,
  TK_RN_VALID_TO
)
SELECT
  cntrct_id,
  DA_RN_msisdn,
  DA_RN_status,
  DA_RN_valid_to,
  VDA_RN_msisdn,
  VDA_RN_status,
  VDA_RN_valid_to,
  TK_RN_msisdn,
  TK_RN_status,
  TK_RN_valid_to
FROM `sof$ta_rn_einzeln` rp
WHERE DA_RN_msisdn IS NOT NULL
   OR VDA_RN_msisdn IS NOT NULL
   OR TK_RN_msisdn IS NOT NULL;
```

**Key changes:**
*   `DEFINE`, `COLUMN ... NEW_VALUE`, `PROMPT`, `WHENEVER SQLERROR`, `COMMIT`, `START`, `SPOOL`, `EXIT` (SQL*Plus/scripting constructs) are removed as they are handled by the Horizon Python script or are not applicable in BigQuery.
*   Oracle hints `/*+ full(rp) parallel(rp,4) */` are ignored.
*   `NVL` is replaced with `COALESCE`.
*   `TO_CHAR(MAX(m.timecreated),\'YYYYMMDD\')` is replaced with BigQuery's `FORMAT_TIMESTAMP` for date formatting.
*   Table names will be updated to fully qualified BigQuery table paths (e.g., `your_dataset.table_name`).

### KornShell Logic to Horizon Python:

The `r_ausd_bp_ta_rn_da_vda_tk.ksh` and `k_ausd_bp_ta_rn_da_vda_tk.ksh` scripts' logic will be translated into a Horizon Python script. This script will perform:

1.  **Environment Setup:** Assumed to be handled by the Horizon runtime environment or explicit `os.getenv` calls for `BERT_DIR_ROOT`, `DW_DIR_UTL`.
2.  **Parameter Parsing:** Parameters `j`, `f`, `s`, `l` will be parsed from command-line arguments using standard Python argument parsing libraries or Horizon's `get_runtime_param` equivalents.
3.  **Validation:** Python logic will replace `pruefeParameterGesetzt` and `DWDate_Datum_Check` to ensure required parameters are present and dates are correctly formatted.
4.  **Date Derivation:** Python functions will replace `gestern.ksh` to determine `p_datum_heute` and `p_datum_gestern`.
5.  **SQL Execution:** The `starteSQLSkript` function will be replaced by a call to `script.func_execute_bq`, passing the generated BigQuery SQL query.
6.  **Record Count:** The `cat $tmpFile` logic will be replaced by reading the output from the `func_execute_bq` call or a designated temporary storage.
7.  **Error Handling:** Python's `try-except` blocks and Horizon's logging mechanisms will replace the KornShell `trap` and `DWMSG_MeldeFehler` calls.

## 6. External Dependencies

The original job has the following external dependencies, and their proposed replacements are:

*   **Oracle Database:**
    *   **Tables:** `isbert_schema.dwtk_meldungen`, `sof$ta_rn_einzeln`, `sof$ta_rn_da_vda_tk`.
    *   **Replacement:** These tables will be migrated to dedicated BigQuery tables within appropriate datasets (e.g., `your_metadata_dataset.dwtk_meldungen`, `your_dataset.sof_ta_rn_einzeln`, `your_dataset.sof_ta_rn_da_vda_tk`).
    *   **Procedure:** `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`.
    *   **Replacement:** The specific `TRUNCATE` command executed by this procedure will be directly translated into BigQuery SQL. If `DWPA_UTIL_SKRIPT` has other functions, they will need to be re-implemented in Python or as BigQuery stored procedures/UDFs based on actual usage.

*   **UC4 Automic:**
    *   **Orchestration:** UC4 `JOBS_UNIX` object.
    *   **Replacement:** Airflow DAG.

*   **KornShell Utilities/Helper Scripts:**
    *   `. $HOME/.dw_init`: Environment initialization script.
    *   `f_alis_msgerr.ksh`: Error handling framework.
    *   `h_alis_date.ksh`: Date handling utilities.
    *   `h_alis_parameter.ksh`: Parameter parsing utilities.
    *   `h_alis_sqlplus.ksh`: SQL*Plus helper routines.
    *   `gestern.ksh`: Script to get today/yesterday dates.
    *   `starteSQLSkript`: Custom function to execute SQL.
    *   **Replacement:** These functionalities will be re-implemented or replaced by Python equivalents, Airflow features, or Horizon Framework utilities within the Horizon Python script. Environment variables will be managed by the GCP runtime environment (e.g., GKE for Airflow, Dataflow, or Cloud Functions).

*   **Temporary Files:**
    *   `${DW_DIR_UTL}/bert_k_ausd_bp_ta_rn_da_vda_tk.tmp`: Used for storing record counts.
    *   **Replacement:** In Horizon Python, this can be handled by in-memory variables or temporary GCS objects if persistence is required. The `script.func_execute_bq` call can directly return result metadata, eliminating the need for file-based record counting.

## 7. Unresolved / Risks

*   **`AL??` Comments:** Several `AL??` comments in the KornShell scripts (e.g., `AL?? . ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_fos_date.ksh`, `AL?? FOSHoleLadedatum`, `AL?? FOSJobDeaktivate`, `AL?? FOSJobErzeugeEintrag`) indicate potentially incomplete or commented-out functionality in the source. Clarification is needed on whether these features are still required or if they represent deprecated logic. The `FOSJobErzeugeEintrag` (job tracking) is currently commented out in the source, implying it's not active, but its future state should be confirmed.
*   **Parameterized `p_wiederanlaufWert`:** The `p_wiederanlaufWert` parameter is passed to the core SQL execution, but its exact usage within the `d_ausd_bp_ta_rn_da_vda_tk.sql` script is not directly visible in the provided SQL. Its role in restartability or incremental processing needs to be fully understood to ensure correct BigQuery implementation.
*   **Commented-out Post-processing:** The `k_ausd_bp_ta_rn_da_vda_tk.ksh` script contains extensive commented-out `sed`, `sort`, and `join` commands for post-processing data files. It should be confirmed if this functionality is intended to be revived in the migration or if it can be ignored. If needed, these transformations would be implemented using BigQuery SQL or PySpark/Python in a subsequent task.
*   **Shared Environment (`.dw_init`)**: The `.dw_init` script is crucial for setting up the environment. Its contents need to be fully analyzed to identify all environment variables and paths that must be replicated or replaced in the GCP environment (e.g., via Airflow variables, Kubernetes secrets, or within the Horizon Python script).
*   **Error Handling Framework:** The `f_alis_msgerr.ksh` and `DWMSG_*` functions are part of a custom error handling framework. This will need to be replaced with GCP-native logging (Cloud Logging) and potentially custom error notification mechanisms (e.g., Pub/Sub, Cloud Functions).
*   **Placeholder Values:** GCP project ID, region, Dataproc cluster name, and GCS bucket names are currently placeholders in the Airflow DAG design and need to be configured specifically for the target environment.

## 8. Build Plan

The migration will involve building the following components:

1.  **BigQuery Tables (DDL):**
    *   `your_dataset.sof_ta_rn_einzeln` (create table statement based on source schema)
    *   `your_dataset.sof_ta_rn_da_vda_tk` (create table statement based on target schema)
    *   `your_metadata_dataset.dwtk_meldungen` (create table statement based on source schema)
    *   **Language:** BigQuery DDL

2.  **Airflow DAG:**
    *   `dw_bert_ausd_bp_ta_rn_da_vda_tk.py`
    *   **Language:** Python
    *   **Content:** Based on the `uc4_to_airflow_dag_design` output, calling the Horizon Python script via `DataprocSubmitJobOperator`.

3.  **Horizon Python Script:**
    *   `dw_bert_ausd_bp_ta_rn_da_vda_tk.py` (or similar naming convention)
    *   **Language:** Python
    *   **Content:** Combines the logic from `r_ausd_bp_ta_rn_da_vda_tk.ksh` and `k_ausd_bp_ta_rn_da_vda_tk.ksh`, including parameter parsing, date derivation, and executing the BigQuery SQL.

4.  **BigQuery SQL Script:**
    *   `d_ausd_bp_ta_rn_da_vda_tk_bq.sql` (or embedded directly in the Horizon Python script)
    *   **Language:** BigQuery SQL
    *   **Content:** The translated SQL query for truncating and inserting data, as detailed in Section 5.

**Order of Build:**
1.  Create BigQuery DDL for all source and target tables.
2.  Develop the BigQuery SQL transformation.
3.  Develop the Horizon Python script, integrating the BigQuery SQL.
4.  Develop the Airflow DAG to orchestrate the Horizon Python script.
5.  Deploy BigQuery tables.
6.  Deploy Horizon Python script to GCS or relevant execution environment.
7.  Deploy Airflow DAG.