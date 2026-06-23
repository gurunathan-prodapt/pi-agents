# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh

## 1. Purpose & Scope

This document outlines the migration design for the legacy KornShell script `k_ausd_bp_ta_apn_vertrag.ksh`. The primary purpose of this script is to orchestrate a data processing job that prepares the execution environment, validates input parameters and dates, and then triggers an external SQL script, `d_ausd_bp_ta_apn_vertrag.sql`, for core data transformation. It also includes mechanisms for record counting and has commented-out sections for job management and file manipulation.

The scope of this migration is to re-platform this KornShell orchestration and its associated SQL logic to Google Cloud Platform, specifically utilizing BigQuery for data storage and transformation, with BigQuery Stored Procedures handling the orchestration and SQL execution.

## 2. Source Inventory

The job consists of the following primary source file and its immediate dependencies:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh`
    *   **Technology:** KornShell (Shell Script)
    *   **Complexity Tier:** Medium
    *   **Automation Bucket:** Semi-Automatic (B2)
    *   **Purpose:** Orchestration, parameter validation, date validation, and execution of an SQL script.
    *   **Description:** This script acts as a wrapper. It sources several common utility scripts for error handling, date functions, parameter parsing, and SQL*Plus execution. It parses command-line arguments for job identification, entry number, reference date (`Stichtag`), and a restart value. It then validates these parameters and the date format. Its main action is to invoke an external SQL script, `d_ausd_bp_ta_apn_vertrag.sql`, via a `starteSQLSkript` function (presumably from `h_alis_sqlplus.ksh`). After SQL execution, it captures the record count from a temporary file and has commented-out logic for job status updates.
*   **Referenced SQL Script:** `d_ausd_bp_ta_apn_vertrag.sql`
    *   **Technology:** SQL (likely Oracle PL/SQL given the context)
    *   **Purpose:** Core data processing logic.
    *   **Description:** This SQL script performs data transformations and movements. It `READS_TABLE` from `TABLE:DWTK_MELDUNGEN` and `TABLE:SOF$TA_BPR_APN` and `WRITES_TABLE` to `TABLE:SOF$TA_APN_VERTRAG`. It also utilizes `PACKAGE:DWPA_UTIL_SKRIPT` and `PACKAGE:PA_ANALYZE` which are likely database utility packages.
*   **Utility Scripts:**
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (Error handling)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (Date validation)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (Parameter parsing helpers)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (SQL execution wrapper)
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh` (Date calculation for yesterday/today)

## 3. Target Architecture

The migrated job will leverage BigQuery as the primary data warehouse and processing engine.

*   **BigQuery Tables:**
    *   `project.dataset.DWTK_MELDUNGEN` (source table, corresponding to legacy `TABLE:DWTK_MELDUNGEN`)
    *   `project.dataset.SOF_TA_BPR_APN` (source table, corresponding to legacy `TABLE:SOF$TA_BPR_APN`)
    *   `project.dataset.SOF_TA_APN_VERTRAG` (target table, corresponding to legacy `TABLE:SOF$TA_APN_VERTRAG`)
    *   `project.dataset.error_log` (for logging errors, replacing `DWMSG_MeldeFehler`)
    *   `project.dataset.job_tracking` (for job status updates, replacing commented FOSJob logic)
*   **BigQuery Stored Procedures:**
    *   `project.dataset.sp_k_ausd_bp_ta_apn_vertrag` (orchestration, replacing `k_ausd_bp_ta_apn_vertrag.ksh`)
    *   `project.dataset.sp_d_ausd_bp_ta_apn_vertrag` (core data logic, replacing `d_ausd_bp_ta_apn_vertrag.sql`)
*   **Orchestration (Optional, for complex multi-job flows):** Cloud Composer (Apache Airflow) can be used to schedule and orchestrate this BigQuery Stored Procedure, passing required parameters. For a single job, a scheduled query in BigQuery or Cloud Scheduler with a Cloud Function trigger might suffice.

## 4. Data Flow & Lineage

The data flow will be replicated within BigQuery using stored procedures.

1.  **Execution Trigger:** The BigQuery Stored Procedure `project.dataset.sp_k_ausd_bp_ta_apn_vertrag` is invoked, either manually, by a scheduled query, or via a Cloud Composer DAG.
2.  **Parameter & Date Validation:** Within `sp_k_ausd_bp_ta_apn_vertrag`, input parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`) are validated, and the `p_Stichtag`'s date format is checked. Errors are logged to `project.dataset.error_log`.
3.  **Date Derivation:** Current date, and yesterday's date will be derived using BigQuery's native date functions (e.g., `CURRENT_DATE()`, `DATE_SUB()`).
4.  **Core Data Transformation:** `sp_k_ausd_bp_ta_apn_vertrag` calls `project.dataset.sp_d_ausd_bp_ta_apn_vertrag`.
    *   `project.dataset.sp_d_ausd_bp_ta_apn_vertrag` will read data from `project.dataset.DWTK_MELDUNGEN` and `project.dataset.SOF_TA_BPR_APN`.
    *   It will perform the necessary transformations (as per the logic in the original `d_ausd_bp_ta_apn_vertrag.sql`).
    *   The transformed data will be written/inserted into `project.dataset.SOF_TA_APN_VERTRAG`.
5.  **Record Count & Job Tracking:** Upon successful completion of `sp_d_ausd_bp_ta_apn_vertrag`, `sp_k_ausd_bp_ta_apn_vertrag` will perform a `COUNT(*)` on the target table `project.dataset.SOF_TA_APN_VERTRAG` (scoped by the `Stichtag` or relevant key) to get the processed record count. This count, along with other job metadata, can then be inserted into `project.dataset.job_tracking` for auditing purposes.

## 5. Transformation Logic

**Orchestration Logic (`k_ausd_bp_ta_apn_vertrag.ksh` -> `sp_k_ausd_bp_ta_apn_vertrag`):**

*   **Environment Sourcing:** The sourcing of `.dw_init` and other utility ksh scripts will be replaced by direct BigQuery Stored Procedure logic. Global variables and configurations can be passed as stored procedure parameters or managed as BigQuery constants/configuration tables.
*   **Parameter Parsing:** The `getopts` logic will be translated into BigQuery Stored Procedure input parameters.
    *   `p_JobKennung` (STRING)
    *   `p_EintragsNr` (STRING)
    *   `p_Stichtag` (STRING, will be parsed to DATE)
    *   `p_wiederanlaufWert` (STRING, defaulting to '0' if empty)
*   **Parameter Validation:** `pruefeParameterGesetzt` will be translated to `IF` conditions checking for `NULL` or empty strings. Errors will be logged to `project.dataset.error_log` and the procedure will `LEAVE` (exit).
*   **Date Validation:** `DWDate_Datum_Check $p_Stichtag 'DDMMYYYY'` will be handled by `SAFE.PARSE_DATE('%d%m%Y', p_Stichtag)`. If `NULL`, an error is logged.
*   **Date Derivation (`gestern.ksh`):** Replaced by BigQuery SQL functions `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`.
*   **SQL Script Execution (`starteSQLSkript`):** The invocation of `d_ausd_bp_ta_apn_vertrag.sql` will be replaced by a `CALL` to the `project.dataset.sp_d_ausd_bp_ta_apn_vertrag` BigQuery Stored Procedure, passing relevant parameters.
*   **Record Count:** The `eval "v_records=\`cat $tmpFile\`"` will be replaced by a `SELECT COUNT(*)` query against the target table (`project.dataset.SOF_TA_APN_VERTRAG`) after the core SQL procedure completes.
*   **Job Tracking:** The commented `FOSJobErzeugeEintrag` logic will be implemented as an `INSERT` statement into `project.dataset.job_tracking` to record job status and metadata.
*   **File Manipulation (Commented `sed`, `sort`, `join`):** These commented sections suggest potential file-based intermediate processing. It will be assumed these are inactive and will not be migrated unless explicitly confirmed as required functionality. If active, they would require re-design as BigQuery SQL transformations or potentially external Python scripts if complex file processing is involved.

**Core Data Logic (`d_ausd_bp_ta_apn_vertrag.sql` -> `sp_d_ausd_bp_ta_apn_vertrag`):**

*   The SQL logic within `d_ausd_bp_ta_apn_vertrag.sql` will be directly translated into BigQuery SQL within the `project.dataset.sp_d_ausd_bp_ta_apn_vertrag` stored procedure.
*   Oracle-specific syntax (e.g., `TO_DATE`, `NVL`, `DECODE`, `ROWNUM`, specific package calls) will be converted to their BigQuery equivalents (e.g., `PARSE_DATE`, `COALESCE`, `CASE`, `ROW_NUMBER()`, UDFs or equivalent logic for packages).
*   The `USES_PACKAGE` references (`DWPA_UTIL_SKRIPT`, `PA_ANALYZE`) will require specific analysis. If they contain complex logic, they will be migrated as BigQuery UDFs or separate helper stored procedures. If they are simple utility functions, they can be directly translated into BigQuery SQL.

## 6. External Dependencies

The `lineage_assembled_jobs` record showed no explicit external systems (`external_systems: []`). However, based on the nature of the script and table names:

*   **Legacy Database (implied Oracle):** The tables `DWTK_MELDUNGEN`, `SOF$TA_BPR_APN`, `SOF$TA_APN_VERTRAG` are currently residing in a legacy database (likely Oracle, given the `SQL*Plus` context implied by `h_alis_sqlplus.ksh`).
    *   **Replacement:** These tables will be migrated to BigQuery as `project.dataset.DWTK_MELDUNGEN`, `project.dataset.SOF_TA_BPR_APN`, and `project.dataset.SOF_TA_APN_VERTRAG`. Data ingestion into these BigQuery tables from the legacy source will be handled by a separate data ingestion pipeline (e.g., Cloud Data Fusion, DataStream, custom batch loads, or other ETL tools).
*   **Local Filesystem (for temporary files):** The script uses a temporary file (`$DW_DIR_UTL/bert_k_ausd_bp_ta_apn_vertrag.tmp`) for record counting.
    *   **Replacement:** This will be replaced by BigQuery's in-memory variables or by storing audit information directly in a BigQuery table. No local filesystem dependency is required in BigQuery.
*   **Operating System Utilities (`getopts`, `print`, `cat`, `eval`, `set`, `sed`, `sort`, `join`):** These are shell-specific commands.
    *   **Replacement:** All parameter parsing, conditional logic, and simple I/O will be handled by BigQuery SQL scripting capabilities within stored procedures. Complex file processing if the commented `sed`/`sort`/`join` become active would be handled by either BigQuery SQL functions or by separate Cloud Functions/Dataflow jobs if file-based transformations are truly necessary.

## 7. Unresolved / Risks

*   **Detailed SQL Logic of `d_ausd_bp_ta_apn_vertrag.sql`:** The exact transformations and business logic within this SQL script are critical and need to be fully analyzed for accurate translation to BigQuery SQL. This document assumes direct translatability.
*   **Utility Scripts and Packages:** The full content and functionality of the sourced KornShell utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) and the referenced SQL packages (`DWPA_UTIL_SKRIPT`, `PA_ANALYZE`) need to be thoroughly understood. While some are clear (date, error), others (`h_alis_sqlplus.ksh`'s `starteSQLSkript`) might encapsulate complex logic that needs careful migration.
*   **Commented-out Code:** The commented FOS job management and file manipulation steps (`sed`, `sort`, `join`) are currently inactive. It's crucial to confirm with business users or subject matter experts whether these functionalities are truly defunct or represent dormant requirements that need to be re-introduced in the BigQuery environment. If active, they would increase complexity and potentially push parts of the solution towards Cloud Dataflow or custom Python scripts.
*   **Oracle-specific Features:** If the `d_ausd_bp_ta_apn_vertrag.sql` or related packages utilize advanced Oracle-specific features (e.g., proprietary PL/SQL constructs, specific data types, hints, complex indexing strategies), these might require more involved redesign in BigQuery.
*   **Data Ingestion Strategy:** The plan assumes the source data in `DWTK_MELDUNGEN` and `SOF$TA_BPR_APN` will be available in BigQuery. The mechanism for this initial and ongoing data ingestion needs to be defined as part of the broader migration plan.

## 8. Build Plan

The migration will involve building the following components in BigQuery:

1.  **BigQuery Table DDLs (Data Definition Language):**
    *   `project.dataset.DWTK_MELDUNGEN`
    *   `project.dataset.SOF_TA_BPR_APN`
    *   `project.dataset.SOF_TA_APN_VERTRAG`
    *   `project.dataset.error_log`
    *   `project.dataset.job_tracking`
    *   **(Language: BigQuery SQL)**
2.  **BigQuery Stored Procedure for Core Data Logic:**
    *   `project.dataset.sp_d_ausd_bp_ta_apn_vertrag` (translating the logic from `d_ausd_bp_ta_apn_vertrag.sql`)
    *   **(Language: BigQuery SQL)**
3.  **BigQuery Stored Procedure for Orchestration:**
    *   `project.dataset.sp_k_ausd_bp_ta_apn_vertrag` (translating the orchestration, validation, and job tracking logic from `k_ausd_bp_ta_apn_vertrag.ksh`)
    *   **(Language: BigQuery SQL)**
4.  **Deployment Scripts:**
    *   Scripts to deploy the BigQuery tables and stored procedures.
    *   **(Language: Shell script / Python with `bq` CLI or client libraries)**
5.  **Orchestration (if needed):**
    *   If part of a larger workflow, a Cloud Composer DAG to schedule and execute `project.dataset.sp_k_ausd_bp_ta_apn_vertrag`.
    *   **(Language: Python)**