# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh

## 1. Purpose & Scope
This document outlines the migration design for the legacy KornShell script `k_ausd_bp_ta_bcp_msisdn.ksh` and its associated SQL script `d_ausd_bp_ta_bcp_msisdn.sql` to Google Cloud BigQuery.

The primary purpose of this job, as indicated by its summary, is "data preparation, handling parameter parsing, date validation, error handling, and orchestrating the execution of an SQL script for data processing." Specifically, the SQL script `d_ausd_bp_ta_bcp_msisdn.sql` reads data from source tables `DWTK_MELDUNGEN` and `SOF$TA_BPR_BCP`, processes it, and writes the results into the target table `SOF$TA_BCP_MSISDN`. The KornShell script acts as an orchestrator, managing execution flow, parameter handling, and logging.

The job is categorized as 'shell' (KornShell) and is considered of 'medium' complexity with a 'semi_auto' migration automation bucket. This suggests that while a significant portion of the migration might be automated, some manual intervention and redesign will be required, particularly for the shell script's orchestration logic and external utility calls.

## 2. Source Inventory
The job is comprised of a single primary source file, a KornShell script, which in turn invokes an SQL script.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh`
    *   **Technology:** KornShell script
    *   **Complexity Tier:** Medium
    *   **Automation Bucket:** Semi-automatic
    *   **Summary:** Control script for data preparation, parameter parsing, date validation, error handling, and SQL script orchestration.
    *   **Invokes:** `d_ausd_bp_ta_bcp_msisdn.sql` (an Oracle SQL*Plus script)

## 3. Target Architecture
The migrated solution will primarily leverage Google Cloud BigQuery for data storage and processing, and potentially Cloud Composer (Apache Airflow) for workflow orchestration.

*   **Data Storage & Processing:**
    *   **Source Tables (`DWTK_MELDUNGEN`, `SOF$TA_BPR_BCP`):** These tables will be migrated to BigQuery as standard BigQuery tables, maintaining their schema and data.
    *   **Target Table (`SOF$TA_BCP_MSISDN`):** This table will also be migrated to BigQuery. The existing DDL for this table will be translated to BigQuery DDL.
    *   **SQL Logic (`d_ausd_bp_ta_bcp_msisdn.sql`):** The Oracle SQL logic will be translated into BigQuery Standard SQL.
*   **Orchestration:**
    *   The KornShell script's orchestration logic (parameter parsing, date checks, SQL execution) will be converted into an Apache Airflow DAG in Cloud Composer.
    *   Utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) will need to be re-implemented as Python functions or BigQuery SQL UDFs/procedures within the Airflow DAG, or replaced with native Airflow operators/functions.

## 4. Data Flow & Lineage
The original data flow involves a KornShell script orchestrating an SQL script to process data between Oracle tables.

**Legacy Data Flow:**
1.  **External Invoker:** An upstream script (`r_ausd_bp_ta_bcp_msisdn.ksh`) invokes `k_ausd_bp_ta_bcp_msisdn.ksh`.
2.  **KornShell Script (`k_ausd_bp_ta_bcp_msisdn.ksh`):**
    *   Initializes environment variables (`. $HOME/.dw_init`).
    *   Sources utility scripts for error handling, date validation, parameter parsing, and SQL*Plus routines.
    *   Parses input parameters (`j`, `f`, `s`, `l` corresponding to JobKennung, EintragsNr, Stichtag, wiederanlaufWert).
    *   Validates the date parameter (`p_Stichtag`).
    *   Determines `p_datum_heute` and `p_datum_gestern` using `gestern.ksh`.
    *   Calls a function `starteSQLSkript` to execute `d_ausd_bp_ta_bcp_msisdn.sql`.
    *   Captures record count from a temporary file (`tmpFile`).
    *   Potentially interacts with a job management system (commented out FOS calls).
3.  **SQL Script (`d_ausd_bp_ta_bcp_msisdn.sql`):**
    *   Reads data from `TABLE:DWTK_MELDUNGEN`.
    *   Reads data from `TABLE:SOF$TA_BPR_BCP`.
    *   Uses `PACKAGE:DWPA_UTIL_SKRIPT` (an Oracle package).
    *   Writes processed data to `TABLE:SOF$TA_BCP_MSISDN`.

**Target BigQuery Data Flow:**
1.  **Airflow Trigger/Scheduler:** The Airflow DAG corresponding to `k_ausd_bp_ta_bcp_msisdn.ksh` will be scheduled or triggered by an upstream process (e.g., another Airflow DAG replacing `r_ausd_bp_ta_bcp_msisdn.ksh`).
2.  **Airflow DAG (`k_ausd_bp_ta_bcp_msisdn_dag`):**
    *   **Parameter Handling Task:** Airflow operators will handle parameter parsing (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
    *   **Date Validation Task:** A Python operator will perform date validation similar to `DWDate_Datum_Check`.
    *   **Date Calculation Task:** A Python operator will calculate `p_datum_heute` and `p_datum_gestern`.
    *   **BigQuery SQL Execution Task:** A `BigQueryOperator` or a custom Python operator will execute the translated BigQuery SQL.
        *   This SQL will read from `bigquery_project.dataset.DWTK_MELDUNGEN`.
        *   This SQL will read from `bigquery_project.dataset.SOF_TA_BPR_BCP`.
        *   It will incorporate logic from `DWPA_UTIL_SKRIPT` (either as BigQuery UDFs/stored procedures or in-line).
        *   It will write the results to `bigquery_project.dataset.SOF_TA_BCP_MSISDN`.
    *   **Record Count & Logging Tasks:** Airflow tasks will capture record counts and log job status, replacing the `tmpFile` and `FOSJobErzeugeEintrag` logic.

## 5. Transformation Logic

### 5.1 `k_ausd_bp_ta_bcp_msisdn.ksh` (KornShell) to Python Airflow DAG
The KornShell script's control flow and utility calls will be translated into a Python Airflow DAG:

*   **Environment Initialization (`. $HOME/.dw_init`):** Environment variables will be managed by Airflow's environment or XComs.
*   **Error Handling (`f_alis_msgerr.ksh`):** Replaced by Airflow's native error handling, alerting, and logging mechanisms.
*   **Parameter Parsing (`h_alis_parameter.ksh`):** Airflow DAG parameters or configuration will replace `getopts` logic.
*   **Date Handling (`h_alis_date.ksh`, `gestern.ksh`):** Python functions using `datetime` module will replace these.
*   **SQL Execution (`h_alis_sqlplus.ksh`, `starteSQLSkript`):** Replaced by `BigQueryOperator` or a Python operator executing BigQuery client code. The parameters passed to `starteSQLSkript` will become parameters for the BigQuery query.
*   **Job Management (`FOSJobDeaktivate`, `FOSJobErzeugeEintrag`):** If these refer to an external job scheduler, they need to be replaced with equivalent Airflow/Cloud Composer mechanisms, or integrated with Google Cloud services like Cloud Logging or Cloud Monitoring.

### 5.2 `d_ausd_bp_ta_bcp_msisdn.sql` (Oracle SQL) to BigQuery Standard SQL
The SQL script will be directly translated to BigQuery Standard SQL.

*   **Schema Translation:** Oracle schema names (e.g., `DWTK`, `SOF$`) will be mapped to BigQuery datasets. Table names will be preserved or adjusted for BigQuery naming conventions (e.g., `SOF_TA_BPR_BCP`).
*   **SQL Dialect:** All Oracle-specific SQL functions, syntax, and data types will be converted to their BigQuery equivalents. This includes:
    *   `DUAL` table references (if any) will be removed.
    *   Date/time functions (e.g., `SYSDATE`, `TO_DATE`, `TO_CHAR`) will be mapped to BigQuery's `CURRENT_DATE()`, `PARSE_DATE()`, `FORMAT_DATE()`, etc.
    *   `NVL`, `DECODE`, etc., will be translated to `COALESCE`, `CASE` statements.
    *   Table joins, subqueries, and DML statements (`INSERT`, `UPDATE`, `DELETE`) will be adapted to BigQuery's syntax.
*   **Package `DWPA_UTIL_SKRIPT`:** The functionality within this Oracle package needs to be analyzed. It will be migrated either as:
    *   **BigQuery Stored Procedures/UDFs:** If the logic is pure SQL and reusable.
    *   **In-line SQL:** If the logic is simple and specific to this script.
    *   **Python functions:** If the logic involves complex procedural aspects, it might be implemented in Python within the Airflow DAG that prepares the SQL.

## 6. External Dependencies

*   **Oracle Database:** The source database hosting `DWTK_MELDUNGEN`, `SOF$TA_BPR_BCP`, `SOF$TA_BCP_MSISDN`, and `DWPA_UTIL_SKRIPT` will be replaced by BigQuery. Data will be ingested into BigQuery prior to or as part of the migration.
*   **Filesystem Utilities (`sed`, `sort`, `join`):** The commented-out sections in the KornShell script suggest prior or potential use of file-based processing. If these functionalities are reactivated, they would need to be replaced by BigQuery transformations or Python processing within the Airflow DAG using Google Cloud Storage for intermediate files if necessary.
*   **Job Management System (FOS):** The commented-out FOS job management calls (`FOSJobDeaktivate`, `FOSJobErzeugeEintrag`) indicate integration with an external scheduler. This will be replaced by Cloud Composer/Airflow's native scheduling and job monitoring capabilities.
*   **KornShell Utility Scripts:** (`f_alis_msgerr.ksh`, `h_alis_job.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) These will be re-implemented as Python modules or Airflow tasks.

## 7. Unresolved / Risks

*   **`DWPA_UTIL_SKRIPT` Package:** The exact functionality of `PACKAGE:DWPA_UTIL_SKRIPT` is unknown without inspecting its source code. This needs to be thoroughly analyzed to ensure accurate translation to BigQuery. This is a primary risk.
*   **Kommented-out Code:** The script contains commented-out sections involving file manipulations (`sed`, `sort`, `join`). It needs to be confirmed whether these functionalities are truly defunct or might be reactivated, which would introduce additional migration complexity (e.g., to Dataflow or PySpark).
*   **`FOSJob*` Calls:** While commented out, the `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` calls indicate an interaction with a job management system. The full scope of this interaction needs to be understood. If active, it would require integration with GCP services (e.g., Cloud Logging, Cloud Monitoring, or custom Airflow sensors/operators).
*   **`BERT_DIR_ROOT` and `DW_DIR_UTL`:** The exact values and usage of these environment variables need to be understood to correctly map file paths and resource locations in the target environment.
*   **`r_ausd_bp_ta_bcp_msisdn.ksh` Invoker:** This job is invoked by `r_ausd_bp_ta_bcp_msisdn.ksh`. The migration of this parent job needs to be considered to ensure a seamless end-to-end workflow in the target environment.

## 8. Build Plan

The build plan will involve a phased approach, starting with schema migration, then SQL logic translation, and finally orchestration.

1.  **Schema Migration:**
    *   **Language:** DDL (BigQuery Standard SQL)
    *   **Files:**
        *   `ddl/DWTK_MELDUNGEN.sql` (CREATE TABLE DDL for BigQuery)
        *   `ddl/SOF_TA_BPR_BCP.sql` (CREATE TABLE DDL for BigQuery)
        *   `ddl/SOF_TA_BCP_MSISDN.sql` (CREATE TABLE DDL for BigQuery)
    *   **Action:** Create BigQuery tables.
2.  **Data Ingestion:**
    *   **Language:** Dataflow/gsutil/BigQuery Load
    *   **Action:** Ingest historical and incremental data for `DWTK_MELDUNGEN` and `SOF$TA_BPR_BCP` into BigQuery.
3.  **SQL Transformation Logic Migration:**
    *   **Language:** BigQuery Standard SQL
    *   **File:** `sql/d_ausd_bp_ta_bcp_msisdn_bq.sql`
    *   **Action:** Translate `d_ausd_bp_ta_bcp_msisdn.sql` to BigQuery Standard SQL, incorporating logic from `DWPA_UTIL_SKRIPT` (as UDFs/Procedures or in-line).
4.  **KornShell Orchestration to Airflow DAG:**
    *   **Language:** Python
    *   **File:** `dags/k_ausd_bp_ta_bcp_msisdn_dag.py`
    *   **Action:**
        *   Create an Airflow DAG that handles parameter parsing, date validation, date calculation, and BigQuery SQL execution.
        *   Implement Python functions to replace `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, and `gestern.ksh`.
        *   Integrate `BigQueryOperator` to run `sql/d_ausd_bp_ta_bcp_msisdn_bq.sql`.
        *   Implement Airflow tasks for capturing record counts and logging.
5.  **Utility Script Replacements (if not in-lined in DAG):**
    *   **Language:** Python
    *   **Files:**
        *   `utils/error_handling.py`
        *   `utils/date_helpers.py`
        *   `utils/parameter_parser.py`
    *   **Action:** Create separate Python modules for reusable utility functions.
6.  **Integration with Upstream Processes:**
    *   **Language:** Python (Airflow DAG)
    *   **Action:** Modify the Airflow DAG that replaces `r_ausd_bp_ta_bcp_msisdn.ksh` to invoke `k_ausd_bp_ta_bcp_msisdn_dag.py`.