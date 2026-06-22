# Migration Design — DW.BERT_AUSD_V_TA_C_BFC

## 1. Purpose & Scope

The job `DW.BERT_AUSD_V_TA_C_BFC` is an ETL workflow primarily responsible for updating the "contract extension period caching" within a data warehouse. Its core function is to calculate and maintain binding dates for contracts in the `sof$ta_c_bfc` table. The process involves aggregating contract-related data from various source tables, applying complex business logic (including a PL/SQL function call over a database link), and incrementally updating the target cache table.

The scope of this migration is to re-platform this ETL job from its current UC4, KornShell, and Oracle SQL environment to Google Cloud Platform (GCP), specifically using Airflow for orchestration and BigQuery for data transformation and storage.

## 2. Source Inventory

The ETL job comprises a sequence of interconnected components:

| File Name | Technology | Category | Complexity Tier | Automation Bucket | Summary |
| :----------------------------------------------------------------------------------------------------------------------- | :--------- | :------ | :-------------- | :---------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_C_BFC.xml` | UC4/Automic | uc4 | medium | semi_auto | UC4 UNIX job definition for updating contract extension period caching. It orchestrates the execution of a KornShell script. |
| `SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh` | KornShell | shell | semi_auto | semi_auto | Rahmenskript fuer den Update des Bindefristcachs: Tabelle ta_c_bfc (Wrapper script for the core control script) |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh` | KornShell | shell | medium | semi_auto | Control script that orchestrates the execution of a SQL script (d_ausd_v_ta_c_bfc.sql) for data processing. It handles parameter validation, job status checks, and error reporting. |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_c_bfc.sql` | Oracle SQL | sql | medium | semi_auto | This SQL script defines synonyms and a function to calculate contract binding dates, then uses these to populate and incrementally update a binding date cache table (SOF$TA_C_BFC) from various contract-related source tables. |

All components are classified as `medium` complexity and fall into the `semi_auto` migration bucket, indicating that some manual intervention or review will be required during the migration process.

## 3. Target Architecture

The target architecture for this job will leverage Google Cloud Platform (GCP) services:

*   **Orchestration**: Apache Airflow (Cloud Composer) will replace UC4. A single DAG will manage the execution flow.
*   **Execution Environment**: Google Cloud Dataproc will be used to execute Python scripts that wrap the BigQuery SQL transformations.
*   **Data Transformation & Storage**: Google BigQuery will replace Oracle SQL for data processing and Oracle tables for data storage.
    *   Source tables (`isbert_schema.dwtk_meldungen`, `sof$ta_cntrct_crs`, `sof$ta_barrier`, `sof$ta_cntrct_valid`, `sof$ta_period`) will be migrated to BigQuery tables.
    *   The temporary table `sof$ta_c_bfc_akt` will be a BigQuery temporary table or a Common Table Expression (CTE) within the main BigQuery SQL.
    *   The target table `sof$ta_c_bfc` will be a BigQuery table.
    *   The Oracle PL/SQL function `Cds$vr_Bindefrist.GetBindeFrist` will require re-implementation in BigQuery, likely as a BigQuery User-Defined Function (UDF) or a BigQuery Stored Procedure, depending on its complexity.

## 4. Data Flow & Lineage

The current and target data flow are as follows:

**Current Data Flow:**

1.  **UC4 Job (`DW.BERT_AUSD_V_TA_C_BFC.xml`)**: Scheduled to run on `DWHDWH1P` host, using `DW.UNIX.ISBERT` login. It executes the wrapper script.
2.  **Wrapper KornShell Script (`r_ausd_v_ta_c_bfc.ksh`)**: Initializes environment, sets up error handling and logging, and then calls the control script.
3.  **Control KornShell Script (`k_ausd_v_ta_c_bfc.ksh`)**: Validates parameters, manages job status, and ultimately invokes the Oracle SQL script via `sqlplus`.
4.  **Oracle SQL Script (`d_ausd_v_ta_c_bfc.sql`)**:
    *   Reads `MAX(timecreated)` from `isbert_schema.dwtk_meldungen` and `created` date of `CDS$VR_BINDEFRIST` package from `all_objects` over `@PCRS1` DB link.
    *   Truncates and populates `sof$ta_c_bfc_akt` from `sof$ta_cntrct_crs`, `sof$ta_barrier`, `sof$ta_cntrct_valid`, and `sof$ta_period` tables using `LEFT JOIN`s and aggregation.
    *   Performs an initial `INSERT` into `sof$ta_c_bfc` if it's empty.
    *   Executes a `MERGE` statement to incrementally update `sof$ta_c_bfc` based on changes in `bfc_age` or `bfc_count` from `sof$ta_c_bfc_akt`, calling the `bfc_get_bindefrist` function (which in turn calls `Cds$vr_Bindefrist.GetBindeFrist` over `@PCRS1`).
    *   Updates remaining records in `sof$ta_c_bfc` that have an outdated `bfc_procedure` using the `bfc_get_bindefrist` function.
    *   Truncates the temporary table `sof$ta_c_bfc_akt`.

**Target Data Flow:**

1.  **Airflow DAG (`dw_bert_ausd_v_ta_c_bfc`)**: The UC4 job will be replaced by an Airflow DAG. The schedule, if determined (not available from current input), will be configured in the DAG.
2.  **DataprocSubmitJobOperator**: A single task in the Airflow DAG will execute a PySpark job (or a Python script) on a Dataproc cluster.
3.  **Python Script (`r_ausd_v_ta_c_bfc.py`)**: This script will encapsulate the logic of the original KornShell wrapper and control scripts. It will handle environment setup, logging, parameter passing, and coordinate the execution of the BigQuery SQL.
4.  **BigQuery SQL (derived from `d_ausd_v_ta_c_bfc.sql`)**: The core transformation logic will be converted to BigQuery SQL and executed by the Python script against BigQuery.
    *   All Oracle tables (`isbert_schema.dwtk_meldungen`, `sof$ta_cntrct_crs`, `sof$ta_barrier`, `sof$ta_cntrct_valid`, `sof$ta_period`, `sof$ta_c_bfc`) will be BigQuery tables.
    *   The `bfc_get_bindefrist` function's logic will be re-implemented as a BigQuery UDF or Stored Procedure.
    *   The temporary table `sof$ta_c_bfc_akt` will be a BigQuery temporary table or CTE.
    *   The BigQuery SQL will perform the `TRUNCATE`, `INSERT`, `MERGE`, and `UPDATE` operations as defined in the Oracle SQL, adapted for BigQuery syntax and functionality.

**Execution Order:**
`Airflow_start_task` >> `Dataproc_PySpark_task` >> `Airflow_end_task`

The `Dataproc_PySpark_task` internally manages the sequence of BigQuery operations.

## 5. Transformation Logic

The core transformation logic resides in the `d_ausd_v_ta_c_bfc.sql` script. The migration involves converting this Oracle SQL to BigQuery SQL.

**Key Conversion Aspects:**

*   **Variables**: Oracle `DEFINE` and `COLUMN ... NEW_VALUE` will be replaced by BigQuery `DECLARE` statements.
*   **Date Functions**:
    *   `NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101')` -> `IFNULL(FORMAT_DATE('%Y%m%d', DATE(MAX(timecreated))), '19000101')`
    *   `TO_DATE('19000101','YYYYMMDD')` -> `DATE '1900-01-01'`
    *   `TO_CHAR(created, 'YYYYMMDD')` -> `FORMAT_DATE('%Y%m%d', DATE(created))`
    *   `TRUNC(o_vbinde - 1 + 1/86400)` -> `DATE(o_vbinde - 1)` (adjustment might be needed based on precise Oracle `TRUNC` behavior with time components).
*   **Join Syntax**: Oracle outer join `(+)` will be converted to `LEFT JOIN`.
*   **DML Statements**: `INSERT`, `MERGE`, `UPDATE`, `TRUNCATE TABLE` constructs are largely compatible but will require syntax adjustments (e.g., table prefixes with backticks, schema definitions).
*   **Performance Hints**: Oracle hints like `/*+ append */` and `/*+ full(c) parallel(c,4) */` will be removed, as BigQuery's query optimizer handles performance automatically.
*   **Oracle-specific Commands**: `PROMPT`, `START`, `SPOOL`, `WHENEVER SQLERROR`, `SET TIMING/SERVEROUTPUT` will be removed as they are not applicable in BigQuery.
*   **PL/SQL Block (`DECLARE...BEGIN...END`)**: Oracle PL/SQL blocks will be translated into BigQuery Scripting or separated into BigQuery Stored Procedures if complex. The initial `IF v_rows = 0 THEN INSERT... END IF;` block will be converted using BigQuery Scripting features.
*   **ROWNUM**: Oracle `ROWNUM <= &v_max_update` will be converted to `LIMIT v_max_update` in BigQuery for the `UPDATE` statement.
*   **Core Logic - `bfc_get_bindefrist`**: This function, which internally calls `Cds$vr_Bindefrist.GetBindeFrist` (a PL/SQL package function), needs to be reimplemented in BigQuery. This could be a BigQuery SQL UDF, a JavaScript UDF, or a BigQuery Stored Procedure, depending on the complexity of the original PL/SQL logic. The function's input parameters (`i_cntrct_id`, `i_commitment_reference_date`, `i_cntrct_validity_id`) and return type (`DATE`) must be maintained.

## 6. External Dependencies

The primary external dependency identified is the Oracle database link `@PCRS1`.

*   **Current State**: The Oracle SQL script uses `@PCRS1` to access `all_objects` (to get procedure creation date) and to call the `spr_schema.Cds$vr_Bindefrist.GetBindeFrist` package function.
*   **Target State**:
    *   **Data Access (`all_objects`)**: If `all_objects` is only used to determine a creation date, this metadata might be static or sourced differently in GCP. If `all_objects` represents actual data, then the data from this source system (`PCRS1`) needs to be ingested into BigQuery.
    *   **PL/SQL Function (`Cds$vr_Bindefrist.GetBindeFrist`)**: This function is critical for calculating `bindefrist`. Since it's a PL/SQL package function on an external Oracle system, its logic must be extracted and re-implemented in BigQuery. This will involve:
        1.  Understanding the business logic of `Cds$vr_Bindefrist.GetBindeFrist` by analyzing its source code in the original Oracle environment.
        2.  Re-implementing this logic as a BigQuery SQL UDF, JavaScript UDF, or a BigQuery Stored Procedure. If the logic is highly complex or dependent on specific Oracle features, a Cloud Function or a service like Cloud Run might be needed to host this logic, called by the BigQuery environment.

## 7. Unresolved / Risks

*   **Missing PL/SQL Source Code**: The exact business logic of `spr_schema.Cds$vr_Bindefrist.GetBindeFrist` is not available from the provided artifacts. This is a significant risk, as its re-implementation is crucial for the job's functionality. It is assumed the logic can be extracted and converted.
*   **Source Data Location**: The `sof$ta_*` tables are assumed to be Oracle tables. Their migration to BigQuery (including schema definition, data loading, and ongoing synchronization) is a prerequisite for this job's migration.
*   **`v_carmen` definition**: The variable `v_carmen` is defined as `\"@pcrs1\"` but then seems to be implicitly used in the `SELECT ... FROM all_objects &v_carmen` which is an Oracle SQL*Plus substitution variable. This usage and the implications for `all_objects` source needs careful review.
*   **`DWPA_UTIL_SKRIPT.runstatement`**: The PL/SQL block calls `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_c_bfc_akt')`. The `DWPA_UTIL_SKRIPT` package and `runstatement` procedure's exact functionality (beyond just executing DDL) need to be understood. This might be a custom utility that needs to be replicated or replaced with standard BigQuery operations.
*   **Error Handling**: The KornShell scripts include custom error handling (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `DWMSG_*` functions). This will need to be re-implemented using Python's error handling and integrated with Airflow's logging and alerting mechanisms.
*   **Scheduler Information**: The UC4 job's actual schedule is not available. This needs to be determined from other sources (e.g., UC4 calendar objects, business requirements) to correctly configure the Airflow DAG schedule.

## 8. Build Plan

The migration will involve generating the following artifacts:

1.  **BigQuery DDL for `sof$ta_c_bfc`**: Create the target table in BigQuery, defining columns and data types based on the Oracle schema.
2.  **BigQuery DDL for `sof$ta_c_bfc_akt`**: Create the temporary table in BigQuery. This could also be implicitly handled by the BigQuery SQL if used as a CTE or managed table.
3.  **BigQuery UDF/Stored Procedure for `bfc_get_bindefrist`**: Implement the logic of the `Cds$vr_Bindefrist.GetBindeFrist` function in BigQuery.
    *   **Language**: BigQuery SQL or JavaScript (for UDFs), BigQuery SQL (for Stored Procedures).
4.  **BigQuery SQL Script (`d_ausd_v_ta_c_bfc.bqsql`)**: The converted core transformation logic, incorporating the BigQuery UDF/Stored Procedure call.
    *   **Language**: BigQuery SQL.
5.  **Python Wrapper Script (`r_ausd_v_ta_c_bfc.py`)**: A Python script to replace `r_ausd_v_ta_c_bfc.ksh` and `k_ausd_v_ta_c_bfc.ksh`. This script will:
    *   Handle parameter parsing (`p_JobKennung`, `p_EintragsNr`).
    *   Manage logging and error handling, potentially integrating with Cloud Logging.
    *   Execute the BigQuery SQL script using the BigQuery Python client library.
    *   **Language**: Python.
6.  **Airflow DAG (`dw_bert_ausd_v_ta_c_bfc.py`)**: An Airflow DAG to orchestrate the entire process.
    *   Define a `DataprocSubmitJobOperator` task to run `r_ausd_v_ta_c_bfc.py` on a Dataproc cluster.
    *   Configure GCP project ID, region, cluster name, and GCS bucket for scripts.
    *   Define task dependencies.
    *   **Language**: Python.

**Ordered Build Steps:**

1.  Migrate source tables (`isbert_schema.dwtk_meldungen`, `sof$ta_cntrct_crs`, `sof$ta_barrier`, `sof$ta_cntrct_valid`, `sof$ta_period`) to BigQuery, ensuring data is available and synchronized.
2.  Create BigQuery DDL for `sof$ta_c_bfc` and `sof$ta_c_bfc_akt`.
3.  Develop and deploy the `bfc_get_bindefrist` BigQuery UDF or Stored Procedure.
4.  Convert `d_ausd_v_ta_c_bfc.sql` into BigQuery SQL (`d_ausd_v_ta_c_bfc.bqsql`).
5.  Develop the Python wrapper script (`r_ausd_v_ta_c_bfc.py`) to execute the BigQuery SQL.
6.  Create the Airflow DAG (`dw_bert_ausd_v_ta_c_bfc.py`) to orchestrate the Python script execution via Dataproc.
7.  Deploy the Airflow DAG to Cloud Composer.