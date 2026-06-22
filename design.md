# Migration Design — BERT_V_TA_DISC_ZUSGF

## 1. Purpose & Scope
This migration job, `BERT_V_TA_DISC_ZUSGF`, is responsible for the "Concatenation of discount descriptions" as described in the source UC4 job definition. It primarily processes contract and discount information to generate a consolidated view of discount descriptions, which is then stored in a target table. The original job is an ETL workflow orchestrated by UC4, executing KornShell scripts that, in turn, trigger an Oracle PL/SQL script for the core data transformation. The scope of this migration is to re-platform this entire workflow to Google Cloud Platform, utilizing BigQuery for data processing and Airflow for orchestration.

## 2. Source Inventory

| File Path                                                                                                   | Technology           | Summary                                                                                                                                                                                                                                                                                                                                                                                                                                                       | Target Technology / Tier (Inferred) | Automation Bucket (Inferred) |
| :---------------------------------------------------------------------------------------------------------- | :------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :---------------------------------- | :--------------------------- |
| `vobs/dw_source/isdwh/uc4_prod_exports/.../DW.BERT_AUSD_V_TA_DISC_ZUSGF.xml`                                | UC4/Automic          | This UC4 UNIX job orchestrates the concatenation of discount descriptions by executing a KornShell script.                                                                                                                                                                                                                                                                                                                                                      | Airflow DAG / Simple                | Auto (B1)                    |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh`                        | KornShell (Wrapper)  | This is a wrapper KornShell script (Rahmenskript) responsible for orchestrating the data reconciliation process for the 'ta_disc_zusgf' table. It handles parameter parsing, environment setup, logging, and error handling before calling a core processing script.                                                                                                                                                                                                 | BigQuery Stored Procedure / Simple  | Auto (B1)                    |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh`                        | KornShell (Control)  | Control script for r_ausd_vertrag.ksh that manages job execution, handles parameters, and orchestrates the execution of a SQL script (d_ausd_v_ta_disc_zusgf.sql) to update the ta_disc_zusgf table.                                                                                                                                                                                                                                                                | BigQuery Stored Procedure / Simple  | Auto (B1)                    |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_disc_zusgf.sql`                        | Oracle PL/SQL        | This Oracle SQL script defines custom object types and a PL/SQL package containing a pipelined table function. The function concatenates discount information, which is then used to populate the SOF$TA_DISC_ZUSGF table.                                                                                                                                                                                                                                            | BigQuery SQL / Medium               | Semi-Auto (B2)               |

_Note: Complexity tier and automation bucket are inferred due to a lack of data in `file_complexity` and `automation_rate` tables for this job ID._

## 3. Target Architecture
The target architecture will leverage Google Cloud Platform services:
*   **Orchestration:** Apache Airflow on Cloud Composer will replace the UC4 job scheduling.
*   **Data Processing:** Google BigQuery will host the transformed data and execute the core transformation logic via SQL stored procedures and native SQL statements.
*   **Script Execution:** The KornShell scripts will be re-implemented as BigQuery stored procedures to manage execution flow, parameter handling, and logging.
*   **Logging and Monitoring:** BigQuery audit tables will replace file-based logging, and Cloud Monitoring will track job execution.

### Target BigQuery Components & Layout
*   **Dataset:** `bert_dwh` (or similar, for `isbert_schema`)
*   **Tables:**
    *   `bert_dwh.dwtk_meldungen` (migrated from `isbert_schema.dwtk_meldungen`)
    *   `bert_dwh.sof_ta_discount` (migrated from `sof$ta_discount`)
    *   `bert_dwh.sof_ta_disc_zusgf` (target table, migrated from `sof$ta_disc_zusgf`)
    *   `bert_dwh.job_control` (new table for job metadata and status)
    *   `bert_dwh.job_log` (new table for logging messages)
    *   `bert_dwh.job_error_log` (new table for detailed error logging)
*   **Stored Procedures:**
    *   `bert_dwh.r_ausd_v_ta_disc_zusgf` (Python/BQ SP equivalent of `r_ausd_v_ta_disc_zusgf.ksh`)
    *   `bert_dwh.k_ausd_v_ta_disc_zusgf` (Python/BQ SP equivalent of `k_ausd_v_ta_disc_zusgf.ksh`)
    *   `bert_dwh.d_ausd_v_ta_disc_zusgf` (BQ SQL equivalent of `d_ausd_v_ta_disc_zusgf.sql` encapsulating the main transformation logic)
*   **Airflow DAG:** `dw_bert_ausd_v_ta_disc_zusgf.py`

## 4. Data Flow & Lineage

The data flow will be as follows:

1.  **Airflow DAG (`dw_bert_ausd_v_ta_disc_zusgf.py`)**:
    *   Initiated by a schedule or external trigger (replacing UC4).
    *   Task: `run_dw_bert_ausd_v_ta_disc_zusgf` (DataprocSubmitJobOperator or BigQueryOperator).
    *   This task will invoke the main Python script (or directly call the top-level BigQuery Stored Procedure).

2.  **Top-Level BigQuery Stored Procedure (`bert_dwh.r_ausd_v_ta_disc_zusgf`)**:
    *   Replaces `r_ausd_v_ta_disc_zusgf.ksh`.
    *   Handles job initialization, parameter parsing (`-j`, `-f`), and logging into `bert_dwh.job_control` and `bert_dwh.job_log`.
    *   Calls `bert_dwh.k_ausd_v_ta_disc_zusgf` with appropriate parameters.
    *   Manages error handling and updates job status in `bert_dwh.job_control`.

3.  **Control BigQuery Stored Procedure (`bert_dwh.k_ausd_v_ta_disc_zusgf`)**:
    *   Replaces `k_ausd_v_ta_disc_zusgf.ksh`.
    *   Receives `p_JobKennung` and `p_EintragsNr`.
    *   Performs parameter validation and updates job status (e.g., deactivating old active jobs) in `bert_dwh.job_control`.
    *   **Executes the core transformation logic** by calling `bert_dwh.d_ausd_v_ta_disc_zusgf`.
    *   Retrieves and records the number of processed records in `bert_dwh.job_control`.

4.  **Core Transformation BigQuery Stored Procedure/SQL (`bert_dwh.d_ausd_v_ta_disc_zusgf`)**:
    *   Replaces `d_ausd_v_ta_disc_zusgf.sql`.
    *   **Reads from:**
        *   `bert_dwh.dwtk_meldungen` (to determine the processing date `v_datum`).
        *   `bert_dwh.sof_ta_discount` (to get contract and discount details).
    *   **Transforms data:** Concatenates discount descriptions using `STRING_AGG` based on `cntrct_id` and `cntrct_obj_version`.
    *   **Writes to:** `bert_dwh.sof_ta_disc_zusgf`. The target table is truncated before insertion or created as `CREATE OR REPLACE TABLE`.

## 5. Transformation Logic

The core transformation logic resides in the `d_ausd_v_ta_disc_zusgf.sql` script. This script defines a PL/SQL package `sof$sp_discount_functions` with a pipelined table function `concat_discounts`. This function groups discount information (`rabatt` and `rabatthoehe`) by `cntrct_id` and `cntrct_obj_version`, concatenating them into a single `rabatt_alle` string.

**BigQuery Equivalent Logic:**

*   **Pipelined Table Function (`concat_discounts`)**: This complex Oracle construct will be replaced by a combination of standard SQL `GROUP BY` and `STRING_AGG` functionality within a BigQuery SQL query. The `hql_sql_to_bqsql_design` tool generated the following logic:
    ```sql
    WITH v_datum AS (...),\n    dzg AS (\n        SELECT DISTINCT cntrct_id, disc_vector_ty, cntrct_obj_version FROM `sof$ta_discount`\n    ),\n    discount_lines AS (\n        SELECT DISTINCT cntrct_id, cntrct_obj_version, CONCAT(CAST(rabatt AS STRING), \' (\', CAST(rabatthoehe AS STRING), \'%)\') AS rabatt_text FROM `sof$ta_discount`\n    ),\n    con AS (\n        SELECT cntrct_id, cntrct_obj_version, STRING_AGG(rabatt_text, \', \' ORDER BY rabatt_text) AS rabatt_alle FROM discount_lines GROUP BY cntrct_id, cntrct_obj_version\n    )\n    SELECT\n        dzg.cntrct_id,\n        dzg.cntrct_obj_version,\n        dzg.disc_vector_ty,\n        con.rabatt_alle\n    FROM dzg\n    LEFT JOIN con\n        ON dzg.cntrct_id = con.cntrct_id AND dzg.cntrct_obj_version = con.cntrct_obj_version;
    ```
*   **Variable `v_datum`**: Derived from `isbert_schema.dwtk_meldungen` using `MAX(m.timecreated)`. In BigQuery, `COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')` will be used.
*   **Oracle `TRUNCATE TABLE`**: The `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_disc_zusgf')` will be replaced by a BigQuery `TRUNCATE TABLE` statement before the `INSERT` or by using `CREATE OR REPLACE TABLE` in the main SQL.
*   **`ALTER SESSION` commands**: Oracle-specific session settings like `ALTER SESSION ENABLE PARALLEL DML` are not directly translatable or necessary in BigQuery, which automatically handles parallelism.
*   **Error Handling**: Oracle `WHENEVER SQLERROR CONTINUE/EXIT FAILURE` and `EXCEPTION` blocks will be replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks and explicit error logging to `bert_dwh.job_error_log`.
*   **SQL*Plus Commands**: `DEFINE`, `COLUMN`, `START`, `SPOOL`, `prompt`, `SET TIMING ON`, `SET SERVEROUTPUT ON`, `COMMIT` are SQL*Plus specific and will be removed or replaced by BigQuery native concepts (e.g., `DECLARE` for variables, logging to tables instead of spooling, BigQuery's auto-commit behavior).

## 6. External Dependencies

The `lineage_assembled_jobs` query initially reported no external systems. However, a deeper look at the `d_ausd_v_ta_disc_zusgf.sql` code reveals:

*   **`DB-Link auf CARMEN DB`**: The script contains `DEFINE v_carmen = "@pcrs1"`, indicating a potential database link to a CARMEN database (likely another Oracle instance). This suggests `sof$ta_discount` might reside in the CARMEN database.
    *   **Replacement:** This will require migrating the `CARMEN DB` data (specifically `sof$ta_discount`) to BigQuery. If the `CARMEN DB` is a separate system, data should be ingested into BigQuery using services like Cloud Data Transfer, Cloud Data Fusion, or custom ETL jobs, and `sof$ta_discount` would become a native BigQuery table (`bert_dwh.sof_ta_discount`).
*   **`isbert_schema.dwtk_meldungen`**: This table is used to determine the `v_datum`.
    *   **Replacement:** This table needs to be migrated to BigQuery as `bert_dwh.dwtk_meldungen`.

No other explicit external system dependencies (e.g., SFTP, S3) were identified in the provided analysis.

## 7. Unresolved / Risks

*   **Missing Lineage and Complexity Data**: The `lineage_edges`, `file_complexity`, and `automation_rate` tables did not return any rows for this job. This means the inferred complexity, automation bucket, and direct execution flow are based on manual code analysis and summaries rather than automated insights. This introduces a risk of underestimating effort or missing subtle dependencies.
*   **`DB-Link auf CARMEN DB`**: The exact location and nature of the `CARMEN DB` and the `sof$ta_discount` table are unclear from the available metadata. The migration assumes `sof$ta_discount` will be migrated to BigQuery. If `CARMEN DB` is a large, separate system, its migration might be a prerequisite or a significant project itself. Further investigation is required to confirm the source of `sof$ta_discount` and its migration plan.
*   **`BERT_DIR_ROOT` and utility scripts**: The KornShell scripts source several utility scripts (`. $HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`). The functionality of these scripts, particularly `starteSQLSkript` and `DWMSG_*` functions, has been inferred and translated to BigQuery stored procedure concepts (parameter validation, logging, job control updates). There is a risk that some complex logic in these utility scripts might be missed without a full analysis of their code.
*   **Parameter `p_EintragsNr`**: The specific purpose and generation mechanism of `p_EintragsNr` (entry number) in the original system are not fully detailed. The BigQuery stored procedure pseudocode assumes a sequential ID for `job_control` table. This should be validated.

## 8. Build Plan

The migration will proceed in the following order:

1.  **BigQuery Schema and Control Tables (DDL)**
    *   Create BigQuery dataset `bert_dwh`.
    *   Define DDL for `bert_dwh.job_control`, `bert_dwh.job_log`, and `bert_dwh.job_error_log`.
    *   Define DDL for `bert_dwh.dwtk_meldungen` and `bert_dwh.sof_ta_discount` based on source system schema.
    *   Define DDL for target table `bert_dwh.sof_ta_disc_zusgf`.
    *   **Language:** BigQuery DDL

2.  **Migrate Oracle Data to BigQuery**
    *   Ingest data from `isbert_schema.dwtk_meldungen` into `bert_dwh.dwtk_meldungen`.
    *   Ingest data from `sof$ta_discount` (from `CARMEN DB` or other source) into `bert_dwh.sof_ta_discount`.
    *   **Language:** Data Transfer/ETL tool configurations, e.g., Cloud Data Transfer, Dataflow, or custom Python scripts.

3.  **Core Transformation BigQuery Stored Procedure (`d_ausd_v_ta_disc_zusgf`)**
    *   Translate `d_ausd_v_ta_disc_zusgf.sql` into a BigQuery stored procedure (`bert_dwh.d_ausd_v_ta_disc_zusgf`) using the provided BigQuery SQL.
    *   **Language:** BigQuery SQL

4.  **Control BigQuery Stored Procedure (`k_ausd_v_ta_disc_zusgf`)**
    *   Translate `k_ausd_v_ta_disc_zusgf.ksh` into a BigQuery stored procedure (`bert_dwh.k_ausd_v_ta_disc_zusgf`) based on the generated BigQuery SQL pseudocode.
    *   **Language:** BigQuery SQL

5.  **Wrapper BigQuery Stored Procedure (`r_ausd_v_ta_disc_zusgf`)**
    *   Translate `r_ausd_v_ta_disc_zusgf.ksh` into a BigQuery stored procedure (`bert_dwh.r_ausd_v_ta_disc_zusgf`) based on the generated BigQuery SQL pseudocode.
    *   **Language:** BigQuery SQL

6.  **Airflow DAG (`dw_bert_ausd_v_ta_disc_zusgf.py`)**
    *   Create an Airflow DAG (`dw_bert_ausd_v_ta_disc_zusgf.py`) that invokes the `bert_dwh.r_ausd_v_ta_disc_zusgf` stored procedure.
    *   **Language:** Python (Airflow DAG)