# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh

## 1. Purpose & Scope

This document outlines the migration design for the legacy KornShell script `k_ausd_v_ta_notice.ksh` to Google Cloud BigQuery.

The primary purpose of `k_ausd_v_ta_notice.ksh` is to act as a control script for a broader job (`r_ausd_vertrag.ksh`), orchestrating the execution of an SQL script (`d_ausd_v_ta_notice.sql`). Its key functions include:
*   Parsing input parameters (`JobKennung`, `EintragsNr`).
*   Validating these parameters and handling errors.
*   Integrating with a legacy error handling framework.
*   Executing a core SQL script to perform data operations.
*   Implicitly managing job states (ignoring active jobs, deactivating old active jobs).
*   Retrieving a record count after SQL execution.

The scope of this migration covers transforming the shell script's orchestration logic and its invoked SQL script's data manipulation into BigQuery-native components, primarily BigQuery Stored Procedures and SQL.

## 2. Source Inventory

| File Path                                                       | Technology  | Type          | Tier   | Automation Bucket |
| :-------------------------------------------------------------- | :---------- | :------------ | :----- | :---------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh` | KornShell   | Control Script| `medium` | `semi_auto`       |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_notice.sql` | SQL (Oracle) | Data Script   | (Derived: likely `medium` or `complex` depending on content) | (Derived: likely `semi_auto`) |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh` | KornShell   | Utility       | (Unknown) | (Unknown) |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh` | KornShell   | Utility       | (Unknown) | (Unknown) |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh` | KornShell   | Utility       | (Unknown) | (Unknown) |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh` | KornShell   | Utility       | (Unknown) | (Unknown) |

**Notes:**
*   The `k_ausd_v_ta_notice.ksh` script sources several other utility ksh scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`). These contain reusable functions used by the main script.
*   The main data transformation logic resides within `d_ausd_v_ta_notice.sql`.

## 3. Target Architecture

The target architecture in BigQuery will consist of:

*   **BigQuery Stored Procedure:** A BigQuery Stored Procedure will encapsulate the orchestration logic currently performed by `k_ausd_v_ta_notice.ksh`. This procedure will:
    *   Accept input parameters (`p_JobKennung`, `p_EintragsNr`) directly.
    *   Implement parameter validation and error handling using BigQuery Scripting.
    *   Execute the migrated SQL logic from `d_ausd_v_ta_notice.sql`.
    *   Manage job states (e.g., updating a job control table).
    *   Return a record count.
*   **BigQuery SQL:** The data manipulation logic from `d_ausd_v_ta_notice.sql` will be converted into native BigQuery SQL. This could be inlined within the Stored Procedure or called via `EXECUTE IMMEDIATE` if dynamic SQL is required.
*   **Job Control Table:** A dedicated BigQuery table (e.g., `dataset.job_table`) will be used to track job execution, status (ACTIVE, DEACTIVATED, COMPLETED), and parameters, replacing the file-based mechanisms for job state management.
*   **Error Logging Table:** A BigQuery table (e.g., `dataset.error_log`) will capture error messages and details, replacing the `f_alis_msgerr.ksh` script's functionality.
*   **Target Tables:**
    *   `dataset.SOF_TA_NOTICE`: This will be the migrated target for `SOF$TA_NOTICE`.
    *   `dataset.VIA`: This will be the migrated target for `VIA`.
*   **Source Tables:**
    *   `dataset.DWTK_MELDUNGEN`: Migrated source table.
    *   `dataset.CDS_TA_NOTICE`: Migrated source table.
*   **Orchestration:** Cloud Composer (Apache Airflow) can be used to schedule and trigger the BigQuery Stored Procedure, replacing the upstream shell script (`r_ausd_v_ta_notice.ksh`) that currently invokes `k_ausd_v_ta_notice.ksh`.

## 4. Data Flow & Lineage

The migrated job will follow this data flow:

1.  **Trigger:** An external orchestrator (e.g., Cloud Composer DAG) invokes the BigQuery Stored Procedure `dataset.r_ausd_vertrag_control` with `p_JobKennung` and `p_EintragsNr` parameters.
2.  **Parameter Validation:** Inside the Stored Procedure, `p_JobKennung` and `p_EintragsNr` are validated.
3.  **Job State Management:**
    *   The `dataset.job_table` is updated to register the new job entry as 'ACTIVE'.
    *   Any existing 'ACTIVE' jobs with the same table name (`ta_notice`) but different job ID are 'DEACTIVATED' in `dataset.job_table`.
4.  **SQL Execution:** The core SQL logic (migrated from `d_ausd_v_ta_notice.sql`) is executed.
    *   This logic `READS` data from `dataset.DWTK_MELDUNGEN` and `dataset.CDS_TA_NOTICE`.
    *   It `WRITES` transformed data into `dataset.SOF_TA_NOTICE` and `dataset.VIA`.
5.  **Record Count:** After successful SQL execution, the procedure calculates the number of records processed (e.g., `COUNT(*)` on the target table for the current job).
6.  **Job Completion:** The `dataset.job_table` is updated to mark the job as 'COMPLETED', including the processed record count.
7.  **Error Handling:** If any validation or SQL execution errors occur, an entry is made in `dataset.error_log`, and an appropriate `RAISE` statement halts execution.

**Lineage (BigQuery Perspective):**
*   **Source Node:** `CLOUD_COMPOSER_DAG` (orchestrating `r_ausd_vertrag_control`)
*   **Invokes:** `BIGQUERY_STORED_PROCEDURE:dataset.r_ausd_vertrag_control`
*   **`dataset.r_ausd_vertrag_control`:**
    *   **Reads:** `BIGQUERY_TABLE:dataset.job_table` (for status checks), `BIGQUERY_TABLE:dataset.DWTK_MELDUNGEN`, `BIGQUERY_TABLE:dataset.CDS_TA_NOTICE`.
    *   **Writes:** `BIGQUERY_TABLE:dataset.job_table` (for status updates), `BIGQUERY_TABLE:dataset.error_log`, `BIGQUERY_TABLE:dataset.SOF_TA_NOTICE`, `BIGQUERY_TABLE:dataset.VIA`.
    *   **Uses:** `BIGQUERY_SQL` (migrated from `d_ausd_v_ta_notice.sql`).

## 5. Transformation Logic

**File: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh` (Orchestration Logic)**

*   **Parameter Parsing (`getopts`):** The `-j` and `-f` parameters will be directly mapped to `IN` parameters of the BigQuery Stored Procedure, namely `p_JobKennung STRING` and `p_EintragsNr STRING`. The `-h` (help) flag will not be directly migrated but its intent (user guidance) would be handled by documentation or pre-validation in the orchestrator.
*   **Environment Sourcing (`. $HOME/.dw_init`, `. ${BERT_DIR_ROOT}/...util/bin/*.ksh`):** These environment setups and utility script inclusions will be replaced by:
    *   BigQuery Stored Procedure declarations for variables.
    *   Referencing other BigQuery Stored Procedures or UDFs for common utility functions (e.g., date handling, logging).
    *   Configuration values (like `BERT_DIR_ROOT`, `DW_DIR_UTL`) will be externalized to metadata tables, environment variables in the orchestrator, or constants within the BigQuery Stored Procedure.
*   **Parameter Validation (`pruefeParameterGesetzt`):** This will be translated into `IF` statements within the BigQuery Stored Procedure to check for `NULL` or empty strings for `p_JobKennung` and `p_EintragsNr`.
*   **Error Handling (`DWMSG_MeldeFehler`, `exit`):** The legacy error framework will be replaced by:
    *   `INSERT` statements into a `dataset.error_log` table.
    *   `RAISE` statements in BigQuery Scripting to signal errors and stop execution.
*   **SQL Script Execution (`starteSQLSkript` for `d_ausd_v_ta_notice.sql`):** The contents of `d_ausd_v_ta_notice.sql` will be converted to BigQuery SQL and either inlined directly into the BigQuery Stored Procedure or executed dynamically using `EXECUTE IMMEDIATE` if the SQL is subject to dynamic generation in the original script.
*   **Temporary File (`tmpFile`, `cat $tmpFile`):** The use of a temporary file for passing the record count will be replaced by a `DECLARE`d variable (`v_records INT64`) within the BigQuery Stored Procedure, populated by a `SELECT COUNT(*)` query.
*   **Job Table Updates:** The implicit "active jobs ignored" and "old active jobs deactivated" logic will be implemented as explicit `INSERT` and `UPDATE` statements on the `dataset.job_table` within the Stored Procedure.

**File: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_notice.sql` (Data Logic)**

*   The Oracle SQL within this file will be translated line-by-line to BigQuery Standard SQL, ensuring:
    *   Data type compatibility.
    *   Function mapping (e.g., `SYSDATE` to `CURRENT_DATE()`, `TRUNC` to `DATE_TRUNC`).
    *   Table and column name adjustments to match BigQuery naming conventions (e.g., removing `$`).
    *   Handling of `PACKAGE:DWPA_UTIL_SKRIPT` - this Oracle package's functions will need to be re-implemented in BigQuery as UDFs (SQL or JavaScript) or within BigQuery Stored Procedures.
*   `TABLE:DWTK_MELDUNGEN` and `TABLE:CDS$TA_NOTICE` will be mapped to their corresponding BigQuery tables for `SELECT` operations.
*   `TABLE:SOF$TA_NOTICE` and `TABLE:VIA` will be mapped to their corresponding BigQuery tables for `INSERT`/`UPDATE` operations.

## 6. External Dependencies

| Legacy External System / Dependency | How it is Replaced in BigQuery                                                                     |
| :---------------------------------- | :------------------------------------------------------------------------------------------------- |
| **Oracle Database** (implied by SQL script) | Replaced by Google BigQuery as the target data warehouse. All Oracle SQL will be converted to BigQuery Standard SQL. |
| **`DWMSG_MeldeFehler` / Error Handling Framework** | Replaced by a BigQuery `error_log` table and `RAISE` statements within BigQuery Stored Procedures for error notification. |
| **Email notifications** (via `DWMSG_ERMITTLENR` in `f_alis_msgerr.ksh`) | Can be integrated using Cloud Functions triggered by BigQuery logs or Pub/Sub notifications from job failures, sending emails via SendGrid or similar services. |
| **`TABLE:DUAL`** (from `h_alis_date.ksh`) | Not needed in BigQuery; simple `SELECT` statements (e.g., `SELECT CURRENT_DATE()`) suffice. |
| **`PACKAGE:DWPA_UTIL_SKRIPT`** | Functions within this Oracle package will need to be re-implemented in BigQuery as BigQuery SQL UDFs, JavaScript UDFs, or integrated directly into BigQuery Stored Procedures. |

## 7. Unresolved / Risks

*   **Complexity of `d_ausd_v_ta_notice.sql`:** While the orchestration is `semi_auto`, the SQL script `d_ausd_v_ta_notice.sql` contains the core transformation logic. Its actual content was not analyzed in detail by a tool in this specific run. If it contains complex, highly procedural Oracle PL/SQL, or very specific Oracle functions, its migration might be `complex` or `redesign` and require manual effort. A separate analysis of this SQL file would be beneficial.
*   **Full functionality of utility scripts:** The full scope of `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` needs to be reviewed to ensure all their functionalities are correctly mapped to BigQuery equivalents or handled by the orchestration layer. For example, `h_alis_sqlplus.ksh` would be entirely replaced by native BigQuery execution.
*   **`r_ausd_vertrag.ksh` (Invoking Script):** This script invokes `k_ausd_v_ta_notice.ksh`. The migration of `r_ausd_vertrag.ksh` (or its upstream process) needs to be designed to call the new BigQuery Stored Procedure. This implies migrating the entire job workflow, possibly to Cloud Composer.
*   **Job-specific parameters and configuration:** Any implicit configurations or environment variables sourced by `$HOME/.dw_init` need to be explicitly defined and managed in the BigQuery environment (e.g., as Cloud Composer variables, BigQuery project constants, or configuration tables).

## 8. Build Plan

The migration will be executed in the following order:

1.  **Migrate Source and Target Tables to BigQuery:**
    *   Create BigQuery DDL for `DWTK_MELDUNGEN`, `CDS_TA_NOTICE`, `SOF_TA_NOTICE`, `VIA`.
    *   Ingest historical data into these new BigQuery tables.
2.  **Create BigQuery Utility Components:**
    *   Design and implement `dataset.job_table` and `dataset.error_log` DDL.
    *   Create any necessary BigQuery UDFs or auxiliary Stored Procedures to replace functions from `DWPA_UTIL_SKRIPT` or other sourced utilities (e.g., date calculations, string manipulations).
3.  **Migrate `d_ausd_v_ta_notice.sql` to BigQuery SQL:**
    *   Convert the Oracle SQL to BigQuery Standard SQL.
    *   Test the standalone BigQuery SQL logic against the migrated tables.
4.  **Create `dataset.r_ausd_vertrag_control` BigQuery Stored Procedure:**
    *   Implement parameter handling (`p_JobKennung`, `p_EintragsNr`).
    *   Integrate parameter validation and error logging (`dataset.error_log`).
    *   Implement job state management logic (`INSERT`/`UPDATE` on `dataset.job_table`).
    *   Embed or call the migrated BigQuery SQL from `d_ausd_v_ta_notice.sql`.
    *   Implement record counting logic.
    *   Test the Stored Procedure in isolation.
5.  **Develop Cloud Composer DAG (or equivalent orchestrator):**
    *   Create a Python DAG in Cloud Composer to invoke the `dataset.r_ausd_vertrag_control` BigQuery Stored Procedure, passing required parameters.
    *   Include tasks for pre-processing (if any), BigQuery job execution, and post-processing/monitoring.
    *   Integrate monitoring and alerting.
6.  **End-to-End Testing and Validation:**
    *   Run the Cloud Composer DAG and verify job execution, data transformation, and record counts.
    *   Compare results with the legacy system to ensure data parity.

**Build Artifacts:**
*   `bigquery/ddl/job_table.sql`
*   `bigquery/ddl/error_log.sql`
*   `bigquery/ddl/DWTK_MELDUNGEN.sql`
*   `bigquery/ddl/CDS_TA_NOTICE.sql`
*   `bigquery/ddl/SOF_TA_NOTICE.sql`
*   `bigquery/ddl/VIA.sql`
*   `bigquery/udf/dwpa_util_skript_function_x.sql` (for each relevant function)
*   `bigquery/stored_procedures/r_ausd_vertrag_control.sql` (containing the migrated ksh + d_ausd_v_ta_notice.sql logic)
*   `composer/dags/r_ausd_vertrag_dag.py`
*   `docs/migration_test_plan.md`