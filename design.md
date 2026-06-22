# Migration Design — DW.BERT_AUSD_BP_TA_BCP_MSISDN

## 1. Purpose & Scope
This migration design document outlines the plan to transition the `DW.BERT_AUSD_BP_TA_BCP_MSISDN` job from its legacy Automic (UC4), KornShell, and Oracle SQL environment to Google Cloud Platform, specifically leveraging BigQuery for data processing and Airflow for orchestration.

The primary purpose of the `DW.BERT_AUSD_BP_TA_BCP_MSISDN` job is the initial provisioning of selected basic products ("Basisprodukte") for the BERT system, specifically related to MSISDN (Mobile Subscriber ISDN Number) data. It generates a cut-off date extraction of contract cache data from the Data Warehouse (DWH) and makes this data available for a downstream Forderungsscoring (FOS) system. The job involves:
*   Orchestration and scheduling via UC4.
*   Shell scripting for parameter handling, environment setup, and sequential execution control.
*   Oracle SQL for the core data extraction, transformation, and loading (ETL) logic.

The scope of this migration is to re-platform this entire workflow to BigQuery and Airflow, ensuring functional equivalence and maintaining data integrity.

## 2. Source Inventory
The `DW.BERT_AUSD_BP_TA_BCP_MSISDN` job is composed of four primary source files:

| File Name (Relative Path)                                                                                                              | Technology         | Tier     | Automation Bucket | Summary                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| :------------------------------------------------------------------------------------------------------------------------------------- | :----------------- | :------- | :---------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_BCP_MSISDN.xml` | UC4/Automic        | medium   | semi_auto         | UC4 job definition for a UNIX script that prepares instantiated basic products related to MSISDN data. It serves as the scheduler, invoking the `r_ausd_bp_ta_bcp_msisdn.ksh` script.                                                                                                                                                                                                                                                                                               |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_msisdn.ksh`                                                 | KornShell          | medium   | semi_auto         | This KornShell script orchestrates the initial provision of selected basic products for BERT. It handles parameter parsing (cutoff date, restart value), environment setup, and calls the core processing script `k_ausd_bp_ta_bcp_msisdn.ksh`. It also includes logging and error handling.                                                                                                                                                                                                             |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh`                                                 | KornShell          | medium   | semi_auto         | This KornShell script acts as a control script for data preparation. It receives parameters from the calling script, performs date validation, and orchestrates the execution of the Oracle SQL script `d_ausd_bp_ta_bcp_msisdn.sql` using an internal `starteSQLSkript` function. It manages temporary files for record counts.                                                                                                                                                                                  |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bcp_msisdn.sql`                                                 | Oracle SQL         | complex  | manual            | This Oracle SQL script is the core data transformation component. It truncates the `sof$ta_bcp_msisdn` table and then populates it by joining `sof$ta_bpr_bcp` and `sof$ta_rn_vertrag` tables, enriching the target with MSISDN data. It also derives a date variable from the `isbert_schema.dwtk_meldungen` metadata table. Oracle-specific syntax and hints are used. |

## 3. Target Architecture
The migrated solution will operate entirely within Google Cloud Platform, utilizing the following components:

*   **Scheduler/Orchestration:** Apache Airflow running on Cloud Composer. This will replace the UC4 scheduler and the KornShell wrapper scripts.
*   **Data Transformation & Processing:** BigQuery. All SQL logic will be converted to BigQuery SQL, primarily implemented as BigQuery Stored Procedures.
*   **Data Storage:** BigQuery datasets and tables will replace the Oracle tables.
*   **Logging & Monitoring:** Cloud Logging and Cloud Monitoring, integrating with Airflow. Custom audit tables in BigQuery will capture job-specific metadata and status.

The overall architecture will involve an Airflow DAG that invokes BigQuery Stored Procedures. The shell script logic for parameter passing and control flow will be translated into parameters and control logic within BigQuery Stored Procedures or potentially Python operators in Airflow for more complex procedural elements.

## 4. Data Flow & Lineage
The data flow and execution lineage of the migrated job will be as follows:

1.  **Airflow DAG (`dw_bert_ausd_bp_ta_bcp_msisdn`)**:
    *   Replaces the UC4 `DW.BERT_AUSD_BP_TA_BCP_MSISDN.xml` job.
    *   This DAG will be defined in Python and scheduled in Cloud Composer.
    *   It will trigger the main BigQuery Stored Procedure, passing necessary parameters like the cutoff date (`p_stichtag`) and restart value (`p_wiederanlaufWert`).

2.  **BigQuery Stored Procedure (`sp_r_ausd_bp_ta_bcp_msisdn`)**:
    *   Replaces the `r_ausd_bp_ta_bcp_msisdn.ksh` KornShell script.
    *   This procedure will handle initial parameter validation and default date logic (if a cutoff date is not provided, it defaults to the current system date).
    *   It will manage job logging/auditing by inserting/updating records in BigQuery audit tables.
    *   It will then call the next BigQuery Stored Procedure, `sp_k_ausd_bp_ta_bcp_msisdn`, passing all required parameters.

3.  **BigQuery Stored Procedure (`sp_k_ausd_bp_ta_bcp_msisdn`)**:
    *   Replaces the `k_ausd_bp_ta_bcp_msisdn.ksh` KornShell script.
    *   This procedure will perform further parameter validation and specific date checks.
    *   It will contain logic for handling the restart value (e.g., deleting records in the target table above the `wiederanlaufWert` threshold if required by the business logic, before re-inserting).
    *   Its primary function is to execute the core data transformation logic, which will be encapsulated in another BigQuery Stored Procedure (`sp_d_ausd_bp_ta_bcp_msisdn`). It will also capture and log the number of processed records.

4.  **BigQuery Stored Procedure (`sp_d_ausd_bp_ta_bcp_msisdn`)**:
    *   Replaces the `d_ausd_bp_ta_bcp_msisdn.sql` Oracle SQL script.
    *   This is the core data processing unit.
    *   It will truncate the target BigQuery table `sof_ta_bcp_msisdn`.
    *   It will insert `DISTINCT` records into `sof_ta_bcp_msisdn` by joining `sof_ta_bpr_bcp` and `sof_ta_rn_vertrag` on `cntrct_id_ref = cntrct_id`.
    *   It will also query `dwtk_meldungen` to determine a date variable, replacing the Oracle-specific `NVL(TO_CHAR(MAX(m.timecreated), 'YYYYMMDD'), '19000101')` logic.

**Source to Target Data Flow:**
*   **Sources:**
    *   Oracle table `sof$ta_bpr_bcp` -> BigQuery table `sof_ta_bpr_bcp`
    *   Oracle table `sof$ta_rn_vertrag` -> BigQuery table `sof_ta_rn_vertrag`
    *   Oracle table `isbert_schema.dwtk_meldungen` -> BigQuery table `dwtk_meldungen`
*   **Target:**
    *   Oracle table `sof$ta_bcp_msisdn` -> BigQuery table `sof_ta_bcp_msisdn`

## 5. Transformation Logic

### 5.1 UC4 Job to Airflow DAG
*   The UC4 job `DW.BERT_AUSD_BP_TA_BCP_MSISDN.xml` will be converted to an Airflow DAG named `dw_bert_ausd_bp_ta_bcp_msisdn`.
*   The DAG will use a `DataprocSubmitJobOperator` (or equivalent if the shell script logic is purely BigQuery native) or a `BigQueryOperator` to invoke the main BigQuery Stored Procedure.
*   Scheduling (if present in UC4 external events, not found in the XML) will be configured in the Airflow DAG.
*   Error handling will leverage Airflow's retry mechanisms and logging, replacing UC4's `AutoDeact` and `MaxRetCode` settings.

### 5.2 KornShell (`r_ausd_bp_ta_bcp_msisdn.ksh`) to BigQuery Stored Procedure (`sp_r_ausd_bp_ta_bcp_msisdn`)
*   **Parameter Handling:** `getopts` logic for `-s` (Stichtag) and `-l` (Wiederanlaufwert) will be replaced with BigQuery Stored Procedure input parameters.
*   **Environment Initialization:** `. $HOME/.dw_init` and `BERT_DIR_ROOT` references will be replaced by BigQuery procedure parameters, constants, or Airflow variables.
*   **Date Logic:** `DWDate_Gib_Zeitraum` will be replaced by BigQuery's `CURRENT_DATE()`, `FORMAT_DATE()`, and `PARSE_DATE()` functions. The logic for defaulting `p_stichtag` to the system date if not provided will be implemented using `IFNULL` or `IF` statements.
*   **Error Handling:** Shell `set -e`, `trap`, and `DWMSG_MeldeFehler` will be replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks and inserts into BigQuery audit tables.
*   **Script Invocation:** The call to `${Name_Kernskript}` (`k_ausd_bp_ta_bcp_msisdn.ksh`) will become a `CALL` statement to `sp_k_ausd_bp_ta_bcp_msisdn`.

### 5.3 KornShell (`k_ausd_bp_ta_bcp_msisdn.ksh`) to BigQuery Stored Procedure (`sp_k_ausd_bp_ta_bcp_msisdn`)
*   **Parameter Handling:** `getopts` logic for `-j` (JobKennung), `-f` (EintragsNr), `-s` (Stichtag), and `-l` (wiederanlaufWert) will be replaced with BigQuery Stored Procedure input parameters.
*   **Parameter Validation:** `pruefeParameterGesetzt` and manual `if` checks will be converted to `IF` statements and `ASSERT`/`RAISE` in BigQuery SQL for robust validation.
*   **Date Validation:** `DWDate_Datum_Check` will be replaced with `SAFE.PARSE_DATE` checks.
*   **SQL Script Execution:** The `starteSQLSkript` function (which executes `d_ausd_bp_ta_bcp_msisdn.sql`) will be replaced by a `CALL` to `sp_d_ausd_bp_ta_bcp_msisdn` within this procedure.
*   **Record Count:** Reading `v_records` from `$tmpFile` will be replaced by a `SELECT COUNT(*)` on the target table after insertion, stored in a BigQuery variable, and logged to an audit table.
*   **Commented-out Post-processing:** The `sed`, `sort`, `join` operations, if deemed necessary, will be reimplemented as BigQuery SQL transformations on staging tables.

### 5.4 Oracle SQL (`d_ausd_bp_ta_bcp_msisdn.sql`) to BigQuery Stored Procedure (`sp_d_ausd_bp_ta_bcp_msisdn`)
*   **Variable Definition:** `DEFINE v_carmen` and `COLUMN s_datum new_value v_datum` will be replaced by BigQuery `DECLARE` and `SET` statements.
*   **Date Derivation:** `SELECT NVL(TO_CHAR(MAX(m.timecreated), 'YYYYMMDD'), '19000101') AS s_datum FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'` will be translated to use `COALESCE`, `FORMAT_TIMESTAMP` (or `FORMAT_DATE`), and BigQuery table `dwtk_meldungen`.
*   **Truncate Table:** Oracle `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_bcp_msisdn REUSE STORAGE')` will be replaced by standard BigQuery `TRUNCATE TABLE sof_ta_bcp_msisdn;`.
*   **INSERT/SELECT:** The core `INSERT INTO ... SELECT DISTINCT ... FROM ... JOIN ... WHERE` statement will be directly translatable to BigQuery SQL. Oracle-specific hints like `/*+ full(bp) parallel(bp,4) full(rn) parallel(rn,4) */` will be removed as BigQuery automatically optimizes queries.
*   **COMMIT:** Oracle `COMMIT;` is implicitly handled by BigQuery's transactional model for DML statements within a script or procedure.
*   **SQL*Plus Commands:** `prompt`, `start`, `spool`, `WHENEVER SQLERROR`, `set timing on`, `exit success` will be removed. Error handling will be managed by BigQuery scripting or the calling stored procedure.

## 6. External Dependencies

| Original External System/Object                                                                                                | How it's currently used                                                                                                                                                                                                                         | Replacement in GCP                                                                                                                                                                                                                                                               |
| :----------------------------------------------------------------------------------------------------------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **UC4/Automic Scheduler** (`DW.BERT_AUSD_BP_TA_BCP_MSISDN.xml`)                                                                | Schedules and triggers the execution of the main KornShell script.                                                                                                                                                                              | **Cloud Composer (Airflow DAG)**: A dedicated Airflow DAG (`dw_bert_ausd_bp_ta_bcp_msisdn`) will be created to manage the scheduling, triggering, and monitoring of the BigQuery data pipeline.                                                                                |
| **Oracle Database** (tables: `sof$ta_bpr_bcp`, `sof$ta_rn_vertrag`, `isbert_schema.dwtk_meldungen`, `sof$ta_bcp_msisdn`)     | Source for contract and MSISDN data, metadata for date determination, and target for the processed MSISDN data.                                                                                                                                 | **BigQuery Tables**: Corresponding tables (`sof_ta_bpr_bcp`, `sof_ta_rn_vertrag`, `dwtk_meldungen`, `sof_ta_bcp_msisdn`) will be created in BigQuery. Initial data migration from Oracle to BigQuery will be required.                                                       |
| **UNIX Environment / KornShell (`. $HOME/.dw_init`, `BERT_DIR_ROOT`)**                                                      | Provides environment variables, paths, and common utilities for the shell scripts.                                                                                                                                                              | **BigQuery Procedure Parameters/Variables & Airflow Variables**: Environment-specific configurations will be passed as parameters to BigQuery Stored Procedures or managed as Airflow Variables/Connections.                                                                      |
| **KornShell Helper Scripts** (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) | Provide common functions for error handling, date manipulation, parameter parsing, and SQL execution.                                                                                                                                           | **BigQuery UDFs / Integrated Logic / Python Utilities**: Simple functions will be integrated directly into BigQuery Stored Procedures or converted to BigQuery User-Defined Functions (UDFs). More complex logic might be implemented in Python helper modules.             |
| **Temporary Files** (`$DW_DIR_UTL/bert_k_ausd_bp_ta_bcp_msisdn.tmp`)                                                         | Used to store intermediate results, specifically the count of processed records.                                                                                                                                                                | **BigQuery Variables / Audit Tables**: Record counts will be stored in BigQuery variables within stored procedures and persisted to BigQuery audit tables for historical tracking, eliminating the need for temporary files.                                               |
| **Oracle PL/SQL (`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`)**                                                            | Used to execute dynamic SQL (specifically `TRUNCATE TABLE`).                                                                                                                                                                                    | **BigQuery DDL/DML Statements**: Directly replaced by standard BigQuery `TRUNCATE TABLE` DDL. BigQuery Stored Procedures support DDL/DML directly.                                                                                                                            |

## 7. Unresolved / Risks

*   **Inferred Dependencies**: The automated lineage tool did not identify direct `lineage_edges` between the files. The execution flow has been inferred from code analysis. While highly probable, there's a minor risk of missing implicit or dynamically resolved dependencies not evident from static code analysis. A thorough functional validation post-migration is crucial.
*   **Helper Script Re-implementation**: The exact logic within helper KornShell scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_date.ksh`) needs to be fully understood and accurately re-implemented in BigQuery SQL (as UDFs or integrated logic) or Python. Lack of full context for these helpers could lead to functional discrepancies.
*   **Commented-out Logic**: The KornShell scripts contain commented-out sections related to "FOS job management" and extensive "Nachverarbeitung" (post-processing) using `sed`, `sort`, `join` on temporary data files. It's unclear if this logic is obsolete or intended for future use. This needs explicit confirmation from stakeholders. If relevant, this "manual" complexity (B3) will require significant effort to migrate to BigQuery (e.g., staging tables, SQL transformations, or Python processing if file-based operations are unavoidable).
*   **Oracle Specifics**: Oracle optimizer hints `/*+ full(bp) parallel(bp,4) full(rn) parallel(rn,4) */` are not applicable in BigQuery. While BigQuery automatically optimizes queries, performance characteristics might differ. Thorough performance testing will be required.
*   **Data Type Mismatch**: While basic column types are usually straightforward, subtle differences in how Oracle and BigQuery handle specific data types (e.g., precision for numbers, time zones for dates) could lead to data discrepancies. A detailed data type mapping and validation plan is essential.
*   **Error Code Semantics**: The KornShell scripts use specific `ErrNr` values (e.g., 192, 193). While BigQuery procedures can `RAISE` errors, the exact mapping and downstream handling of these specific error codes need to be designed if external systems rely on them.

## 8. Build Plan

The migration will be executed in a phased approach, focusing on modular conversion and validation.

1.  **BigQuery Schema Definition (SQL - DDL)**
    *   Create `sof_ta_bpr_bcp` table in BigQuery.
    *   Create `sof_ta_rn_vertrag` table in BigQuery.
    *   Create `dwtk_meldungen` table in BigQuery.
    *   Create `sof_ta_bcp_msisdn` target table in BigQuery.
    *   Define BigQuery tables for job auditing/logging (e.g., `job_audit_log`, `job_registry`).

2.  **Initial Data Loading (Python/Dataflow/Cloud Storage)**
    *   Extract historical data from Oracle `sof$ta_bpr_bcp` and load into BigQuery `sof_ta_bpr_bcp`.
    *   Extract historical data from Oracle `sof$ta_rn_vertrag` and load into BigQuery `sof_ta_rn_vertrag`.
    *   Extract historical data from Oracle `isbert_schema.dwtk_meldungen` and load into BigQuery `dwtk_meldungen`.
    *   Establish ongoing data ingestion strategy (e.g., CDC, batch export/import) for these source tables.

3.  **BigQuery Stored Procedure: `sp_d_ausd_bp_ta_bcp_msisdn` (BigQuery SQL)**
    *   Translate `d_ausd_bp_ta_bcp_msisdn.sql` into BigQuery SQL, encapsulating it as a stored procedure.
    *   Implement `TRUNCATE TABLE sof_ta_bcp_msisdn;`
    *   Implement the `INSERT INTO sof_ta_bcp_msisdn ... SELECT DISTINCT ...` logic with BigQuery-compatible syntax.
    *   Replace Oracle date functions with BigQuery equivalents.

4.  **BigQuery Stored Procedure: `sp_k_ausd_bp_ta_bcp_msisdn` (BigQuery SQL)**
    *   Translate `k_ausd_bp_ta_bcp_msisdn.ksh` control flow and parameter validation into a BigQuery stored procedure.
    *   Implement date validation using BigQuery functions.
    *   Include a `CALL sp_d_ausd_bp_ta_bcp_msisdn(...)` statement.
    *   Implement logging for record counts and job status using BigQuery audit tables.
    *   Address restart logic by modifying the core SQL or adding conditional DML.

5.  **BigQuery Stored Procedure: `sp_r_ausd_bp_ta_bcp_msisdn` (BigQuery SQL)**
    *   Translate `r_ausd_bp_ta_bcp_msisdn.ksh` orchestration logic into a BigQuery stored procedure.
    *   Handle `p_stichtag` and `p_wiederanlaufWert` input parameters.
    *   Implement default date logic using BigQuery functions.
    *   Include a `CALL sp_k_ausd_bp_ta_bcp_msisdn(...)` statement.
    *   Implement overall job logging and error handling.

6.  **Airflow DAG: `dw_bert_ausd_bp_ta_bcp_msisdn` (Python)**
    *   Create an Airflow DAG that triggers `sp_r_ausd_bp_ta_bcp_msisdn` using a `BigQueryExecuteStoredProcedureOperator` or `BigQueryOperator` (if the wrapper logic remains simple).
    *   Configure scheduling, retry policies, and error notifications.
    *   Map Airflow variables/connections to BigQuery project, dataset, and any runtime parameters.

7.  **Helper Functions/UDFs (BigQuery SQL / Python)**
    *   Analyze and convert remaining shell helper script logic (e.g., parameter validation, date utilities) into BigQuery UDFs or Python modules if used in Airflow.

8.  **Testing and Validation (Automated & Manual)**
    *   **Unit Tests:** For each BigQuery Stored Procedure.
    *   **Integration Tests:** End-to-end tests through the Airflow DAG.
    *   **Data Validation:** Compare output data in `sof_ta_bcp_msisdn` in BigQuery with the legacy Oracle output for accuracy and completeness, especially focusing on edge cases like restart values and date handling.
    *   **Performance Testing:** Benchmark query performance in BigQuery against legacy Oracle.

9.  **Deployment & Cutover**
    *   Deploy DDL and Stored Procedures to BigQuery.
    *   Deploy Airflow DAG to Cloud Composer.
    *   Schedule and monitor production runs.
    *   Decommission legacy UC4 job and related scripts/tables after successful cutover.