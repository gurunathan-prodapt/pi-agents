# Migration Design — DW.BERT_AUSD_BP_TA_TARIFOPTION

## 1. Purpose & Scope
This migration design document outlines the conversion of the legacy ETL job `DW.BERT_AUSD_BP_TA_TARIFOPTION` to Google Cloud Platform, targeting BigQuery for data processing and Airflow for orchestration. The original job is responsible for the preparation and provisioning of selected base products and tariff options for the BERT system. It involves an UC4 scheduler, shell scripts for control flow and parameter handling, and an Oracle SQL script for the core data transformation logic. The scope of this migration includes translating the UC4 orchestration to an Airflow DAG, converting KornShell scripts to BigQuery stored procedures, and adapting the Oracle SQL script to BigQuery SQL, preserving the existing business logic and data outputs.

## 2. Source Inventory

| File Name | Technology | Complexity Tier | Automation Bucket | Summary |
|---|---|---|---|---|
| `DW.BERT_AUSD_BP_TA_TARIFOPTION.xml` | UC4/Automic | medium | semi_auto | UC4 job definition for a UNIX job named DW.BERT_AUSD_BP_TA_TARIFOPTION, which orchestrates the execution of a KornShell script for data preparation. |
| `d_ausd_bp_ta_tarifoption.sql` | Oracle SQL/PLSQL | medium | semi_auto | This SQL script defines and populates two tables, SOF$TA_BPR_OPT_FILTER and SOF$TA_TARIFOPTION, by joining and transforming data from source tables, including a dynamically named table and custom concatenation functions. |
| `k_ausd_bp_ta_tarifoption.ksh` | KornShell | medium | semi_auto | This KornShell script acts as a control script for a data processing job, handling parameter parsing, date validation, and orchestrating the execution of a SQL script to process data related to 'PoolBasisprodukt'. |
| `r_ausd_bp_ta_tarifoption.ksh` | KornShell | medium | semi_auto | This ksh script orchestrates the initial provision of selected basic products for BERT. It generates a snapshot of contract cache from DWH and makes it available for demand scoring (FOS-Tabelle). |

## 3. Target Architecture

The target architecture will leverage Google Cloud Platform services:
- **Orchestration**: Apache Airflow on Cloud Composer for scheduling and managing the job workflow.
- **Data Processing**: BigQuery for all SQL-based data transformations and data storage.
- **Scripting Logic**: BigQuery Stored Procedures for shell script logic translation and parameter handling.
- **Data Storage**: BigQuery native tables will replace Oracle tables.

The flow will be structured as follows:
- An Airflow DAG will be created to orchestrate the entire process.
- The Airflow DAG will invoke a BigQuery Stored Procedure that encapsulates the logic from `r_ausd_bp_ta_tarifoption.ksh`.
- This stored procedure will in turn call another BigQuery Stored Procedure representing `k_ausd_bp_ta_tarifoption.ksh`.
- The `k_ausd_bp_ta_tarifoption.ksh` equivalent stored procedure will execute the BigQuery SQL translation of `d_ausd_bp_ta_tarifoption.sql`.

## 4. Data Flow & Lineage

The original job execution flow is:
`UC4 XML (DW.BERT_AUSD_BP_TA_TARIFOPTION)` -> `r_ausd_bp_ta_tarifoption.ksh` -> `k_ausd_bp_ta_tarifoption.ksh` -> `d_ausd_bp_ta_tarifoption.sql`

This translates to the following target data flow:

1.  **Airflow DAG (`dw_bert_ausd_bp_ta_tarifoption`)**: Triggered by schedule (TBD, as no UC4 schedule was provided).
    *   **Task**: `run_r_ausd_bp_ta_tarifoption` (DataprocSubmitJobOperator or BigQueryOperator calling a stored procedure).
        *   This task encapsulates the logic of `r_ausd_bp_ta_tarifoption.ksh`, converted to a BigQuery Stored Procedure (`project.dataset.sp_bereitstellung_basisprodukte_bert`).
        *   It handles job-level parameters (Stichtag, Wiederanlaufwert), logging, and error handling.
        *   It then calls the next processing step.
2.  **BigQuery Stored Procedure (`project.dataset.sp_k_ausd_bp_ta_tarifoption`)**: Called by `sp_bereitstellung_basisprodukte_bert`.
    *   This stored procedure encapsulates the logic of `k_ausd_bp_ta_tarifoption.ksh`.
    *   It performs parameter validation, date format checks, and dynamically determines other parameters.
    *   It then executes the core SQL logic.
3.  **BigQuery SQL Script (`d_ausd_bp_ta_tarifoption.sql` equivalent)**: Executed within `sp_k_ausd_bp_ta_tarifoption`.
    *   This script performs the main data transformation.
    *   **Reads from**:
        *   `isbert_schema.dwtk_meldungen` (legacy Oracle table) -> `project.dataset.dwtk_meldungen` (BigQuery)
        *   `isbert_schema.sof$ta_l_bpr_optionen_filter` (legacy Oracle table) -> `project.dataset.sof_ta_l_bpr_optionen_filter` (BigQuery)
        *   `sof$ta_bpr_opt_text_&v_datum` (dynamically named Oracle table) -> `project.dataset.sof_ta_bpr_opt_text_YYYYMMDD` (BigQuery)
    *   **Writes to**:
        *   `sof$ta_bpr_opt_filter` (legacy Oracle table) -> `project.dataset.sof_ta_bpr_opt_filter` (BigQuery temporary/intermediate table)
        *   `sof$ta_tarifoption` (legacy Oracle table) -> `project.dataset.sof_ta_tarifoption` (BigQuery final output table)

## 5. Transformation Logic

**5.1. UC4 Orchestration (`DW.BERT_AUSD_BP_TA_TARIFOPTION.xml`)**
-   **Original**: A `JOBS_UNIX` object that executes the `r_ausd_bp_ta_tarifoption.ksh` script. It includes logging and environment setup.
-   **Target**: An Airflow DAG `dw_bert_ausd_bp_ta_tarifoption`.
    -   It will have a single task, likely using `BigQueryOperator` or `DataprocSubmitJobOperator` (if Python script is used as wrapper), to execute the BigQuery stored procedure that mirrors `r_ausd_bp_ta_tarifoption.ksh`.
    -   `dag_id` will be `dw_bert_ausd_bp_ta_tarifoption`.
    -   `schedule` will be determined during the build phase as no UC4 schedule object was provided.
    -   Error handling will be managed by Airflow's default retry mechanism and potential `on_failure_callback` for alerting.

**5.2. Wrapper Script (`r_ausd_bp_ta_tarifoption.ksh`)**
-   **Original**: Main orchestrator script. Handles parameters (`-s` for Stichtag, `-l` for Wiederanlaufwert), date derivation, calls the kernel script (`k_ausd_bp_ta_tarifoption.ksh`), and manages custom logging.
-   **Target**: BigQuery Stored Procedure `project.dataset.sp_bereitstellung_basisprodukte_bert`.
    -   **Parameters**: `p_stichtag STRING`, `p_wiederanlaufWert INT64`.
    -   **Date Handling**: `DWDate_Gib_Zeitraum` will be replaced by BigQuery's `CURRENT_DATE()` and `FORMAT_DATE()`. Defaulting `p_stichtag` to `v_sysdate` if not provided.
    -   **Parameter Validation**: `pruefeParameterGesetzt` and `DWDate_Datum_Check` will be replaced by `IF` statements and `PARSE_DATE()` with error handling (`EXCEPTION WHEN ERROR`).
    -   **Logging**: `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_SetzeStatusOK`, `DWMSG_MeldeFehler` will be translated into `INSERT` statements into a BigQuery `job_audit_log` table.
    -   **Core Logic Invocation**: The call to `k_ausd_bp_ta_tarifoption.ksh` will be translated to a `CALL` statement to `project.dataset.sp_k_ausd_bp_ta_tarifoption`.
    -   **Exit Status**: Shell exit codes will be managed by BigQuery's error handling within the stored procedure.

**5.3. Kernel Script (`k_ausd_bp_ta_tarifoption.ksh`)**
-   **Original**: Control script. Parses job-specific parameters (`-j`, `-f`, `-s`, `-l`), performs parameter and date validation, determines yesterday/today, and calls `starteSQLSkript` to execute `d_ausd_bp_ta_tarifoption.sql`.
-   **Target**: BigQuery Stored Procedure `project.dataset.sp_k_ausd_bp_ta_tarifoption`.
    -   **Parameters**: `p_JobKennung STRING`, `p_EintragsNr STRING`, `p_Stichtag STRING`, `p_wiederanlaufWert STRING`.
    -   **Parameter Parsing & Validation**: `getopts` will be replaced by direct stored procedure parameters. `pruefeParameterGesetzt` and date checks will use `IF` statements and `PARSE_DATE()`.
    -   **Date Derivation**: `gestern.ksh` will be replaced by `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`.
    -   **SQL Execution**: The `starteSQLSkript` invocation of `d_ausd_bp_ta_tarifoption.sql` will be directly incorporated or called as a separate BigQuery script or procedure.
    -   **Temporary Files**: `tmpFile` for record count will be replaced by a BigQuery `DECLARE` variable or an `OUT` parameter.
    -   **Job Management**: Commented `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` will be ignored or implemented as BigQuery audit table updates if required.

**5.4. SQL Script (`d_ausd_bp_ta_tarifoption.sql`)**
-   **Original**: Oracle SQL script for data transformation. It involves:
    -   Defining `v_carmen` and `v_datum` substitution variables.
    -   Conditional `DROP TABLE` statements for restartability.
    -   `CREATE TABLE ... AS SELECT` statements for `sof$ta_bpr_opt_filter` and `sof$ta_tarifoption`.
    -   Uses Oracle-specific functions (`NVL`, `TO_CHAR`, `SUBSTR`, `LTRIM`, `RTRIM`), `CASE WHEN`, and `LEAD` analytic function.
    -   References custom concatenation functions (`sof$ab_con.concat1`, `sof$ab_con.concat1r`, etc.).
    -   Uses `parallel` hints, `nologging`, and `tablespace` clauses.
    -   `grant select` statements.
-   **Target**: BigQuery SQL script, likely embedded within `project.dataset.sp_k_ausd_bp_ta_tarifoption` or as a separate BigQuery script.
    -   **Variable Handling**: `DEFINE`, `COLUMN ... NEW_VALUE` will be replaced by BigQuery `DECLARE` and `SET` statements.
    -   **Dynamic Table Naming**: `sof$ta_bpr_opt_text_&v_datum` will require dynamic SQL using `EXECUTE IMMEDIATE` with the `v_datum` variable.
    -   **DDL**: `DROP TABLE`, `CREATE TABLE` will be converted to BigQuery `DROP TABLE IF EXISTS` and `CREATE OR REPLACE TABLE AS SELECT`. `NOLOGGING`, `TABLESPACE`, `PARALLEL` hints are not applicable in BigQuery and will be removed.
    -   **Functions**:
        -   `NVL(expr, val)` -> `COALESCE(expr, val)`
        -   `TO_CHAR(date, 'YYYYMMDD')` -> `FORMAT_DATE('%Y%m%d', date_expression)`
        -   `SUBSTR`, `LTRIM`, `RTRIM`, `CASE WHEN` are directly supported in BigQuery.
        -   `LEAD(cntrct_id, 1, -1) OVER (ORDER BY NULL)` will be adapted to `LEAD(cntrct_id, 1, -1) OVER (ORDER BY cntrct_id, pds_description)` to provide a deterministic order for the analytic function.
        -   Custom `sof$ab_con.concatX` functions will need to be re-implemented as BigQuery UDFs (User Defined Functions) or inline logic if simple enough. This is a critical component for review.
    -   **Join Syntax**: Oracle implicit comma joins will be converted to explicit `INNER JOIN` statements.
    -   **Permissions**: `GRANT SELECT` statements will be handled via BigQuery IAM roles or dataset-level permissions, not within the SQL script.

## 6. External Dependencies

| Original System / Object | Type | How Replaced in BigQuery |
|---|---|---|
| UC4 Scheduler | Orchestrator | Airflow DAG on Cloud Composer |
| Oracle Database | Database | BigQuery native tables |
| `isbert_schema.dwtk_meldungen` | Oracle Table | BigQuery table `project.dataset.dwtk_meldungen` |
| `isbert_schema.sof$ta_l_bpr_optionen_filter` | Oracle Table | BigQuery table `project.dataset.sof_ta_l_bpr_optionen_filter` |
| `sof$ta_bpr_opt_text_&v_datum` | Oracle Table (dynamic) | BigQuery table `project.dataset.sof_ta_bpr_opt_text_YYYYMMDD` (resolved dynamically) |
| `$HOME/.dw_init` (env script) | Shell script | BigQuery stored procedure parameters/configuration tables or Airflow variables |
| Shell helper scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_date.ksh`) | Shell script | BigQuery scripting, audit tables, and standard BigQuery date functions |
| `gestern.ksh` (date utility) | Shell script | BigQuery `CURRENT_DATE()` and `DATE_SUB()` |
| Custom `sof$ab_con.concatX` functions | Oracle Package/Function | BigQuery UDFs or equivalent inline SQL logic |
| SQL*Plus commands (`WHENEVER SQLERROR`, `spool`, `start`, `prompt`) | Oracle Client | BigQuery scripting error handling (`EXCEPTION WHEN ERROR`), logging to audit tables, and direct BigQuery DDL/DML execution |
| Temporary file `$DW_DIR_UTL/bert_k_ausd_bp_ta_tarifoption.tmp` | File | BigQuery `DECLARE` variable or `OUT` parameter in stored procedures |

## 7. Unresolved / Risks

-   **Custom Oracle Functions**: The `sof$ab_con.concatX` functions are critical. Their exact logic needs to be fully understood and accurately re-implemented as BigQuery UDFs to ensure data integrity and functional equivalence. Without the source for these, this is a significant risk.
-   **Dynamic Table Naming**: The `sof$ta_bpr_opt_text_&v_datum` requires dynamic SQL in BigQuery. While feasible with `EXECUTE IMMEDIATE`, it adds complexity and needs careful testing.
-   **Airflow Schedule**: The UC4 job's schedule was not available in the provided metadata. This will need to be determined from external sources or business requirements during the build phase.
-   **Oracle `trace.sql.cfg`**: The `start ../trace.sql.cfg` command is present in `d_ausd_bp_ta_tarifoption.sql`. If this contains critical logging or tracing functionality, it needs to be captured and replicated, potentially via BigQuery audit tables or Cloud Logging.
-   **Uncommented `FOSJobDeaktivate` and `FOSJobErzeugeEintrag`**: The commented-out job management calls in the ksh scripts (`AL??`) suggest potential missing functionality. It needs to be clarified if these functionalities are truly deprecated or need to be re-implemented in BigQuery for job status tracking.
-   **BigQuery Cost Management**: Conversion of parallel hints to BigQuery will rely on BigQuery's auto-scaling and slot allocation. Performance tuning may be required.
-   **Data Volume**: The design assumes data volumes are manageable for BigQuery's standard pricing and performance. Large data volumes might necessitate partitioning or clustering strategies for the new BigQuery tables.

## 8. Build Plan

1.  **BigQuery Schema Creation**:
    *   Create BigQuery dataset: `project.dataset`.
    *   Create `project.dataset.dwtk_meldungen`, `project.dataset.sof_ta_l_bpr_optionen_filter` and other necessary source tables, ensuring correct data types.
    *   Create `project.dataset.job_audit_log` table for logging.
    *   Create placeholder tables for `sof_ta_bpr_opt_text_YYYYMMDD` with appropriate schema.
    *   Define target tables `project.dataset.sof_ta_bpr_opt_filter` and `project.dataset.sof_ta_tarifoption`.

2.  **UDF Development**:
    *   Analyze the exact logic of Oracle's `sof$ab_con.concatX` functions.
    *   Develop equivalent BigQuery JavaScript UDFs or inline SQL logic for these concatenation operations.

3.  **SQL Script Conversion (`d_ausd_bp_ta_tarifoption.sql`)**:
    *   Convert `d_ausd_bp_ta_tarifoption.sql` into a BigQuery SQL script (`d_ausd_bp_ta_tarifoption_bq.sql`).
    *   Handle variable substitutions (`v_datum`).
    *   Replace Oracle-specific syntax with BigQuery equivalents.
    *   Incorporate the developed UDFs for string concatenations.

4.  **KornShell Script Conversion (`k_ausd_bp_ta_tarifoption.ksh`)**:
    *   Develop a BigQuery Stored Procedure `project.dataset.sp_k_ausd_bp_ta_tarifoption` that:
        *   Accepts parameters `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`.
        *   Implements parameter validation and date defaulting.
        *   Executes `d_ausd_bp_ta_tarifoption_bq.sql` (either inline or via dynamic SQL).
        *   Manages record counts (`v_records`).
        *   Includes BigQuery error handling.

5.  **KornShell Script Conversion (`r_ausd_bp_ta_tarifoption.ksh`)**:
    *   Develop a BigQuery Stored Procedure `project.dataset.sp_bereitstellung_basisprodukte_bert` that:
        *   Accepts `p_stichtag` and `p_wiederanlaufWert`.
        *   Implements the job-level logging and audit.
        *   Calls `project.dataset.sp_k_ausd_bp_ta_tarifoption` with appropriate parameters.
        *   Handles overall job status updates.

6.  **Airflow DAG Development**:
    *   Create an Airflow DAG `dw_bert_ausd_bp_ta_tarifoption.py`.
    *   Define `schedule_interval` (TBD).
    *   Include a `BigQueryOperator` or `DataprocSubmitJobOperator` (if Python wrapper is preferred) to call `project.dataset.sp_bereitstellung_basisprodukte_bert`.
    *   Configure `default_args` including retries and owner.
    *   Implement basic Airflow logging and error notification (e.g., Slack/email).

7.  **Testing**:
    *   Unit test each BigQuery stored procedure and UDF.
    *   Integration test the entire BigQuery SQL script execution flow.
    *   End-to-end test the Airflow DAG with the BigQuery components, verifying data output and logging.

8.  **Deployment**:
    *   Deploy BigQuery tables, UDFs, and stored procedures.
    *   Deploy the Airflow DAG to Cloud Composer.