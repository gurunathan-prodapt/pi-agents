# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh

## 1. Purpose & Scope
This migration design document outlines the strategy for re-platforming the legacy ETL job identified by `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh` to Google Cloud Platform, specifically utilizing BigQuery as the target data warehouse.

The original job serves as a control script (`Kontrollscript`) responsible for orchestrating a data preparation task. Its primary functions include:
*   Parsing and validating input parameters (`JobKennung`, `EintragsNr`, `Stichtag`, `wiederanlaufWert`).
*   Performing date format checks.
*   Deriving current and previous day values.
*   Executing a core Oracle SQL*Plus script, `d_ausd_geschaeftspartner.sql`, passing several parameters to it.
*   Capturing the record count resulting from the SQL script's execution.
*   Conceptually, it also manages job-table entries for tracking, though some of this functionality is commented out in the source.

The core business logic, including data extraction, transformation, and loading (ETL), resides within the `d_ausd_geschaeftspartner.sql` script. This script involves significant Oracle SQL constructs, including data manipulation (INSERT statements), joins across multiple tables, and the use of Oracle-specific functions and database links.

The scope of this migration is to fully replicate the functionality of both the KornShell orchestrator and the Oracle SQL*Plus ETL script in a BigQuery-native environment, leveraging BigQuery Stored Procedures for the transformation logic and Cloud Composer (Airflow) for job orchestration.

## 2. Source Inventory

**File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh`
*   **Technology:** KornShell (ksh)
*   **Tier:** Medium
*   **Automation Bucket:** Semi-Automatic
*   **Purpose:** Orchestration, parameter validation, environment setup, and execution of the core SQL script.

**File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_geschaeftspartner.sql`
*   **Technology:** Oracle SQL*Plus
*   **Tier:** Complex
*   **Automation Bucket:** Manual
*   **Purpose:** Core ETL logic. It reads from various source tables, performs truncations of intermediate/target tables, and populates `sof$ta_segm_prem`, `sof$ta_bpr_dn_evn_his`, `sof$ta_bpr_dn_evn`, `sof$ta_p_geschaeftspartner`, `sof$ta_p_dienstenutzer`, and `sof$ta_p_evn_empf`.

## 3. Target Architecture
The target architecture will leverage Google Cloud Platform services to provide a scalable, managed, and cost-effective solution for this ETL workflow.

*   **Data Warehouse:** BigQuery will serve as the primary data warehouse for all transformed and target data.
    *   Source tables will be ingested into BigQuery as staging tables.
    *   Intermediate and final target tables (e.g., `sof$ta_segm_prem`, `sof$ta_p_gesch_part`) will reside in BigQuery datasets.
*   **Orchestration:** Cloud Composer (Apache Airflow) will manage the scheduling, execution, and monitoring of the migrated ETL job.
*   **ETL Logic:** The shell script's orchestration logic and the SQL script's data transformation logic will be re-implemented as BigQuery Stored Procedures. These procedures will encapsulate the entire workflow.
*   **External Data Ingestion:** For external Oracle sources, a dedicated ingestion pipeline (e.g., Datastream for CDC, or periodic batch loads using Dataflow/Cloud Storage) will land the data into BigQuery staging tables.

## 4. Data Flow & Lineage

The overall data flow can be summarized as:

1.  **External Source Systems (Oracle)**: `bpd$ta_bp_valueseg_assoc`, `pds$ta_bpri_com` (accessed via `@pcrs1` database link), and `isbert_schema.dwtk_meldungen`. Also, other source tables like `sof$ta_e_reach_gp`, `sof$ta_e_business_gp`, `sof$ta_e_reach_dn`, `sof$ta_e_business_dn`, `sof$ta_e_reach_ev`, `sof$ta_e_business_ev`.
2.  **Ingestion Layer (GCP)**: Data from external Oracle sources will be ingested into BigQuery staging tables.
3.  **BigQuery Orchestration Stored Procedure** (`k_ausd_geschaeftspartner_main`):
    *   Receives input parameters.
    *   Performs validation and date calculations.
    *   Invokes the core BigQuery ETL Stored Procedure (`d_ausd_geschaeftspartner_proc`).
    *   Handles job logging.
4.  **BigQuery ETL Stored Procedure** (`d_ausd_geschaeftspartner_proc`):
    *   Reads from BigQuery staging tables (migrated `isbert_schema.dwtk_meldungen`, `bpd$ta_bp_valueseg_assoc`, `pds$ta_bpri_com`, `sof$ta_e_reach_gp`, etc.).
    *   Truncates intermediate and target BigQuery tables (e.g., `sof$ta_segm_prem`, `sof$ta_bpr_dn_evn_his`, `sof$ta_bpr_dn_evn`, `sof$ta_p_gesch_part`, `sof$ta_p_dn_nutzer`, `sof$ta_p_evn_empf`).
    *   Performs complex joins and transformations.
    *   Inserts data into the intermediate tables: `sof$ta_segm_prem`, `sof$ta_bpr_dn_evn_his`, `sof$ta_bpr_dn_evn`.
    *   Inserts data into the final target tables: `sof$ta_p_gesch_part`, `sof$ta_p_dn_nutzer`, `sof$ta_p_evn_empf`.
    *   Returns the record count to the calling orchestration procedure.
5.  **BigQuery Target Tables**: `sof$ta_p_gesch_part`, `sof$ta_p_dn_nutzer`, `sof$ta_p_evn_empf` are the final outputs.

The execution order will be sequential, mirroring the steps in the original KornShell and SQL scripts, but within BigQuery Stored Procedures.

## 5. Transformation Logic

### a. `k_ausd_geschaeftspartner.ksh` (Orchestration Layer)
This will be migrated into a main BigQuery Stored Procedure, e.g., `project.dataset.k_ausd_geschaeftspartner_main`.

*   **Parameter Handling:**
    *   The `getopts` logic will be replaced with direct BigQuery Stored Procedure input parameters: `p_JobKennung STRING`, `p_EintragsNr STRING`, `p_Stichtag STRING`, `p_wiederanlaufWert INT64`.
*   **Validation:**
    *   Parameter presence checks (`p_JobKennung`, `p_Stichtag`, `p_EintragsNr`) will use `IF ... IS NULL THEN ... SIGNAL SQLSTATE` or `ASSERT` statements.
    *   Date format validation (`DDMMYYYY`) will use `SAFE.PARSE_DATE('%d%m%Y', p_Stichtag)` and check for `NULL` results.
*   **Variable Initialization:**
    *   Shell variables (`v_TabName`, `tmpFile`, `p_wiederanlaufWert`) will become `DECLARE`d variables in BigQuery scripting.
*   **Date Derivation:**
    *   The external script `gestern.ksh` will be replaced by BigQuery's `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` functions for `v_datum_heute` and `v_datum_gestern`.
*   **SQL Script Execution:**
    *   The `starteSQLSkript` call will be replaced by a `CALL` statement to the migrated BigQuery ETL Stored Procedure, `project.dataset.d_ausd_geschaeftspartner_proc`, passing all required parameters.
*   **Record Count:**
    *   The temporary file (`tmpFile`) will be replaced by an `OUT` parameter from `d_ausd_geschaeftspartner_proc`, which will capture the number of records processed/inserted.
*   **Logging:**
    *   `print` and `echo` statements will be replaced by `INSERT` statements into a dedicated BigQuery job log table (e.g., `project.dataset.job_log`). Error messages via `DWMSG_MeldeFehler` will also be directed to this log table and potentially trigger `SIGNAL SQLSTATE` for robust error handling.

### b. `d_ausd_geschaeftspartner.sql` (Core ETL Layer)
This will be migrated into a BigQuery Stored Procedure, e.g., `project.dataset.d_ausd_geschaeftspartner_proc`, taking necessary input parameters (e.g., `p_EintragsNr`, `p_JobKennung`, `v_stichtag_date`, `v_restart`, `v_datum_heute`, `v_datum_gestern`) and an `OUT` parameter for `v_records`.

*   **Variable Definitions:**
    *   `DEFINE v_carmen = "@pcrs1"` will be handled by configuring external connections or ensuring the source tables from `@pcrs1` are already ingested into BigQuery.
    *   `COLUMN s_datum new_value v_datum noprint` and the `SELECT` statement deriving `v_datum` from `isbert_schema.dwtk_meldungen` will be translated into a BigQuery `DECLARE` variable assignment using a `SELECT` query on the migrated `dwtk_meldungen` table.
*   **SQL*Plus Specific Commands:**
    *   `prompt`, `start ../trace.sql.cfg`, `spool`, `WHENEVER SQLERROR CONTINUE`, `set timing on`, `DESC` will be removed or replaced by BigQuery scripting equivalents (e.g., logging to a table, using `CONTINUE HANDLER` for errors).
*   **Truncation (Step 02):**
    *   `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE ...')` will be replaced with direct `TRUNCATE TABLE` statements in BigQuery for the respective tables.
*   **Data Population (Steps 03, 04, 05, 06, 07):**
    *   All `INSERT INTO ... SELECT FROM ...` statements will be directly translated to BigQuery SQL.
    *   **Oracle-specific functions:** `NVL` will become `IFNULL` or `COALESCE`. `DECODE` will become `CASE` statements. `TO_CHAR`, `TO_DATE` will be replaced by `FORMAT_DATE`, `PARSE_DATE`.
    *   **Concatenation:** `||` will be replaced by `CONCAT`.
    *   **Joins:** Oracle join syntax (e.g., `table1 t1, table2 t2 WHERE t1.col = t2.col`) will be converted to explicit `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN` statements. The `(+)` for `LEFT OUTER JOIN` will be converted.
    *   **Parallel Hints:** Oracle `/*+ parallel(table,4) */` hints are not required in BigQuery, as BigQuery automatically parallelizes queries. These hints will be removed.
    *   `COMMIT;` statements are implicit in BigQuery DML operations within a stored procedure; explicit `COMMIT` is not needed.
*   **Intermediate Tables:** `sof$ta_segm_prem`, `sof$ta_bpr_dn_evn_his`, `sof$ta_bpr_dn_evn` will remain as intermediate tables in BigQuery.
*   **Final Targets:** `sof$ta_p_gesch_part`, `sof$ta_p_dn_nutzer`, `sof$ta_p_evn_empf` will be the final target tables in BigQuery.

## 6. External Dependencies

*   **Oracle Database Links (`@pcrs1`):**
    *   **Usage:** Used to access `bpd$ta_bp_valueseg_assoc` and `pds$ta_bpri_com` from a remote Oracle instance.
    *   **Replacement Strategy:**
        1.  **Managed Data Ingestion:** The preferred approach is to establish a robust data ingestion pipeline (e.g., using Datastream for real-time replication or Dataflow for batch loading) to bring `bpd$ta_bp_valueseg_assoc` and `pds$ta_bpri_com` into BigQuery as managed staging tables. This eliminates direct dependency on an external Oracle database link at runtime.
        2.  **BigQuery Federated Queries (Alternative/Temporary):** If direct ingestion is not immediately feasible or for ad-hoc access, BigQuery Federated Queries can be used to query the external Oracle database. This requires configuring an external connection in BigQuery. However, this option introduces latency and external system dependency into the BigQuery job, so it is generally less preferred for production ETL.
*   **Oracle PL/SQL Package (`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`):**
    *   **Usage:** Executes `TRUNCATE TABLE` commands.
    *   **Replacement Strategy:** This specific usage is simple and will be replaced by direct `TRUNCATE TABLE` DDL statements within the BigQuery Stored Procedures. If `DWPA_UTIL_SKRIPT.runstatement` had more complex logic for other use cases, that logic would need to be re-implemented as BigQuery Stored Procedures.
*   **KornShell Helper Scripts (`. $HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`):**
    *   **Usage:** Provide environment setup, error handling, date utilities, parameter parsing, SQL*Plus wrapper functions, and date derivation.
    *   **Replacement Strategy:** The functionalities provided by these scripts will be integrated directly into the BigQuery Stored Procedures.
        *   Environment variables will become BigQuery parameters or configuration table lookups.
        *   Error handling, date checks, and parameter parsing logic will be implemented using BigQuery scripting (`IF`, `CASE`, `ASSERT`, `SAFE.PARSE_DATE`, etc.).
        *   The date derivation from `gestern.ksh` will use BigQuery's native date functions.
        *   The SQL*Plus wrapper functionality (`h_alis_sqlplus.ksh` and `starteSQLSkript`) will be replaced by the direct `CALL` to the BigQuery ETL Stored Procedure.
*   **Temporary File (`$DW_DIR_UTL/bert_k_ausd_geschaeftspartner_$$.tmp`):**
    *   **Usage:** Stores the record count from the SQL script.
    *   **Replacement Strategy:** The record count will be returned as an `OUT` parameter from the BigQuery ETL Stored Procedure directly to the calling orchestration procedure. Alternatively, for complex scenarios, a temporary table or a BigQuery scripting variable can be used.

## 7. Unresolved / Risks

*   **Oracle SQL*Plus Specific Syntax:** The Oracle SQL script uses several SQL*Plus commands (`SET`, `WHENEVER SQLERROR`, `COLUMN`, `prompt`, `spool`, `start`) which are not standard SQL and have no direct BigQuery equivalents. These need to be re-evaluated for their purpose and translated to BigQuery scripting features or logging mechanisms.
*   **Oracle-specific SQL Functions:** While many Oracle SQL functions have direct BigQuery equivalents (`NVL` to `IFNULL`/`COALESCE`, `DECODE` to `CASE`), complex or custom Oracle PL/SQL functions/packages, if any are indirectly used, will require manual inspection and re-implementation in BigQuery SQL or JavaScript UDFs.
*   **Complex Transformation Logic (`d_ausd_geschaeftspartner.sql`):** The script was identified as `complex` and in the `manual` migration bucket. This indicates that the translation of its core business logic will require significant manual effort, careful testing, and validation to ensure functional equivalence and optimal performance in BigQuery.
*   **Commented-Out Functionality:** The commented-out lines for `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` indicate potentially unused or legacy job management features. A decision needs to be made whether these functionalities are still required in the BigQuery environment. If so, they need to be designed as BigQuery job control tables and procedures.
*   **Performance Optimization:** Oracle's `parallel` hints are not directly translatable to BigQuery. The migrated BigQuery SQL will need to be reviewed and optimized for BigQuery's execution model, which often involves partitioning, clustering, and efficient query structuring.
*   **Error Handling Fidelity:** The exact behavior of `WHENEVER SQLERROR CONTINUE/EXIT FAILURE` and `DWMSG_MeldeFehler` needs to be mapped to BigQuery's `BEGIN...EXCEPTION` blocks, `SIGNAL SQLSTATE`, `ASSERT`, and logging mechanisms to ensure equivalent error reporting and control flow.

## 8. Build Plan

The migration will follow these steps:

1.  **Data Ingestion Pipeline (Phase 1 - Foundation):**
    *   **Task:** Implement a reliable, scheduled (e.g., daily) data ingestion pipeline to transfer the following Oracle source tables into BigQuery staging datasets:
        *   `bpd$ta_bp_valueseg_assoc`
        *   `pds$ta_bpri_com`
        *   `isbert_schema.dwtk_meldungen`
        *   `sof$ta_e_reach_gp`
        *   `sof$ta_e_business_gp`
        *   `sof$ta_e_reach_dn`
        *   `sof$ta_e_business_dn`
        *   `sof$ta_e_reach_ev`
        *   `sof$ta_e_business_ev`
    *   **Tool/Language:** Dataflow (Python/Java), Datastream, or Cloud Storage + BigQuery Load Jobs.
    *   **Output:** BigQuery staging tables (e.g., `stg_oracle.bpd_ta_bp_valueseg_assoc`, `stg_oracle.dwtk_meldungen`).

2.  **BigQuery ETL Stored Procedure for `d_ausd_geschaeftspartner.sql` (Phase 2 - Core Logic):**
    *   **Task:** Develop and test a BigQuery Stored Procedure (`d_ausd_geschaeftspartner_proc`) that encapsulates the entire ETL logic from the `d_ausd_geschaeftspartner.sql` script.
        *   Translate all Oracle SQL to BigQuery SQL, including functions (`NVL` to `IFNULL`, `DECODE` to `CASE`), join syntax, and DDL/DML.
        *   Define input parameters for `p_EintragsNr`, `p_JobKennung`, `v_stichtag_date`, `v_restart`, `v_datum_heute`, `v_datum_gestern`.
        *   Define an `OUT` parameter for the record count (`v_records`).
        *   Implement `TRUNCATE TABLE` statements directly for intermediate and target tables.
        *   Ensure transactions (implicit in BQ DML) and error handling are robust.
    *   **Tool/Language:** BigQuery SQL (Scripting).
    *   **Output:** BigQuery Stored Procedure (`project.dataset.d_ausd_geschaeftspartner_proc`).

3.  **BigQuery Orchestration Stored Procedure for `k_ausd_geschaeftspartner.ksh` (Phase 3 - Orchestration):**
    *   **Task:** Develop and test a BigQuery Stored Procedure (`k_ausd_geschaeftspartner_main`) to replace the KornShell script's orchestration.
        *   Define input parameters matching the original shell script's arguments (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
        *   Implement parameter validation and date format checks using BigQuery scripting and `ASSERT`.
        *   Derive `v_datum_heute` and `v_datum_gestern` using BigQuery date functions.
        *   Add calls to `project.dataset.d_ausd_geschaeftspartner_proc`, passing the necessary parameters and receiving the `v_records` output.
        *   Implement logging to a BigQuery audit table for job status, start/end times, and any errors.
    *   **Tool/Language:** BigQuery SQL (Scripting).
    *   **Output:** BigQuery Stored Procedure (`project.dataset.k_ausd_geschaeftspartner_main`).

4.  **Cloud Composer (Airflow) DAG (Phase 4 - Scheduling):**
    *   **Task:** Create an Airflow DAG to schedule and execute the `k_ausd_geschaeftspartner_main` BigQuery Stored Procedure.
        *   Define a `BigQueryExecuteStoredProcedureOperator` or `BigQueryOperator` to call `k_ausd_geschaeftspartner_main`.
        *   Configure the DAG to pass required parameters to the stored procedure (e.g., via Airflow variables or context).
        *   Set up appropriate scheduling, retries, and monitoring.
    *   **Tool/Language:** Python (Airflow DAG).
    *   **Output:** Airflow DAG file deployed to Cloud Composer.

5.  **Testing and Validation (Iterative throughout Phases):**
    *   **Task:** Conduct comprehensive testing at each stage.
        *   **Unit Tests:** For individual BigQuery Stored Procedures.
        *   **Integration Tests:** Verify the interaction between `k_ausd_geschaeftspartner_main` and `d_ausd_geschaeftspartner_proc`, and with the BigQuery staging tables.
        *   **Data Validation:** Compare output data in BigQuery target tables with data produced by the legacy Oracle job to ensure functional equivalence and data integrity.
    *   **Tool/Language:** BigQuery queries, Python/JUnit for automated tests.