# Migration Design — DW.BERT_AUSD_V_TA_VERTRAG_TMP

## 1. Purpose & Scope
This job, `DW.BERT_AUSD_V_TA_VERTRAG_TMP`, is responsible for combining contract-related information from various source systems into a temporary contract table (`sof$ta_vertrag_tmp`). This process involves orchestration via UC4, wrapper and control shell scripts, and an Oracle PL/SQL script for data extraction and transformation.

The scope of this migration is to re-implement this entire ETL workflow on Google Cloud Platform, targeting BigQuery for data storage and transformation, and Airflow for orchestration.

## 2. Source Inventory
The job is composed of four primary source files:

*   **vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_VERTRAG_TMP.xml**
    *   **Technology**: UC4 XML (UC4/Automic)
    *   **Tier**: Medium
    *   **Automation Bucket**: Semi-Auto
    *   **Purpose**: Orchestration - defines the overall job execution, invoking a KornShell script.

*   **vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/nach_122_bert_fix_20120711/d_ausd_v_ta_vertrag_tmp.sql**
    *   **Technology**: Oracle PL/SQL
    *   **Tier**: Medium
    *   **Automation Bucket**: Semi-Auto
    *   **Purpose**: ETL - populates a temporary contract table (`sof$ta_vertrag_tmp`) by selecting and transforming data from various source tables and views.

*   **vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh**
    *   **Technology**: KornShell
    *   **Tier**: Medium
    *   **Automation Bucket**: Retire
    *   **Purpose**: Control Script - handles environment setup, parameter parsing, error management, and orchestrates the execution of the SQL script.

*   **vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh**
    *   **Technology**: KornShell
    *   **Tier**: Medium
    *   **Automation Bucket**: Semi-Auto
    *   **Purpose**: Wrapper Script - acts as a wrapper for the contract data reconciliation process, managing parameters, environment, error trapping, and invoking the core control script (`k_ausd_v_ta_vertrag_tmp.ksh`).

## 3. Target Architecture
The target architecture will leverage Google Cloud Platform services:
*   **Orchestration**: Apache Airflow (managed by Cloud Composer) to manage the end-to-end workflow.
*   **Data Storage & Transformation**: Google BigQuery for the temporary contract table and all source data tables.
*   **Script Execution**: Python tasks within Airflow for any remaining procedural logic from the KornShell scripts.

## 4. Data Flow & Lineage
The original job flow is:
1.  **UC4 Job**: `DW.BERT_AUSD_V_TA_VERTRAG_TMP.xml` initiates the process.
2.  **UC4 invokes**: `r_ausd_v_ta_vertrag_tmp.ksh`.
3.  **`r_ausd_v_ta_vertrag_tmp.ksh` invokes**: `k_ausd_v_ta_vertrag_tmp.ksh`.
4.  **`k_ausd_v_ta_vertrag_tmp.ksh` executes**: `d_ausd_v_ta_vertrag_tmp.sql`.
5.  **`d_ausd_v_ta_vertrag_tmp.sql`**: Reads from various Oracle tables/views (e.g., `sof$ta_cntrct_crs3`, `sof$ta_bp_ref`, `sof$ta_inv_acc`, `dwh$vi_s_rd_segment`, `sof$ta_notice`, `sof$ta_barrier_zusgf`, `sof$ta_cntrct_templ`, `sof$ta_cntrct_valid`, `sof$ta_period`, `sof$ta_vvl_upgrade`, `sof$ta_apn_ve`, `sof$ta_action_assoc`, `sof$vi_c_bfc`, `isbert_schema.dwtk_meldungen`) and inserts into a temporary table `sof$ta_vertrag_tmp`.

**Target Data Flow:**
1.  **Airflow DAG**: The UC4 job will be translated into an Airflow DAG.
2.  **Airflow Task 1 (PythonOperator)**: Replaces `r_ausd_v_ta_vertrag_tmp.ksh` and `k_ausd_v_ta_vertrag_tmp.ksh`. This task will handle environment setup, parameter passing (from Airflow configurations), and error handling. It will then trigger the BigQuery SQL task.
3.  **Airflow Task 2 (BigQueryOperator)**: Executes the migrated BigQuery SQL script (`d_ausd_v_ta_vertrag_tmp.sql`). This task will:
    *   Truncate/clear the target BigQuery temporary table (equivalent to `TRUNCATE TABLE sof$ta_vertrag_tmp`).
    *   Execute the `INSERT INTO` or `MERGE INTO` statement to populate the BigQuery temporary table (`<target_dataset>.ta_vertrag_tmp`).

## 5. Transformation Logic

*   **UC4 XML (`DW.BERT_AUSD_V_TA_VERTRAG_TMP.xml`)**:
    *   **Legacy**: Defines a UNIX job that executes a KornShell script. It includes variable definitions (`DWH_JOB_KENNUNG`) and includes other scripts (`DW.HOLE_PFAD`, `DW.BERT_LESE_LOG`).
    *   **Target**: Replaced by an Airflow DAG. The DAG will define the sequence of tasks, parameter passing, and logging, subsuming the functionality of the UC4 job and the wrapper shell scripts.

*   **KornShell Scripts (`r_ausd_v_ta_vertrag_tmp.ksh`, `k_ausd_v_ta_vertrag_tmp.ksh`)**:
    *   **Legacy**: Handle environment sourcing (`. $HOME/.dw_init`), error handling (`f_alis_msgerr.ksh`), parameter parsing (`h_alis_parameter.ksh`), date utilities (`h_alis_date.ksh`), and SQL execution (`h_alis_sqlplus.ksh`). They orchestrate the call to the Oracle PL/SQL script.
    *   **Target**: The functionality of these scripts will be re-implemented as Python functions or operators within the Airflow DAG.
        *   Environment setup will be managed through Airflow variables or environment variables in the Cloud Composer environment.
        *   Parameter parsing will be handled by Airflow task parameters.
        *   Error handling will leverage Airflow's native retry mechanisms and alerting.
        *   SQL execution will be handled by the BigQueryOperator, passing the transformed SQL script.
        *   The `k_ausd_v_ta_vertrag_tmp.ksh` script is in the "retire" bucket, suggesting its core logic might be directly absorbed into the Python operator or BigQuery SQL.

*   **Oracle PL/SQL (`d_ausd_v_ta_vertrag_tmp.sql`)**:
    *   **Legacy**:
        *   Sets SQL*Plus environment variables (`SET TIMING ON`, `SET SERVEROUTPUT ON`, `WHENEVER SQLERROR EXIT FAILURE`).
        *   Defines a DB link `v_carmen = "@pcrs1"`.
        *   Determines a `v_datum` from `isbert_schema.dwtk_meldungen`.
        *   Truncates `sof$ta_vertrag_tmp`.
        *   Performs a `UNION ALL` query to `INSERT INTO sof$ta_vertrag_tmp` from numerous source tables with complex `JOIN` and `DECODE`/`CASE` logic. Includes Oracle-specific hints (`/*+ parallel(...) full(...) */`).
    *   **Target**:
        *   The SQL*Plus commands will be removed or translated to BigQuery best practices (e.g., using `TRUNCATE TABLE` directly in a BigQuery script).
        *   The DB link `v_carmen` will be replaced by direct BigQuery table references if the `CARMEN` database is also migrated to BigQuery, or by external tables/data transfers if `CARMEN` remains external.
        *   Date variable derivation (`v_datum`) will be converted to a BigQuery-compatible subquery or derived via Airflow parameters.
        *   The `INSERT INTO` statement will be converted to BigQuery Standard SQL, replacing Oracle-specific functions (`NVL`, `TO_DATE`, `MONTHS_BETWEEN`, `DECODE`) with their BigQuery equivalents (`COALESCE`, `PARSE_DATE`, `DATE_DIFF`, `CASE`).
        *   Oracle `/*+ parallel(...) */` hints will be removed as BigQuery automatically handles query optimization and parallelism.
        *   All source tables (`sof$ta_cntrct_crs3`, `sof$ta_bp_ref`, etc.) and views (`dwh$vi_s_rd_segment`, `sof$vi_c_bfc`) must be migrated to BigQuery and accessible under a specific dataset (e.g., `<source_dataset>.<table_name>`).

## 6. External Dependencies
*   **Oracle Database (CARMEN DB)**: The Oracle PL/SQL script explicitly references a DB-Link `v_carmen = "@pcrs1"` and reads from tables and views within an Oracle database.
    *   **Replacement**: The `CARMEN` database will need to be migrated to BigQuery as well, or a data ingestion pipeline (e.g., Data Migration Service, Fivetran, Change Data Capture) must be established to replicate the necessary tables and views into BigQuery before this job can run. The tables will then be referenced directly in BigQuery SQL.
*   **Filesystem (`$HOME`, `${BERT_DIR_ROOT}`, `$DW_DIR_UTL`)**: The KornShell scripts rely heavily on a specific filesystem structure for sourcing environment variables and utility scripts, and for spooling temporary files.
    *   **Replacement**: This dependency will be removed. Environment variables will be managed by Airflow. Utility functions will be reimplemented in Python as part of the Airflow DAG. Temporary files will either be eliminated, managed within Airflow's temporary storage, or written directly to BigQuery tables.
*   **UC4 Scheduler**: The job is currently scheduled and managed by UC4.
    *   **Replacement**: Apache Airflow will take over scheduling and monitoring of the job.

## 7. Unresolved / Risks
*   **No Lineage Edges found**: The initial `lineage_edges` query returned no direct dependency edges. The execution flow is inferred from script content. While this seems clear, any hidden or dynamic dependencies not captured by static analysis could pose a risk during migration.
*   **Oracle PL/SQL Complexities**: The PL/SQL script contains several Oracle-specific functions (`DECODE`, `MONTHS_BETWEEN`, `NVL`, `TO_DATE`) and hints. Direct translation requires careful testing.
*   **Shell Script Logic**: The KornShell scripts contain logic for parameter handling, logging, and error management. Reimplementing this in Python within Airflow requires careful consideration to ensure all original functionality is preserved. The "retire" bucket for `k_ausd_v_ta_vertrag_tmp.ksh` suggests some logic might not need a direct 1:1 translation.
*   **DB-Link Management**: The use of a DB-Link (`@pcrs1`) indicates potential cross-database queries. The migration strategy for the `CARMEN` database (source of these cross-database queries) is critical and should be clearly defined.
*   **Dynamic SQL/Parameters**: The Oracle SQL uses a substitution variable `&v_datum`. This dynamic parameterization needs to be handled appropriately in BigQuery, possibly via Airflow template fields or Python parameter injection.

## 8. Build Plan
1.  **Migrate Oracle Source Data to BigQuery**:
    *   Identify all Oracle source tables and views (e.g., `sof$ta_cntrct_crs3`, `sof$ta_bp_ref`, `sof$ta_inv_acc`, `dwh$vi_s_rd_segment`, `sof$ta_notice`, `sof$ta_barrier_zusgf`, `sof$ta_cntrct_templ`, `sof$ta_cntrct_valid`, `sof$ta_period`, `sof$ta_vvl_upgrade`, `sof$ta_apn_ve`, `sof$ta_action_assoc`, `sof$vi_c_bfc`, `isbert_schema.dwtk_meldungen`) and ingest them into a BigQuery dataset (e.g., `raw_oracle`).
    *   Establish ongoing data replication for these tables.

2.  **Create Target BigQuery Table**:
    *   Define the schema for the target temporary table `ta_vertrag_tmp` in BigQuery, mirroring the output of the Oracle script.
    *   Language: DDL (BigQuery Standard SQL).

3.  **Transform Oracle SQL to BigQuery SQL**:
    *   Translate `d_ausd_v_ta_vertrag_tmp.sql` into BigQuery Standard SQL, replacing Oracle-specific syntax and functions.
    *   Handle the `v_datum` substitution variable.
    *   Language: BigQuery Standard SQL.
    *   Output: `bq_d_ausd_v_ta_vertrag_tmp.sql`

4.  **Develop Airflow DAG**:
    *   Create an Airflow DAG that replicates the orchestration logic of the UC4 job and the KornShell scripts.
    *   **Task 1 (PythonOperator)**: Implement the logic from `r_ausd_v_ta_vertrag_tmp.ksh` and `k_ausd_v_ta_vertrag_tmp.ksh`. This task will include:
        *   Fetching necessary parameters (e.g., `v_datum`).
        *   Setting up environment variables for subsequent tasks if needed.
        *   Error handling and logging.
    *   **Task 2 (BigQueryOperator)**: Execute the `bq_d_ausd_v_ta_vertrag_tmp.sql` script.
    *   Language: Python (for DAG definition and PythonOperators), BigQuery Standard SQL (for BigQueryOperator).
    *   Output: `dw_bert_ausd_v_ta_vertrag_tmp_dag.py`

5.  **Testing and Validation**:
    *   Implement comprehensive unit and integration tests for the migrated BigQuery SQL and the Airflow DAG.
    *   Perform data validation to ensure the BigQuery output matches the legacy Oracle output.
    *   Language: Python (for Airflow tests), BigQuery Standard SQL (for data validation queries).