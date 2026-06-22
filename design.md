# Migration Design — DW.BERT_AUSD_V_TA_CNTRCT_TEMPL

## 1. Purpose & Scope
This document outlines the migration design for the ETL job `DW.BERT_AUSD_V_TA_CNTRCT_TEMPL`. The primary objective is to modernize an existing ETL pipeline, comprising UC4 orchestration, KornShell control scripts, and Oracle SQL transformation logic, to Google Cloud Platform (GCP). The target architecture will utilize Apache Airflow for workflow orchestration and BigQuery for data storage and SQL-based transformations. The migration aims to preserve existing business logic, execution order, data lineage, operational behavior, and output datasets, while leveraging cloud-native capabilities for improved scalability, maintainability, and auditability.

The job's business purpose is to mirror Carmen contract templates. It extracts contract template data from a Carmen source system, processes it with date-based filtering and joins, and loads it into a target table (`sof$ta_cntrct_templ`).

## 2. Source Inventory
The ETL job `DW.BERT_AUSD_V_TA_CNTRCT_TEMPL` is composed of the following files:

1.  **`vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_CNTRCT_TEMPL.xml`**
    *   **Technology:** UC4/Automic Job Definition (XML)
    *   **Summary:** Defines a UC4 UNIX job named DW.BERT_AUSD_V_TA_CNTRCT_TEMPL that executes a KornShell script to mirror Carmen contract templates. This is the top-level orchestrator.
    *   **Complexity Tier:** medium
    *   **Automation Bucket:** semi_auto

2.  **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_templ.ksh`**
    *   **Technology:** KornShell Script
    *   **Summary:** This KornShell script serves as a wrapper for the contract data reconciliation process for the `ta_cntrct_templ` table. It handles environment setup, parameter parsing, and error logging before invoking a core processing script.
    *   **Complexity Tier:** medium
    *   **Automation Bucket:** semi_auto

3.  **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_templ.ksh`**
    *   **Technology:** KornShell Script
    *   **Summary:** This is a control script that orchestrates the execution of an SQL script (`d_ausd_v_ta_cntrct_templ.sql`) which likely updates the `ta_cntrct_templ` table. It performs environment setup, parameter parsing, and error handling.
    *   **Complexity Tier:** medium
    *   **Automation Bucket:** semi_auto

4.  **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_cntrct_templ.sql`**
    *   **Technology:** Oracle SQL*Plus Script
    *   **Summary:** This SQL*Plus script truncates a target table (`sof$ta_cntrct_templ`) and then populates it by selecting and joining data from source tables (`cds$ta_cntrct_template`, `cds$ta_care_description`), applying date-based filtering. It determines a processing date from a metadata table (`isbert_schema.dwtk_meldungen`).
    *   **Complexity Tier:** medium
    *   **Automation Bucket:** semi_auto

## 3. Target Architecture
The migrated ETL job will be re-architected on Google Cloud Platform, utilizing the following core components:

*   **Orchestration:** **Apache Airflow** (managed Airflow on Cloud Composer) will replace the UC4 job scheduler and KornShell wrapper/control scripts. A single Airflow DAG will manage the end-to-end workflow, encompassing data extraction, transformation, and loading.
*   **Data Storage & Transformation:** **BigQuery** will be the primary data warehouse for all transformed data. All Oracle tables will be migrated to BigQuery datasets and tables. The SQL transformation logic will be rewritten in BigQuery Standard SQL.
*   **Data Staging:** **Cloud Storage** will be used for temporary storage of any intermediate files or for initial landing of extracted data if direct BigQuery ingestion is not feasible/optimal. SQL templates and configuration files may also be stored here.
*   **Security:** **Google Secret Manager** for storing sensitive credentials (e.g., for Carmen DB access), and **IAM** for managing granular access permissions to GCP resources.
*   **Monitoring & Logging:** Integrated with **Cloud Logging** and **Cloud Monitoring** for centralized log collection, metrics, and alerting.

The proposed BigQuery table categories include:
*   `control.etl_job_run`: For tracking job execution.
*   `control.etl_watermark`: For managing incremental data loads.
*   `staging.source_data_stg`: For raw or lightly transformed source data.
*   `intermediate.transformed_data_int`: For business-rule-specific transformation outputs.
*   `curated.final_fact_table`: For reporting-ready or downstream consumption tables (corresponds to `sof$ta_cntrct_templ`).
*   `audit.etl_validation_results`: For storing data quality validation outcomes.

## 4. Data Flow & Lineage
The end-to-end data flow and execution lineage will be as follows:

1.  **Airflow DAG Initiation:** The Airflow DAG, replacing the UC4 job, initiates the ETL process based on its defined schedule.
2.  **Environment Setup & Parameter Management (Python/Airflow Tasks):** Airflow tasks will handle environment variable loading, parameter parsing, and error handling, replacing the `r_ausd_v_ta_cntrct_templ.ksh` and `k_ausd_v_ta_cntrct_templ.ksh` scripts. This includes dynamically determining the processing date (`v_datum`) from a BigQuery equivalent of `isbert_schema.dwtk_meldungen`.
3.  **Data Extraction (GCP Services):**
    *   Data from `cds$ta_cntrct_template` and `cds$ta_care_description` (currently in the Carmen DB via Oracle DB link) will be extracted. This may involve:
        *   Setting up secure connectivity (e.g., Cloud VPN/Interconnect, Private Service Connect) to the Carmen DB.
        *   Using a data transfer service (e.g., Cloud Data Fusion, Dataflow, custom Python script with `apache-airflow-providers-google` BigQuery operators or database hooks) to pull data from the Carmen DB.
    *   The `isbert_schema.dwtk_meldungen` table (now in BigQuery) will be queried to get the `v_datum`.
4.  **Staging in BigQuery:** Extracted raw data from Carmen DB will be loaded into temporary staging tables in BigQuery (e.g., `staging.source_data_stg`).
5.  **Data Transformation (BigQuery SQL):** BigQuery SQL scripts, derived from `d_ausd_v_ta_cntrct_templ.sql`, will perform the core data transformations. This involves:
    *   Truncating/overwriting the target table (e.g., `curated.final_fact_table` which maps to `sof$ta_cntrct_templ`).
    *   Joining the staged `cds$ta_cntrct_template` and `cds$ta_care_description` data.
    *   Applying filtering logic based on `insert_at`, `modified_at`, `valid_from`, `valid_to`, `is_production`, and `language` using the dynamically determined `v_datum`.
    *   Loading the transformed data into the final target table.
6.  **Post-Processing & Auditing (Airflow Tasks & BigQuery):**
    *   Update `control.etl_watermark` with the latest processing date.
    *   Record job execution status in `control.etl_job_run`.
    *   Perform data quality checks and store results in `audit.etl_validation_results`.
    *   Send notifications on success/failure.

## 5. Transformation Logic
The core transformation logic, currently implemented in `d_ausd_v_ta_cntrct_templ.sql`, will be translated into BigQuery Standard SQL.

**Original Oracle SQL Logic:**

1.  **Determine Processing Date (`v_datum`):**
    ```sql
    COLUMN s_datum new_value v_datum noprint
    SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum
    FROM isbert_schema.dwtk_meldungen m
    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
    ```
    *   **Migration:** This will be converted to a BigQuery SQL query against the migrated `isbert_schema.dwtk_meldungen` table to determine the processing date. This date will likely be passed as an Airflow DAG parameter or derived within a Python task.

2.  **Truncate Target Table:**
    ```sql
    begin
    isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_cntrct_templ');
    end;
    /
    ```
    *   **Migration:** This will be replaced by `TRUNCATE TABLE \`project.curated.final_fact_table\`` or `CREATE OR REPLACE TABLE` in BigQuery SQL, where `final_fact_table` is the new name for `sof$ta_cntrct_templ`.

3.  **Populate Target Table:**
    ```sql
    INSERT  INTO sof$ta_cntrct_templ
    (CNTRCT_TEMPLATE_ID,
      CDS_DESCRIPTION_ID,
      CDS_DESCRIPTION )
    SELECT
            ct.cntrct_template_id,
            ct.cds_description_id,
            cd.cds_description
    FROM
            cds$ta_cntrct_template     &v_carmen     ct,
            cds$ta_care_description    &v_carmen     cd
    WHERE
            ct.cds_description_id    = cd.cds_description_id
    AND
            ct.insert_at <= TO_DATE('&v_datum','YYYYMMDD')
    AND     (   ct.modified_at IS NULL
             OR ct.modified_at > TO_DATE('&v_datum','YYYYMMDD')     )
    AND     ct.valid_from <= TO_DATE('&v_datum','YYYYMMDD')
    AND     (   ct.valid_to IS NULL
             OR ct.valid_to > TO_DATE('&v_datum','YYYYMMDD')       )
    AND     ct.is_production = 1
    AND     cd.language = 1;
    ```
    *   **Migration:** This `INSERT...SELECT` statement will be converted to BigQuery Standard SQL, joining the migrated `cds$ta_cntrct_template` and `cds$ta_care_description` tables (potentially in a staging area). The `TO_DATE` function will be replaced by BigQuery's date functions, and the `&v_datum` bind variable will be replaced by an Airflow templated parameter or a subquery.

**Business Rules to Preserve:**
*   **Execution Order:** Maintained by Airflow DAG task dependencies.
*   **Watermark-based Processing:** The `v_datum` logic ensures only relevant data up to a specific date is processed. This will be adapted for BigQuery and managed by Airflow.
*   **Data Filtering:** All `WHERE` clause conditions related to dates (`insert_at`, `modified_at`, `valid_from`, `valid_to`), `is_production`, and `language` must be accurately replicated.

## 6. External Dependencies
The current ETL job has the following external dependencies:

*   **Carmen Database:** This is a critical external system referenced via an Oracle DB Link (`@pcrs1`). It provides source tables `cds$ta_cntrct_template` and `cds$ta_care_description`.
    *   **Replacement Strategy:**
        *   **Secure Connectivity:** Establish secure, private network connectivity between GCP and the Carmen Database (e.g., Cloud VPN, Cloud Interconnect, Private Service Connect).
        *   **Data Ingestion:** Implement a robust data ingestion mechanism using GCP services like:
            *   **Cloud Data Fusion:** For ETL pipelines from Oracle to BigQuery.
            *   **Dataflow:** For custom, scalable data pipelines.
            *   **Custom Python/Airflow Operators:** Using `apache-airflow-providers-google` to connect directly to Oracle and ingest data into BigQuery staging tables.
*   **Oracle Database for `isbert_schema.dwtk_meldungen` and `sof$ta_cntrct_templ`:** These are internal Oracle tables.
    *   **Replacement Strategy:**
        *   These tables will be migrated directly to BigQuery datasets and tables. `isbert_schema.dwtk_meldungen` will likely become a control or metadata table in BigQuery, and `sof$ta_cntrct_templ` will become the `curated.final_fact_table`.

## 7. Unresolved / Risks
*   **Missing Lineage Details:** The automated lineage analysis (`lineage_edges` query) did not return direct relationships between files. The execution flow and data flow were inferred manually from file content and summaries. This indicates a potential gap in automated discovery that needs to be manually verified during implementation.
*   **Oracle DB Link (`@pcrs1`) to Carmen:** This is a significant external dependency. The specifics of connecting to and extracting data from the Carmen database (e.g., authentication, data volume, network latency) need detailed investigation and planning.
*   **KornShell Utilities:** The KornShell scripts (`r_ausd_v_ta_cntrct_templ.ksh`, `k_ausd_v_ta_cntrct_templ.ksh`) rely on several custom utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`). These utilities will need to be re-implemented in Python as Airflow tasks or helper functions to maintain equivalent functionality (e.g., error handling, logging, date calculations).
*   **SQL*Plus Specific Features:** The `d_ausd_v_ta_cntrct_templ.sql` script uses SQL*Plus specific commands (`DEFINE`, `COLUMN new_value`, `START`, `SPOOL`, `WHENEVER SQLERROR`). These will need to be removed or replaced with Airflow's error handling, logging, and templating capabilities.
*   **Data Volume and Performance:** The current performance of the Oracle SQL query and the volume of data processed are unknown. This needs to be assessed to ensure the BigQuery migration meets performance requirements and cost efficiency.
*   **Error Handling and Logging:** The legacy shell scripts implement custom error handling and logging (`DWMSG_MeldeFehler`, `DWMSG_ErzeugeEintrag`). These will need to be accurately translated to Airflow's logging and alerting mechanisms, potentially integrating with Cloud Logging and Cloud Monitoring.

## 8. Build Plan
The migration will proceed in the following ordered steps, generating new artifacts in Python and BigQuery SQL:

1.  **Define BigQuery Schema for Source & Target Tables (BigQuery SQL DDL):**
    *   Create `project.control.etl_job_run`
    *   Create `project.control.etl_watermark`
    *   Create `project.staging.cds_ta_cntrct_template_stg` (for `cds$ta_cntrct_template`)
    *   Create `project.staging.cds_ta_care_description_stg` (for `cds$ta_care_description`)
    *   Create `project.metadata.dwtk_meldungen` (for `isbert_schema.dwtk_meldungen`)
    *   Create `project.curated.final_fact_table` (for `sof$ta_cntrct_templ`)
    *   Create `project.audit.etl_validation_results`

2.  **Develop Data Extraction/Ingestion Mechanism from Carmen DB (Python/Airflow Task):**
    *   Python script/Airflow operator to connect to the Carmen Oracle DB.
    *   Extract `cds$ta_cntrct_template` and `cds$ta_care_description` data.
    *   Load extracted data into `project.staging.cds_ta_cntrct_template_stg` and `project.staging.cds_ta_care_description_stg` in BigQuery.

3.  **Implement Utility Functions (Python):**
    *   Python functions replacing `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh` for error handling, date calculations, parameter parsing, and SQL execution.

4.  **Translate SQL Transformation Logic (BigQuery SQL):**
    *   Rewrite `d_ausd_v_ta_cntrct_templ.sql` into BigQuery Standard SQL, performing:
        *   Dynamic `v_datum` determination (as a subquery or parameter).
        *   Truncate/`CREATE OR REPLACE` for `project.curated.final_fact_table`.
        *   `INSERT INTO` with `SELECT` from staged Carmen tables, applying all original join and filter conditions.

5.  **Develop Airflow DAG (Python):**
    *   Create a new Airflow DAG (e.g., `dw_bert_ausd_v_ta_cntrct_templ_dag.py`) in Python.
    *   Define tasks for:
        *   Initial environment setup and parameter retrieval.
        *   Data ingestion from Carmen DB to BigQuery staging.
        *   SQL transformations using `BigQueryOperator` or `PythonOperator` with BigQuery client.
        *   Update control tables (`etl_job_run`, `etl_watermark`).
        *   Data quality checks and validation (`etl_validation_results`).
        *   Notification tasks (e.g., email alerts).
    *   Establish task dependencies to match the original execution flow.

6.  **Configuration Management (YAML/JSON):**
    *   Externalize configurations (e.g., project IDs, dataset names, connection details) into a separate configuration file.

7.  **Testing and Validation (Python/BigQuery SQL):**
    *   Develop unit tests for Python functions and Airflow tasks.
    *   Write integration tests to validate data integrity and transformation accuracy in BigQuery.
    *   Implement data reconciliation queries to compare migrated output with legacy output.