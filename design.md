# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh

## 1. Purpose & Scope

This job is responsible for performing an initial snapshot (Stichtags-Abzug) of address-related data from the Customer Relationship System (CRS) for business partners and invoice recipients. The output of this job serves as the foundation for further processing of invoice recipients and business partners. The overall migration aims to re-implement this ETL workflow on Google Cloud Platform, specifically using BigQuery for data storage and transformation, and likely Cloud Composer (Airflow) for orchestration.

The scope of this migration includes the conversion of two KornShell scripts that handle orchestration and parameter management, and a core Oracle SQL*Plus script that performs the actual data extraction and transformation.

## 2. Source Inventory

The assembled job consists of the following primary files:

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh`**
    *   **Technology:** KornShell
    *   **Role:** Orchestrator (Main entry point, parameter parsing, calls `k_ausd_adressen.ksh`)
    *   **Complexity Tier:** Medium
    *   **Automation Bucket:** Semi-auto
    *   **Summary:** This ksh script orchestrates the initial extraction of address data from the CRS system for business partners and billing recipients, setting up the environment, parsing parameters, and calling a core processing script.

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_adressen.ksh`**
    *   **Technology:** KornShell
    *   **Role:** Controller/Wrapper (Further parameter processing, error checking, date validation, executes `d_ausd_adressen.sql`)
    *   **Complexity Tier:** Medium
    *   **Automation Bucket:** Semi-auto
    *   **Summary:** This is a control script for 'r_ausd_adressen.ksh' that handles parameter parsing, error checking, date validation, and orchestrates the execution of an SQL script 'd_ausd_adressen.sql'. It also manages job table entries, deactivating old jobs and creating new ones, though some of this functionality is currently commented out.

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_adressen.sql`**
    *   **Technology:** Oracle SQL*Plus
    *   **Role:** Core Data Transformation (Extracts, transforms, and loads address data)
    *   **Complexity Tier:** Complex
    *   **Automation Bucket:** Manual
    *   **Summary:** This SQL*Plus script prepares and loads address-related data for various business partner roles (contract partners, invoice recipients, EVN recipients, service users, regulators) into a set of intermediate 'sof$ta_' tables. It involves reading from source 'cds$ta_', 'glv$ta_', and 'bpd$ta_' tables, performing joins, and then populating final 'sof$ta_e_' tables.

## 3. Target Architecture

The target architecture will leverage Google Cloud Platform services:

*   **Orchestration:** Apache Airflow on Cloud Composer. The shell scripts (`r_ausd_adressen.ksh`, `k_ausd_adressen.ksh`) will be re-implemented as a Python-based Airflow DAG. This DAG will handle parameter passing, error handling, and the sequential execution of BigQuery operations.
*   **Data Storage & Transformation:** Google BigQuery.
    *   **Source Data:** Oracle source tables (`cds$ta_bp_ref`, `glv$ta_country`, `bpd$ta_reachability`, etc.) will be ingested into BigQuery. This could be achieved via tools like Cloud Data Fusion, database migration services, or custom data transfer jobs, resulting in staging tables (e.g., `stg_cds_bp_ref`, `stg_glv_country`) in a dedicated BigQuery dataset.
    *   **Intermediate Tables:** The `sof$ta_` tables used in the Oracle SQL*Plus script will be re-created as temporary or intermediate tables/views in BigQuery. These will reside in a dedicated processing dataset (e.g., `temp_address_processing`).
    *   **Final Output Tables:** The `sof$ta_e_` tables will be materialized as permanent tables in a target analytical dataset (e.g., `reporting_address_data`).
*   **Error Handling & Logging:** Cloud Logging and Cloud Monitoring integrated with Airflow. The custom error handling functions from the original KornShell scripts (`f_alis_msgerr.ksh`) will be replaced by native Airflow and GCP logging mechanisms.
*   **Date Functions:** Standard BigQuery SQL functions and Airflow macros for date manipulation. The custom date utilities (`h_alis_date.ksh`, `gestern.ksh`) will be replaced.

## 4. Data Flow & Lineage

The data flow in the migrated solution will be as follows:

1.  **Trigger:** The Airflow DAG is triggered, potentially on a schedule or manually with parameters (`stichtag`, `wiederanlaufwert`).
2.  **Parameter Handling:** The DAG parses and validates input parameters, similar to how `r_ausd_adressen.ksh` and `k_ausd_adressen.ksh` handle `p_stichtag` and `p_wiederanlaufWert`.
3.  **Source Data Ingestion:**
    *   Oracle source tables (e.g., `cds$ta_bp_ref`, `glv$ta_country`, `bpd$ta_reachability`, `bpd$ta_business_partner`, `isbert_schema.dwtk_meldungen`, `cds$ta_inv_definition`) are assumed to be already present in BigQuery staging tables.
4.  **Intermediate Table Preparation (BigQuery):**
    *   The DAG orchestrates the execution of BigQuery SQL statements, mirroring the logic in `d_ausd_adressen.sql`.
    *   **Truncation:** Initial step to truncate intermediate `sof$ta_` tables (e.g., `TRUNCATE TABLE temp_address_processing.bp_ref_gp`).
    *   **`sof$ta_bp_ref_*` Population:** Data is selected from `stg_cds_bp_ref` and `stg_cds_inv_definition` and inserted into BigQuery equivalents of `sof$ta_bp_ref_gp`, `sof$ta_bp_ref_re`, `sof$ta_bp_ref_ev`, `sof$ta_bp_ref_dn`.
    *   **Country Data (`sof$ta_country`, `sof$ta_country_desc`, `sof$ta_laender_kng`):** Data is selected from `stg_glv_country` and `stg_glv_description` to populate these intermediate tables.
    *   **Reachability (`sof$ta_reachability`):** Data is selected from `stg_bpd_reachability` to populate this intermediate table.
    *   **Business Partner (`sof$ta_business_pt`):** Data is selected from `stg_bpd_business_partner` to populate this intermediate table.
5.  **Final Output Table Population (BigQuery):**
    *   **`sof$ta_e_reach_*` Population:** Joins between BigQuery equivalents of `sof$ta_bp_ref_*`, `sof$ta_reachability`, and `sof$ta_laender_kng` are performed to populate `reporting_address_data.reach_gp`, `reporting_address_data.reach_re`, `reporting_address_data.reach_dn`, `reporting_address_data.reach_ev`.
    *   **`sof$ta_e_business_*` Population:** Joins between BigQuery equivalents of `sof$ta_bp_ref_*_nodp` (derived intermediate tables) and `sof$ta_business_pt` are performed to populate `reporting_address_data.business_gp`, `reporting_address_data.business_re`, `reporting_address_data.business_dn`, `reporting_address_data.business_ev`.
    *   **`sof$ta_e_regulierer` Population:** Data is selected from `stg_cds_bp_ref` to populate `reporting_address_data.regulierer`.
6.  **Cleanup:** Intermediate tables are truncated or dropped.
7.  **Logging & Monitoring:** Airflow logs task statuses, and BigQuery job history provides detailed execution metrics.

## 5. Transformation Logic

The core transformation logic resides within `d_ausd_adressen.sql`, which will be converted to BigQuery SQL. Given its 'complex' tier and 'manual' migration bucket, direct automated translation is not expected. Key areas for manual transformation include:

*   **Oracle SQL*Plus specifics:** `WHENEVER SQLERROR`, `DEFINE`, `COLUMN new_value`, `start ../trace.sql.cfg`, `spool`, `&v_carmen`, `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`. These constructs need to be replaced with BigQuery equivalents or Airflow task management.
    *   `DEFINE v_carmen = "@pcrs1"` indicates a database link or schema qualifier. In BigQuery, this would correspond to specifying the correct dataset and project for source tables (e.g., `project_id.source_dataset.table_name`).
    *   `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE ...')` calls a PL/SQL procedure. This must be replaced with direct BigQuery DDL (e.g., `TRUNCATE TABLE` or `DELETE FROM` for partitioned tables).
*   **Date Handling:** Oracle `TO_DATE('&v_datum', 'YYYYMMDD')` and other date manipulations will need to be translated to BigQuery's date and timestamp functions (e.g., `PARSE_DATE('%Y%m%d', 'YYYYMMDD_STRING')`, `CURRENT_DATE()`). The `v_datum` derivation from `dwtk_meldungen` will need to be re-implemented.
*   **Table Naming:** The `sof$ta_` and `sof$ta_e_` tables should be renamed to conform to BigQuery naming conventions (e.g., `sof_ta_bp_ref_gp` or `bp_ref_gp_int`).
*   **Query Structure:** The `INSERT /*+ APPEND */ INTO ... SELECT` statements are common. Oracle `/*+ parallel(table,4) */` and `/*+ use_hash(table) */` hints will be removed as BigQuery automatically handles parallelism and query optimization.
*   **`UNION ALL` logic:** This should translate directly to BigQuery's `UNION ALL`.
*   **Conditional Logic:** The WHERE clauses with `insert_at`, `modified_at`, `valid_from`, `valid_to`, `is_production` are standard and will translate directly. The specific values for `bp_ref_ty`, `address_ref_ty`, `rdndant_invrec`, `mop_ref_ty` represent business rules and need to be preserved.
*   **`substr(lk.short_description,1,3)`:** String manipulations need to be mapped to BigQuery string functions.
*   **Implicit joins:** The Oracle syntax `FROM table1, table2 WHERE table1.id = table2.id` should be explicitly converted to `FROM table1 JOIN table2 ON table1.id = table2.id` for clarity and best practice in BigQuery.
*   **`p_wiederanlaufWert`:** The logic around this parameter, which filters records based on `DWH_VERTRAG_ID > Wiederanlaufwert`, will need to be integrated into the BigQuery SQL queries as a dynamic filter.

## 6. External Dependencies

*   **Oracle Database (CRS):** The primary external dependency is the source Oracle database hosting `cds$ta_bp_ref`, `glv$ta_country`, `bpd$ta_reachability`, `bpd$ta_business_partner`, `isbert_schema.dwtk_meldungen`, and `cds$ta_inv_definition`.
    *   **Replacement Strategy:** These source tables will be migrated or continuously replicated to BigQuery staging tables. Google Cloud offers various services for this, such as Cloud Data Fusion, Database Migration Service, or BigQuery Data Transfer Service. The exact method will depend on the source database size, complexity, and latency requirements. Once in BigQuery, they become internal to the GCP ecosystem.
*   **File System (`$HOME/.dw_init`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/...`, `${DW_DIR_UTL}/...`):** The KornShell scripts rely heavily on a local file system for sourcing utility scripts and temporary files (`tmpFile`).
    *   **Replacement Strategy:**
        *   **Utility Scripts:** The functionality of `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh` will be absorbed into the Airflow DAG (e.g., Python functions for parameter validation, date calculations) or replaced by native Airflow/BigQuery features.
        *   **Temporary Files:** `tmpFile` used to store record counts will be replaced by Airflow XComs for inter-task communication or direct logging.
        *   **`BERT_DIR_ROOT`:** This environment variable will be mapped to a configurable path in Airflow or removed if the underlying scripts are absorbed.
*   **SQL*Plus Client:** The `k_ausd_adressen.ksh` script implicitly relies on `sqlplus` being available to execute `d_ausd_adressen.sql`.
    *   **Replacement Strategy:** BigQuery operations will be executed directly via Airflow BigQuery operators, removing the need for a `sqlplus` client.

## 7. Unresolved / Risks

*   **Oracle `isbert_schema.dwtk_meldungen`:** This table is used to determine `v_datum` (stichtag). The logic `SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'` needs careful re-evaluation. It suggests a mechanism for managing dates based on job activity. This should be re-implemented in Airflow using a metadata table or an equivalent logic that fits the BigQuery/Airflow paradigm.
*   **`AL??` comments:** The scripts contain commented-out lines like `AL?? . ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_job.ksh` and `FOSJobDeaktivate`, `FOSJobErzeugeEintrag`. These indicate potentially deprecated or future functionality related to "FOS-Jobverwaltung." A decision is needed on whether this functionality should be implemented in the new environment or completely ignored. If relevant, it would require a BigQuery metadata table and corresponding Airflow tasks.
*   **`Wiederanlaufwert` logic:** The parameter `-l` (`p_wiederanlaufWert`) is described as "only contracts with DWH_VERTRAG_ID > Wiederanlaufwert" are written. This partial processing/restart logic needs to be carefully implemented in BigQuery SQL, ensuring idempotency and correctness for restarts.
*   **Performance:** The original Oracle SQL uses `/*+ parallel(table,4) */` hints. While BigQuery handles parallelism automatically, the performance of the translated BigQuery SQL should be thoroughly tested and optimized for large datasets.
*   **Data Validation:** The original `pruefeParameterGesetzt` and `DWDate_Datum_Check` functions ensure data quality. Equivalent validation needs to be built into the Airflow DAG or BigQuery SQL.
*   **Code Ownership and Business Rules:** Given the `manual` migration bucket for the SQL, there may be complex business rules embedded in the Oracle SQL that require deep understanding and careful re-implementation. Consultation with original developers/SMEs is recommended.

## 8. Build Plan

The migration will be executed in phases, focusing on re-implementing the orchestration and then the core data transformation.

**Phase 1: Environment Setup & Data Ingestion**
*   **Task 1.1:** Set up Google Cloud Project, BigQuery datasets (e.g., `staging`, `temp_address_processing`, `reporting_address_data`).
*   **Task 1.2:** Implement data ingestion pipelines for source Oracle tables (`cds$ta_bp_ref`, `glv$ta_country`, `bpd$ta_reachability`, `bpd$ta_business_partner`, `isbert_schema.dwtk_meldungen`, `cds$ta_inv_definition`) into BigQuery staging tables. (Tool: Cloud Data Fusion / Database Migration Service / BigQuery Data Transfer Service).
*   **Task 1.3:** Set up Cloud Composer environment.

**Phase 2: Orchestration Layer Migration (Airflow DAG)**
*   **File 2.1:** Design and develop an Airflow DAG (Python) to replace `r_ausd_adressen.ksh` and `k_ausd_adressen.ksh`.
    *   **Language:** Python
    *   **Components:**
        *   `PythonOperator` for parameter parsing, validation, and date calculations (replacing `getopts`, `pruefeParameterGesetzt`, `DWDate_Gib_Zeitraum`, `DWDate_Datum_Check`).
        *   `BashOperator` or `PythonOperator` to manage job status entries if `FOSJob` functionality (`AL??`) is deemed necessary.
        *   Integration with Cloud Logging for status and error messages.
        *   Tasks for managing the `stichtag` derivation from `dwtk_meldungen`.
*   **File 2.2:** Implement `gestern.ksh` equivalent as a Python function or an Airflow task to derive `p_datum_heute` and `p_datum_gestern`.
    *   **Language:** Python

**Phase 3: Data Transformation Logic Migration (BigQuery SQL)**
*   **File 3.1:** Convert `d_ausd_adressen.sql` to BigQuery Standard SQL.
    *   **Language:** BigQuery SQL
    *   **Components:**
        *   Individual BigQuery SQL scripts or `BigQueryOperator` tasks within the DAG for each `INSERT INTO` statement, starting with truncations.
        *   Replace Oracle-specific syntax (hints, SQL*Plus commands, PL/SQL calls) with BigQuery equivalents.
        *   Rename tables to BigQuery conventions.
        *   Thorough testing of each SQL component for functional equivalence and performance.
        *   The `manual` migration bucket for this file indicates significant redesign and validation effort.
*   **File 3.2:** Develop data quality checks and reconciliation processes between source Oracle and target BigQuery output tables.

**Phase 4: Testing & Deployment**
*   **Task 4.1:** Unit, integration, and end-to-end testing of the Airflow DAG and BigQuery SQL transformations.
*   **Task 4.2:** Performance testing and optimization of BigQuery queries.
*   **Task 4.3:** Deployment to production environment.