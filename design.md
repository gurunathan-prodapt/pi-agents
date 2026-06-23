# Migration Design — DW.BERT_AUSD_V_TA_P_DISCOUNT

## 1. Purpose & Scope
This job, `DW.BERT_AUSD_V_TA_P_DISCOUNT`, is an ETL workflow primarily designed to add contract numbers and contract templates to discount information. It involves orchestrating shell scripts and Oracle SQL scripts via a UC4 scheduler. The overall purpose is data preparation related to discounts, enriching existing discount data with contract details.

## 2. Source Inventory

| File Path | Category | Tool | Automation Bucket | Purpose/Summary | Complexity Tier | Migration Flags |
| :------------------------------------------------------------------------------------------------------------------------------- | :------- | :---------- | :---------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | :-------------- | :---------------- |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_P_DISCOUNT_RR.xml` | uc4 | UC4/Automic | semi_auto | UC4 UNIX job definition that executes a KornShell script to add contract number and contract template to discounts. | Unknown | Unknown |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_P_DISCOUNT.xml` | uc4 | UC4/Automic | semi_auto | UC4 UNIX job definition for 'DW.BERT_AUSD_V_TA_P_DISCOUNT' which adds contract numbers to discounts by executing a KornShell script. | Unknown | Unknown |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount_rr.ksh` | shell | KornShell | semi_auto | This is a control script for `r_ausd_vertrag.ksh` that handles parameter parsing, environment setup, error handling, and orchestrates the execution of an SQL script to process data for `ta_p_discount_rr`. | Unknown | Unknown |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount.ksh` | shell | KornShell | semi_auto | KornShell script to control and execute a SQL script (d_ausd_v_ta_p_discount.sql) for data preparation, handling job parameters and error logging. | Unknown | Unknown |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh` | shell | KornShell | semi_auto | This KornShell script acts as a wrapper for the 'k_ausd_v_ta_p_discount_rr.ksh' core script, handling environment setup, parameter parsing, error trapping, and logging for the 'ta_p_discount_rr' data reconciliation process. | Unknown | Unknown |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount.ksh` | shell | KornShell | semi_auto | KornShell script that orchestrates the data synchronization process for the 'ta_p_discount' table by setting up the environment, parsing parameters, and calling a core script. | Unknown | Unknown |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_p_discount_rr.sql` | sql | Oracle SQL*Plus | semi_auto | This SQL*Plus script truncates a target table and then populates it by joining data from multiple source tables, enriching discount information with contract details. | Unknown | Unknown |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_p_discount.sql` | sql | Oracle SQL/PLSQL | semi_auto | This SQL script truncates and reloads the SOF$TA_P_DISCOUNT table by joining data from SOF$TA_DISC_ZUSGF and SOF$TA_CNTRCT_CRS, and determines a processing date from DWTK_MELDUNGEN. | Unknown | Unknown |

*Note: Complexity Tier and Migration Flags are unknown as no data was found in the `file_complexity` table for these files.*

## 3. Target Architecture
The target architecture will leverage Google Cloud Platform (GCP) services, primarily BigQuery for data warehousing and transformations, and Cloud Composer (Airflow) for orchestration.

**Key Components:**
- **BigQuery Datasets and Tables:**
    - `isbert_schema.dwtk_meldungen` will be migrated to `project.dataset.dwtk_meldungen`.
    - `sof$ta_p_discount_rr` will be migrated to `project.dataset.ta_p_discount_rr`.
    - `sof$ta_discount_rr` will be migrated to `project.dataset.ta_discount_rr`.
    - `sof$ta_cntrct_crs` will be migrated to `project.dataset.ta_cntrct_crs`.
    - `sof$ta_cntrct_templ` will be migrated to `project.dataset.ta_cntrct_templ`.
    - `sof$ta_p_discount` will be migrated to `project.dataset.ta_p_discount`.
    - `sof$ta_disc_zusgf` will be migrated to `project.dataset.ta_disc_zusgf`.
    - Auxiliary tables for job control (`job_control`), job audit (`job_audit`), job log (`job_log`), and error log (`job_error_log`) will be created in BigQuery.
- **BigQuery Stored Procedures:** The KornShell scripts (`k_ausd_v_ta_p_discount_rr.ksh`, `k_ausd_v_ta_p_discount.ksh`, `r_ausd_v_ta_p_discount_rr.ksh`, `r_ausd_v_ta_p_discount.ksh`) and Oracle SQL scripts (`d_ausd_v_ta_p_discount_rr.sql`, `d_ausd_v_ta_p_discount.sql`) will be refactored into BigQuery stored procedures.
- **Cloud Composer (Airflow) DAGs:** The UC4 XML job definitions (`DW.BERT_AUSD_V_TA_P_DISCOUNT_RR.xml`, `DW.BERT_AUSD_V_TA_P_DISCOUNT.xml`) will be converted into Airflow DAGs to manage the orchestration of the BigQuery stored procedures.
- **Dataproc (optional for PySpark):** The current design for UC4 directly maps to DataprocSubmitJobOperator calling PySpark scripts. However, given the KornShell scripts primarily orchestrate SQL, a direct BigQuery stored procedure call from Airflow might be more efficient. The existing UC4 design implies a DataprocSubmitJobOperator calling PySpark scripts derived from the KornShell. This needs to be reviewed if the core logic in KSH is purely SQL orchestration.

## 4. Data Flow & Lineage
Based on the analysis of the source code and the UC4 job definitions, the data flow can be inferred as follows:

1.  **UC4 Jobs (Orchestration Layer):** The UC4 XML files act as the top-level orchestrators.
    - `DW.BERT_AUSD_V_TA_P_DISCOUNT_RR.xml` invokes `r_ausd_v_ta_p_discount_rr.ksh`.
    - `DW.BERT_AUSD_V_TA_P_DISCOUNT.xml` invokes `r_ausd_v_ta_p_discount.ksh`.

2.  **KornShell Wrapper Scripts (Orchestration & Parameter Handling):**
    - `r_ausd_v_ta_p_discount_rr.ksh` calls `k_ausd_v_ta_p_discount_rr.ksh`.
    - `r_ausd_v_ta_p_discount.ksh` calls `k_ausd_v_ta_p_discount.ksh`.
    These `r_` scripts primarily handle environment setup, parameter parsing, error logging, and then call their respective `k_` scripts.

3.  **KornShell Core Scripts (SQL Orchestration):**
    - `k_ausd_v_ta_p_discount_rr.ksh` executes `d_ausd_v_ta_p_discount_rr.sql`.
    - `k_ausd_v_ta_p_discount.ksh` executes `d_ausd_v_ta_p_discount.sql`.
    These `k_` scripts are control scripts that manage the execution of the Oracle SQL scripts, including job registration and status updates.

4.  **Oracle SQL Scripts (Data Transformation):**
    - `d_ausd_v_ta_p_discount_rr.sql`: Truncates `sof$ta_p_discount_rr` and inserts data by joining `sof$ta_discount_rr`, `sof$ta_cntrct_crs`, and `sof$ta_cntrct_templ`. It also reads from `isbert_schema.dwtk_meldungen` to determine a processing date.
    - `d_ausd_v_ta_p_discount.sql`: Truncates `sof$ta_p_discount` and inserts data by joining `sof$ta_disc_zusgf` and `sof$ta_cntrct_crs`. It also reads from `isbert_schema.dwtk_meldungen`.

**Migration Data Flow in GCP:**
-   Airflow DAGs (replacing UC4) will trigger BigQuery Stored Procedures.
-   BigQuery Stored Procedures (replacing KornShell and Oracle SQL) will perform data transformations:
    -   Read from source BigQuery tables (migrated `dwtk_meldungen`, `ta_discount_rr`, `ta_cntrct_crs`, `ta_cntrct_templ`, `ta_disc_zusgf`).
    -   Write to target BigQuery tables (migrated `ta_p_discount_rr`, `ta_p_discount`).
    -   Log job execution status and errors into BigQuery audit/log tables.

## 5. Transformation Logic

### 5.1. UC4 XML to Airflow DAG Design
The two UC4 XML files will be converted into two separate Airflow DAGs:
-   `DW.BERT_AUSD_V_TA_P_DISCOUNT_RR.xml` → `dw_bert_ausd_v_ta_p_discount_rr` DAG.
    -   This DAG will contain a single `DataprocSubmitJobOperator` task named `bert_ausd_v_ta_p_discount_rr` (or `BigQueryOperator` if the shell script is directly migrated to BQ SP) that calls the migrated `r_ausd_v_ta_p_discount_rr` logic.
-   `DW.BERT_AUSD_V_TA_P_DISCOUNT.xml` → `dw_bert_ausd_v_ta_p_discount` DAG.
    -   This DAG will contain a single `DataprocSubmitJobOperator` task named `run_dw_bert_ausd_v_ta_p_discount` (or `BigQueryOperator`) that calls the migrated `r_ausd_v_ta_p_discount` logic.

Both UC4 designs indicate "incomplete workflow export" as no `EVNT_TIME` or `JOBP` was provided. Schedules will be `None` and `start_date` will be a placeholder, requiring manual definition based on external scheduling requirements.

### 5.2. KornShell Scripts to BigQuery Stored Procedures Design
The four KornShell scripts (`r_*.ksh` and `k_*.ksh`) are primarily orchestration and control scripts. They will be migrated to BigQuery Stored Procedures.
-   **Parameter Handling:** `getopts`-style parameter parsing will be replaced by BigQuery stored procedure parameters.
-   **Environment Sourcing:** `. $HOME/.dw_init` and other utility script sourcing will be replaced by BigQuery `DECLARE` variables, configuration tables, or directly incorporated logic.
-   **Error Handling and Logging:** The `f_alis_msgerr.ksh` and `h_alis_sqlplus.ksh` helper functions will be replaced by BigQuery `BEGIN...EXCEPTION...END` blocks, `ASSERT` statements, and `INSERT` statements into BigQuery logging/audit tables.
-   **SQL Script Execution:** The `starteSQLSkript` function will be replaced by direct `CALL` statements to BigQuery stored procedures that implement the migrated Oracle SQL logic.
-   **Temporary Files:** Logic involving reading record counts from temporary files (`tmpFile`) will be replaced by `SELECT COUNT(*)` queries or `OUT` parameters from the called stored procedures.

### 5.3. Oracle SQL Scripts to BigQuery SQL Design
The two Oracle SQL scripts will be converted to BigQuery SQL, typically as part of BigQuery Stored Procedures or scheduled queries.
-   `d_ausd_v_ta_p_discount_rr.sql`:
    -   **Stichtag determination:** `SELECT NVL(TO_CHAR(MAX(m.timecreated),\'YYYYMMDD\'),\'19000101\')` will become `SELECT IFNULL(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')` in BigQuery.
    -   **Table Truncation:** `TRUNCATE TABLE sof$ta_p_discount_rr` remains `TRUNCATE TABLE project.dataset.ta_p_discount_rr`.
    -   **INSERT/SELECT Logic:** The `INSERT INTO ... SELECT` statement joining `sof$ta_discount_rr`, `sof$ta_cntrct_crs`, and `sof$ta_cntrct_templ` will be directly translated to BigQuery SQL, maintaining join conditions and column selections. The Oracle hints (`/*+ parallel(da,4) ... */`) will be removed as BigQuery handles parallelism automatically.
-   `d_ausd_v_ta_p_discount.sql`:
    -   **Stichtag determination:** Similar translation as `d_ausd_v_ta_p_discount_rr.sql`.
    -   **Table Truncation:** `TRUNCATE TABLE sof$ta_p_discount` becomes `TRUNCATE TABLE project.dataset.ta_p_discount`.
    -   **INSERT/SELECT Logic:** The `INSERT INTO ... SELECT` statement joining `sof$ta_disc_zusgf` and `sof$ta_cntrct_crs` will be directly translated to BigQuery SQL.

## 6. External Dependencies

The initial `external_systems` query returned empty. However, code analysis reveals:

-   **Oracle Database (`@pcrs1`):** Both SQL scripts use a `DB-Link` defined as `v_carmen = "@pcrs1"`. This indicates that the Oracle database "Carmen" is an external source for the discount and contract data (`isbert_schema.dwtk_meldungen`, `sof$ta_discount_rr`, `sof$ta_cntrct_crs`, `sof$ta_cntrct_templ`, `sof$ta_disc_zusgf`).

**Replacement Strategy:**
-   The Carmen Oracle database will need to be replaced with a GCP-native solution for data ingestion.
-   **Option 1: Database Migration:** If Carmen is being fully migrated to GCP, these tables (`dwtk_meldungen`, `ta_discount_rr`, `ta_cntrct_crs`, `ta_cntrct_templ`, `ta_disc_zusgf`) would reside in a Google Cloud SQL (PostgreSQL or MySQL) or AlloyDB instance. Data would then be ingested into BigQuery using batch loads (e.g., Cloud Data Fusion, Dataflow, or custom Python scripts via BigQuery Data Transfer Service) or streaming (e.g., Change Data Capture via Debezium to Pub/Sub to Dataflow to BigQuery).
-   **Option 2: Data Transfer Service (DTS):** If Carmen remains on-premises, BigQuery Data Transfer Service can be configured to regularly ingest data from Oracle into BigQuery.
-   **Option 3: Custom Dataflow/Python Connector:** A custom solution using Dataflow or Cloud Functions with Python to connect to the on-premises Oracle database and load data into BigQuery can be implemented for more complex ingestion logic.

The `isbert_schema.dwtk_meldungen` table (for `BERT_DROP_TEMP_TABLE` job_kennung) is crucial for determining the `Stichtag` (processing date). This metadata source must be available in BigQuery before the main transformation logic can run.

## 7. Unresolved / Risks

1.  **Missing `file_complexity` data:** The complexity tier and migration flags were not available for any of the source files. This means that an accurate, automated assessment of migration effort and potential challenges is lacking. Manual review of these files is required to assign complexity and identify specific migration risks.
2.  **Absence of `lineage_edges`:** No direct lineage edges were found between the files, requiring inference of the execution flow from code content. This indicates a potential gap in the automated lineage analysis for this job.
3.  **Incomplete UC4 Workflow Export:** The UC4 designs explicitly state that the provided XML files are not a complete workflow export (`EVNT_TIME`, `JOBP` missing). This means that the scheduling and inter-job dependencies (if any) are not fully captured and will need to be manually defined in Airflow DAGs based on external information. The `schedule` and `start_date` for the Airflow DAGs are placeholders.
4.  **Oracle DB-Link to Carmen (`@pcrs1`):** This is a critical external dependency. The migration strategy for the Carmen database and its data ingestion into BigQuery must be finalized. Any on-premises Oracle data sources need to be continuously synchronized with BigQuery or fully migrated.
5.  **KornShell Helpers:** The KornShell scripts rely on several helper scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`). The logic within these helpers needs to be thoroughly analyzed and either reimplemented as BigQuery stored procedures/UDFs or integrated into the Python code of the Airflow tasks if complex shell-specific logic exists.
6.  **"Active Jobs Ignored" / "Deactivate Old Active Jobs" logic:** The KornShell scripts contain logic to handle active jobs (ignore or deactivate). This job management logic needs to be carefully translated into the BigQuery stored procedures and Airflow tasks to ensure correct behavior in the new environment.

## 8. Build Plan

The migration will involve building the following components in an ordered sequence:

1.  **Migrate Oracle Source Tables to BigQuery:**
    -   Create BigQuery tables: `project.dataset.dwtk_meldungen`, `project.dataset.ta_discount_rr`, `project.dataset.ta_cntrct_crs`, `project.dataset.ta_cntrct_templ`, `project.dataset.ta_p_discount`, `project.dataset.ta_disc_zusgf`.
    -   Implement a data ingestion pipeline (e.g., using BigQuery DTS, Dataflow, or Cloud SQL federation) to populate these tables from the source Oracle "Carmen" database (`@pcrs1`). This is a prerequisite for all subsequent steps.

2.  **Build BigQuery Audit/Logging Infrastructure:**
    -   Create BigQuery tables: `project.dataset.job_control`, `project.dataset.job_audit`, `project.dataset.job_log`, `project.dataset.job_error_log`.
    -   Language: BigQuery DDL.

3.  **Develop BigQuery Stored Procedures for SQL Transformations:**
    -   Migrate `d_ausd_v_ta_p_discount_rr.sql` to `project.dataset.sp_d_ausd_v_ta_p_discount_rr`.
    -   Migrate `d_ausd_v_ta_p_discount.sql` to `project.dataset.sp_d_ausd_v_ta_p_discount`.
    -   Language: BigQuery SQL (Stored Procedures).

4.  **Develop BigQuery Stored Procedures for KornShell Logic:**
    -   Migrate `k_ausd_v_ta_p_discount_rr.ksh` to `project.dataset.sp_k_ausd_v_ta_p_discount_rr`. This procedure will call `project.dataset.sp_d_ausd_v_ta_p_discount_rr`.
    -   Migrate `k_ausd_v_ta_p_discount.ksh` to `project.dataset.sp_k_ausd_v_ta_p_discount`. This procedure will call `project.dataset.sp_d_ausd_v_ta_p_discount`.
    -   Migrate `r_ausd_v_ta_p_discount_rr.ksh` to `project.dataset.sp_r_ausd_v_ta_p_discount_rr`. This procedure will call `project.dataset.sp_k_ausd_v_ta_p_discount_rr`.
    -   Migrate `r_ausd_v_ta_p_discount.ksh` to `project.dataset.sp_r_ausd_v_ta_p_discount`. This procedure will call `project.dataset.sp_k_ausd_v_ta_p_discount`.
    -   Ensure that job control, parameter handling, and error logging from the KornShell scripts are correctly implemented in these BigQuery stored procedures.
    -   Language: BigQuery SQL (Stored Procedures).

5.  **Develop Airflow DAGs for Orchestration:**
    -   Migrate `DW.BERT_AUSD_V_TA_P_DISCOUNT_RR.xml` to `dw_bert_ausd_v_ta_p_discount_rr.py` Airflow DAG. This DAG will have a task (e.g., `BigQueryOperator`) to call `project.dataset.sp_r_ausd_v_ta_p_discount_rr`.
    -   Migrate `DW.BERT_AUSD_V_TA_P_DISCOUNT.xml` to `dw_bert_ausd_v_ta_p_discount.py` Airflow DAG. This DAG will have a task (e.g., `BigQueryOperator`) to call `project.dataset.sp_r_ausd_v_ta_p_discount`.
    -   Configure appropriate scheduling, error handling, and alerting within Airflow.
    -   Language: Python (Airflow DAG).

This structured approach ensures that dependencies are addressed incrementally, from foundational data sources and logging to complex transformations and orchestration.