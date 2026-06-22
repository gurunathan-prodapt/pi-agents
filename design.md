# Migration Design — DW.BERT_AUSD_BP_TA_TARIFOPTION

## 1. Purpose & Scope

The legacy job `DW.BERT_AUSD_BP_TA_TARIFOPTION` is an ETL process responsible for preparing and providing "instantiierten Basisprodukte" (instantiated basic products) for tariff options within the BERT system. This process generates a snapshot of contract cache data from the Data Warehouse (DWH) and makes it available for demand scoring (referred to as "FOS-Tabelle"). It involves reading input data, performing transformations, and producing two output tables related to tariff options. The job is scheduled and orchestrated using UC4/Automic and executes a series of KornShell scripts that, in turn, run an Oracle SQL script.

The scope of this migration is to re-platform this ETL job from its current UC4/KornShell/Oracle environment to Google Cloud Platform (GCP), specifically using Cloud Composer (Airflow) for orchestration and BigQuery for data storage and transformation.

## 2. Source Inventory

The job `DW.BERT_AUSD_BP_TA_TARIFOPTION` consists of the following source components:

*   **`vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_TARIFOPTION.xml`**
    *   **Technology**: UC4/Automic (XML Job Definition)
    *   **Role**: Top-level orchestrator. Executes `r_ausd_bp_ta_tarifoption.ksh`.
    *   **Complexity Tier**: (Not available, inferred Simple - Orchestration)
    *   **Automation Bucket**: (Not available, inferred B1 - Auto)
    *   **Summary**: UC4 job definition for a UNIX job orchestrating data preparation.

*   **`vobs/dw_source/isrpt/isbert/install_save/r_ausd_bp_ta_tarifoption.ksh`**
    *   **Technology**: KornShell (UNIX Shell Script)
    *   **Role**: Orchestration script. Parses parameters, sets environment, handles logging/error trapping, and invokes `k_ausd_bp_ta_tarifoption.ksh`.
    *   **Complexity Tier**: (Not available, inferred Medium - Orchestration with parameter handling)
    *   **Automation Bucket**: (Not available, inferred B2 - Semi-auto)
    *   **Summary**: Orchestrates initial provision of basic products, generates contract cache snapshot. Inputs: `DWH$TA_C_VERTRAG`. Outputs: `FOS-Tabelle`.

*   **`vobs/dw_source/isrpt/isbert/install_save/k_ausd_bp_ta_tarifoption.ksh`**
    *   **Technology**: KornShell (UNIX Shell Script)
    *   **Role**: Control script. Receives parameters, performs date validation, and executes the core Oracle SQL script `d_ausd_bp_ta_tarifoption.sql` via a helper function (`starteSQLSkript`).
    *   **Complexity Tier**: (Not available, inferred Medium - Control logic, validation)
    *   **Automation Bucket**: (Not available, inferred B2 - Semi-auto)
    *   **Summary**: Control script for data processing, handling parameters, date validation, and orchestrating SQL execution for `PoolBasisprodukt`. Inputs: `d_ausd_bp_ta_tarifoption.sql`, `PoolBasisprodukt`. Outputs: `PoolBasisprodukt`, `tmpFile`.

*   **`vobs/dw_source/isrpt/isbert/install_save/d_ausd_bp_ta_tarifoption.sql`**
    *   **Technology**: Oracle SQL/PLSQL (SQL Script)
    *   **Role**: Core ETL transformation logic. Defines and populates two target tables.
    *   **Complexity Tier**: (Not available, inferred Complex - Oracle-specific features, dynamic SQL)
    *   **Automation Bucket**: (Not available, inferred B2 - Semi-auto/B3 - Manual for complex Oracle functions)
    *   **Summary**: Defines and populates `SOF$TA_BPR_OPT_FILTER` and `SOF$TA_TARIFOPTION` by joining and transforming data from source tables, including dynamic table naming and custom Oracle functions. Inputs: `isbert_schema.dwtk_meldungen`, `isbert_schema.sof$ta_l_bpr_optionen_filter`, `sof$ta_bpr_opt_text_&v_datum`. Outputs: `SOF$TA_BPR_OPT_FILTER`, `SOF$TA_TARIFOPTION`.

## 3. Target Architecture

The migrated solution will reside entirely on Google Cloud Platform.

*   **Orchestration**: Cloud Composer (Apache Airflow) will manage the scheduling and execution of the job.
    *   A single Airflow DAG `dw_bert_ausd_bp_ta_tarifoption` will be created.
    *   The DAG will consist of a single task that executes a PySpark job on Dataproc (as suggested by the `uc4_to_airflow_dag_design` tool). This PySpark job will encapsulate the logic of the original KornShell scripts.
*   **Data Processing & Storage**: BigQuery will serve as the target data warehouse.
    *   All source Oracle tables (`isbert_schema.dwtk_meldungen`, `isbert_schema.sof$ta_l_bpr_optionen_filter`, `sof$ta_bpr_opt_text_&v_datum`, `DWH$TA_C_VERTRAG`, `FOS-Tabelle`) will be migrated to BigQuery tables.
    *   The Oracle SQL transformation logic (`d_ausd_bp_ta_tarifoption.sql`) will be translated into BigQuery SQL. This will likely involve a BigQuery stored procedure or a series of SQL scripts executed as separate tasks within the Airflow DAG.
    *   The target tables `SOF$TA_BPR_OPT_FILTER` and `SOF$TA_TARIFOPTION` will be BigQuery tables.
    *   Custom Oracle functions (`sof$ab_con.concatX`) will need to be re-implemented as BigQuery UDFs (User Defined Functions) or inline SQL logic.

## 4. Data Flow & Lineage

The migrated data flow will be as follows:

1.  **Airflow DAG (`dw_bert_ausd_bp_ta_tarifoption`)**: Triggered by a schedule (to be defined, as not present in source).
2.  **`run_dw_bert_ausd_bp_ta_tarifoption` (Dataproc PySpark Job)**: This task will be the equivalent of the `r_ausd_bp_ta_tarifoption.ksh` and `k_ausd_bp_ta_tarifoption.ksh` scripts.
    *   It will handle parameter passing (Stichtag, Wiederanlaufwert) and environment setup.
    *   It will construct the dynamic table name for `sof$ta_bpr_opt_text_&v_datum`.
    *   It will then execute the BigQuery SQL transformation.
3.  **BigQuery SQL Transformation (within PySpark Job or separate BQ task)**:
    *   **Source Tables (BigQuery)**:
        *   `isbert_schema.dwtk_meldungen`
        *   `isbert_schema.sof$ta_l_bpr_optionen_filter`
        *   `sof$ta_bpr_opt_text_<dynamic_date>`
        *   `DWH$TA_C_VERTRAG` (if directly consumed by a SQL component, otherwise only passed as parameter)
        *   `FOS-Tabelle` (if directly consumed by a SQL component, otherwise only passed as parameter)
    *   **Intermediate Table (BigQuery)**: `SOF$TA_BPR_OPT_FILTER`
    *   **Target Table (BigQuery)**: `SOF$TA_TARIFOPTION`

The execution order will be:
`start (Airflow) --> Dataproc PySpark Task (orchestrator logic) --> BigQuery SQL Task (ETL logic) --> end (Airflow)`

## 5. Transformation Logic

The core transformation logic resides in the Oracle SQL script `d_ausd_bp_ta_tarifoption.sql`. This will be converted to BigQuery SQL.

**Key Transformation Steps and Migrations:**

*   **Variable Definition (`v_datum`)**:
    *   **Source**: `DEFINE v_carmen = "@pcrs1"`, `SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';`
    *   **Target**: Use BigQuery scripting `DECLARE v_datum STRING;` and `SET v_datum = (SELECT IFNULL(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101') FROM `isbert_schema.dwtk_meldungen` m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE');`
*   **Table Management**:
    *   **Source**: `drop table sof$ta_bpr_opt_filter;`, `drop table sof$ta_tarifoption;`, `create table ... nologging tablespace ... parallel (degree 4) as select ...`
    *   **Target**: `DROP TABLE IF EXISTS `sof$ta_bpr_opt_filter`;`, `DROP TABLE IF EXISTS `sof$ta_tarifoption`;`, `CREATE TABLE ... OPTIONS (description = '...') AS SELECT ...`. Parallel hints, NOLOGGING, and tablespace specifications are not applicable in BigQuery and will be removed.
*   **Dynamic Table Naming (`sof$ta_bpr_opt_text_&v_datum`)**:
    *   **Source**: SQL*Plus substitution variable.
    *   **Target**: BigQuery `EXECUTE IMMEDIATE FORMAT` will be used to construct and execute the SQL query with the dynamically determined table name.
*   **Intermediate Table (`SOF$TA_BPR_OPT_FILTER`) Population**:
    *   **Source**: `create table sof$ta_bpr_opt_filter ... as select ... from isbert_schema.sof$ta_l_bpr_optionen_filter l, sof$ta_bpr_opt_text_&v_datum t where t.bpr_id = l.bpr_id`
    *   **Target**: Direct translation to BigQuery `CREATE TABLE ... AS SELECT` with proper table references.
*   **Final Table (`SOF$TA_TARIFOPTION`) Population**:
    *   **Source**: Complex `SELECT` statement with `LEAD` analytic function (`ORDER BY NULL` will be converted to `ORDER BY 1` or a suitable ordering if required for business logic beyond row numbering), `CASE` statements, `RTRIM`, `LTRIM`, `SUBSTR`, and custom Oracle functions `sof$ab_con.concatX`.
    *   **Target**:
        *   `LEAD` function will be directly translated.
        *   `RTRIM`, `LTRIM`, `SUBSTR` functions have direct BigQuery equivalents.
        *   `CASE` statements will be translated directly.
        *   **Critical**: The custom Oracle functions (`sof$ab_con.concat1`, `sof$ab_con.concat1r`, `sof$ab_con.concat2`, `sof$ab_con.concat2r`, `sof$ab_con.concat3`, `sof$ab_con.concat3r`) are Oracle-specific. These **must be re-implemented as BigQuery SQL UDFs**. The logic within these functions needs to be extracted from their source code (not provided in this analysis) and translated.
*   **Permissions**:
    *   **Source**: `grant select on ... to isbert_schema;`
    *   **Target**: BigQuery permissions are managed via IAM policies at the dataset or table level, not via SQL `GRANT` statements. This will be an external configuration step.

## 6. External Dependencies

| Original System/Object | Description | Migration Strategy | Target GCP Equivalent |
|:-----------------------|:------------|:-------------------|:----------------------|
| UC4/Automic            | Scheduler   | Re-platform        | Cloud Composer (Airflow) DAG |
| Oracle Database        | Source and Target data store | Migrate data and queries | BigQuery Datasets and Tables |
| `DWH$TA_C_VERTRAG`     | Oracle Table | Migrate to BigQuery | BigQuery Table |
| `FOS-Tabelle`          | Oracle Table | Migrate to BigQuery | BigQuery Table |
| `isbert_schema.dwtk_meldungen` | Oracle Table | Migrate to BigQuery | BigQuery Table |
| `isbert_schema.sof$ta_l_bpr_optionen_filter` | Oracle Table | Migrate to BigQuery | BigQuery Table |
| `sof$ta_bpr_opt_text_&v_datum` | Oracle Table (dynamic name) | Migrate to BigQuery | BigQuery Table (dynamic name handled via `EXECUTE IMMEDIATE`) |
| `sof$ab_con.concatX` functions | Custom Oracle PL/SQL functions | Re-implement logic as BigQuery UDFs | BigQuery SQL UDFs |
| `$HOME/.dw_init`       | KornShell initialization script | Environment setup in Airflow/PySpark | Python environment setup within Airflow/Dataproc |
| `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh` | KornShell utility scripts | Re-implement relevant logic in PySpark for parameter parsing, date handling, and logging. | Python functions/modules within PySpark job |

## 7. Unresolved / Risks

*   **Missing Source for Custom Oracle Functions**: The source code for `sof$ab_con.concatX` functions was not available. These are critical for the ETL logic and need to be analyzed and re-implemented as BigQuery UDFs. This is a **high-risk** item if the logic is complex or not easily accessible.
*   **Empty `file_complexity` and `automation_rate`**: The complexity tiers and automation buckets were inferred due to missing data. This could lead to underestimation of effort. A manual review of these metrics is recommended.
*   **Full `DWH$TA_C_VERTRAG` and `FOS-Tabelle` definitions**: While `file_analysis` identified these as inputs/outputs, their full schema definitions were not explicitly retrieved. These will be needed for accurate BigQuery table creation.
*   **`ORDER BY NULL` in `LEAD` function**: The `LEAD(..., 1, -1) OVER (ORDER BY NULL)` construct in Oracle implies non-deterministic ordering or a specific Oracle internal ordering. In BigQuery, `ORDER BY NULL` is not valid. It should be replaced with `LEAD(..., 1, -1) OVER ()` if order does not matter for the logic, or with an explicit `ORDER BY <column(s)>` clause if the order is logically significant. This should be verified with the business logic.
*   **`Stichtag` determination**: The original `r_ausd_bp_ta_tarifoption.ksh` script has logic to determine `p_stichtag` if not provided, potentially using `MIN(sysdate, maxladedatum)`. The migration to PySpark should accurately reflect this logic.
*   **`starteSQLSkript` helper function**: The KornShell script uses a helper function `starteSQLSkript`. The implementation of this function (e.g., how it connects to Oracle SQL*Plus and executes the SQL) is not fully detailed but implies standard `sqlplus` execution. This will be replaced by direct BigQuery client execution within the PySpark script.

## 8. Build Plan

The build plan will involve translating each component into its GCP equivalent.

1.  **Migrate Oracle Tables to BigQuery**:
    *   Create BigQuery datasets for `isbert_schema` and any other relevant schemas.
    *   Migrate data for `isbert_schema.dwtk_meldungen`, `isbert_schema.sof$ta_l_bpr_optionen_filter`, `DWH$TA_C_VERTRAG`, and `FOS-Tabelle` to corresponding BigQuery tables.
    *   Define schema for `sof$ta_bpr_opt_text_<dynamic_date>` based on source data.
    *   **Language**: BigQuery DDL, Data Migration tools (e.g., BigQuery Data Transfer Service, custom Dataflow jobs).
2.  **Translate Custom Oracle Functions to BigQuery UDFs**:
    *   Analyze the logic of `sof$ab_con.concat1`, `sof$ab_con.concat1r`, `sof$ab_con.concat2`, `sof$ab_con.concat2r`, `sof$ab_con.concat3`, `sof$ab_con.concat3r`.
    *   Implement these as BigQuery SQL UDFs.
    *   **Language**: BigQuery SQL UDFs.
3.  **Convert Oracle SQL Script to BigQuery SQL**:
    *   Translate `d_ausd_bp_ta_tarifoption.sql` to BigQuery SQL, incorporating the BigQuery UDFs created in step 2, handling dynamic table names via `EXECUTE IMMEDIATE`, and adapting Oracle-specific syntax.
    *   This will form the core ETL script.
    *   **Language**: BigQuery SQL.
4.  **Convert KornShell Scripts to PySpark Orchestration Logic**:
    *   Create a PySpark script (e.g., `r_ausd_bp_ta_tarifoption_main.py`) that encapsulates the logic of `r_ausd_bp_ta_tarifoption.ksh` and `k_ausd_bp_ta_tarifoption.ksh`.
    *   This script will:
        *   Parse parameters (`Stichtag`, `Wiederanlaufwert`).
        *   Set up necessary environment variables or configurations.
        *   Call the BigQuery SQL script (from step 3) using a BigQuery client library (e.g., `google-cloud-bigquery` Python client).
        *   Handle logging and error trapping.
    *   **Language**: Python (PySpark).
5.  **Create Airflow DAG**:
    *   Develop an Airflow DAG `dw_bert_ausd_bp_ta_tarifoption.py`.
    *   The DAG will define a `DataprocSubmitJobOperator` task that executes the PySpark script (from step 4) on a Dataproc cluster.
    *   Configure DAG schedule, `start_date`, `owner`, and other Airflow properties.
    *   **Language**: Python.
6.  **Deployment**:
    *   Deploy BigQuery tables and UDFs.
    *   Upload PySpark script to a GCS bucket.
    *   Deploy Airflow DAG to Cloud Composer environment.
    *   **Language**: gcloud CLI, Terraform (for IaC).