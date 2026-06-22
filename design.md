# Migration Design — DW.BERT_AUSD_BP_TA_TARIFOPTION

## 1. Purpose & Scope

This migration job, `DW.BERT_AUSD_BP_TA_TARIFOPTION`, is responsible for the preparation and provision of selected basic product (tarifoption) data for the BERT system. Its primary business purpose is to generate a snapshot of contract cache from the Data Warehouse (DWH) and make it available for demand scoring (FOS-Tabelle), populating specific intermediate and final tables. The job processes and transforms tariff option data, categorizing and aggregating it based on business, GPRS, and other criteria.

The scope of this migration covers the entire ETL workflow from its UC4 scheduling to the final data transformation and loading into target tables within BigQuery.

## 2. Source Inventory

| File Path                                                                                                   | Technology        | Tier   | Automation Bucket | Summary                                                                                                                                                                                                                               |
| :---------------------------------------------------------------------------------------------------------- | :---------------- | :----- | :---------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_TARIFOPTION.xml` | UC4/Automic       | medium | semi_auto         | UC4 job definition for a UNIX job named DW.BERT_AUSD_BP_TA_TARIFOPTION, which orchestrates the execution of a KornShell script for data preparation.                                                                                    |
| `vobs/dw_source/isrpt/isbert/install_save/r_ausd_bp_ta_tarifoption.ksh`                                     | KornShell         | medium | semi_auto         | This ksh script orchestrates the initial provision of selected basic products for BERT. It generates a snapshot of contract cache from DWH and makes it available for demand scoring (FOS-Tabelle).                                           |
| `vobs/dw_source/isrpt/isbert/install_save/k_ausd_bp_ta_tarifoption.ksh`                                     | KornShell         | medium | semi_auto         | This KornShell script acts as a control script for a data processing job, handling parameter parsing, date validation, and orchestrating the execution of a SQL script to process data related to 'PoolBasisprodukt'.                     |
| `vobs/dw_source/isrpt/isbert/install_save/d_ausd_bp_ta_tarifoption.sql`                                     | Oracle SQL/PLSQL  | medium | semi_auto         | This SQL script defines and populates two tables, SOF$TA_BPR_OPT_FILTER and SOF$TA_TARIFOPTION, by joining and transforming data from source tables, including a dynamically named table and custom concatenation functions. |

## 3. Target Architecture

The target architecture in BigQuery will leverage Google Cloud services for orchestration and data processing.

*   **Orchestration**: Apache Airflow (Cloud Composer) will replace the UC4 scheduler. A single Airflow DAG will manage the end-to-end workflow.
*   **Data Processing**:
    *   The core transformation logic from the Oracle SQL script will be converted into BigQuery SQL.
    *   The KornShell scripts' logic (parameter handling, date validation, SQL execution) will be rewritten in Python, likely encapsulated within a PySpark job running on Dataproc, or directly within Airflow PythonOperators / BigQueryOperators where appropriate.
    *   Intermediate and final tables will reside in BigQuery datasets (e.g., `bert_staging`, `bert_reporting`).
*   **User-Defined Functions (UDFs)**: Oracle custom concatenation functions (`sof$ab_con.concatX`, `sof$ab_con.concatXr`) will be reimplemented as BigQuery SQL UDFs or JavaScript UDFs.

## 4. Data Flow & Lineage

The original data flow is:
`UC4 Job (DW.BERT_AUSD_BP_TA_TARIFOPTION.xml)`
  `INVOKES`
  `r_ausd_bp_ta_tarifoption.ksh` (Orchestrates initial data provision)
    `INVOKES`
    `k_ausd_bp_ta_tarifoption.ksh` (Control script: parameter parsing, date validation)
      `INVOKES`
      `d_ausd_bp_ta_tarifoption.sql` (Core data transformation)
        `READS` from:
          - `isbert_schema.dwtk_meldungen`
          - `isbert_schema.sof$ta_l_bpr_optionen_filter`
          - `sof$ta_bpr_opt_text_<dynamic_date_variable>`
        `WRITES` to:
          - `sof$ta_bpr_opt_filter` (intermediate table)
          - `sof$ta_tarifoption` (final output table)
        `USES` custom Oracle functions: `sof$ab_con.concat1`, `sof$ab_con.concat1r`, `sof$ab_con.concat2`, `sof$ab_con.concat2r`, `sof$ab_con.concat3`, `sof$ab_con.concat3r`.

**Target Data Flow (Airflow DAG)**:
1.  **Start Task**: Placeholder.
2.  **Date Variable Task (PythonOperator/Dataproc PySpark)**:
    *   Retrieves the maximum `timecreated` from `isbert_schema.dwtk_meldungen` (migrated to BigQuery, e.g., `bert_staging.dwtk_meldungen`).
    *   Formats the date to `YYYYMMDD` to construct the dynamic table name.
    *   Passes this `v_datum` variable as an Airflow XCom or to the BigQuery operator.
3.  **Drop Tables Task (BigQueryOperator)**:
    *   Executes `DROP TABLE IF EXISTS` for `bert_staging.bpr_opt_filter` and `bert_reporting.tarifoption`.
4.  **Create Intermediate Filter Table Task (BigQueryOperator)**:
    *   Executes the BigQuery SQL to create `bert_staging.bpr_opt_filter` by joining `isbert_schema.sof$ta_l_bpr_optionen_filter` (migrated to BigQuery, e.g., `bert_master.sof_l_bpr_optionen_filter`) and the dynamic table (`sof_ta_bpr_opt_text_YYYYMMDD`, which would also be a BigQuery table, potentially partitioned or sharded by date).
5.  **Create Final Tariff Option Table Task (BigQueryOperator)**:
    *   Executes the BigQuery SQL to create `bert_reporting.tarifoption`, applying the `LEAD` window function, `CASE` logic, string manipulations, and the custom UDFs.
6.  **End Task**: Placeholder.

## 5. Transformation Logic

The core transformation logic is contained within `d_ausd_bp_ta_tarifoption.sql`.

**Key Transformations:**

*   **Dynamic Table Name**: The `sof$ta_bpr_opt_text_&v_datum` table name, where `v_datum` is derived from `MAX(m.timecreated)` in `isbert_schema.dwtk_meldungen`, will be handled using BigQuery scripting variables or by passing the date as a parameter. The source table `sof$ta_bpr_opt_text_<DATE>` needs to be available in BigQuery, likely as a daily partitioned/sharded table.
*   **Temporary Tables**: `sof$ta_bpr_opt_filter` and `sof$ta_tarifoption` are created in Oracle. In BigQuery, these will be migrated to permanent tables, e.g., `bert_staging.bpr_opt_filter` and `bert_reporting.tarifoption`.
*   **`NVL` to `IFNULL`**: Oracle's `NVL` function will be replaced by BigQuery's `IFNULL`.
*   **`TO_CHAR(date, 'YYYYMMDD')`**: Oracle's date formatting will be replaced by BigQuery's `FORMAT_DATE('%Y%m%d', DATE(...))`.
*   **Window Functions**: The `LEAD(bpr_opt.cntrct_id, 1, -1) OVER (ORDER BY NULL)` will be converted to `LEAD(bpr_opt.cntrct_id, 1, -1) OVER (ORDER BY bpr_opt.cntrct_id, bpr_opt.pds_description)`. The `ORDER BY` clause will be explicitly defined based on the subquery's ordering.
*   **String Functions**: `RTRIM`, `LTRIM`, `SUBSTR` are directly translatable to BigQuery SQL.
*   **Custom Concatenation Functions**: `sof$ab_con.concat1`, `sof$ab_con.concat1r`, `sof$ab_con.concat2`, `sof$ab_con.concat2r`, `sof$ab_con.concat3`, `sof$ab_con.concat3r` are custom Oracle functions. These will need to be reimplemented as BigQuery SQL UDFs or JavaScript UDFs, preserving their exact logic. If their logic is simple string concatenation, standard BigQuery functions (`CONCAT`, `CONCAT_WS`) can be used.

## 6. External Dependencies

The following external dependencies were identified:

*   **Oracle Database**:
    *   **Source Tables**: `isbert_schema.dwtk_meldungen`, `isbert_schema.sof$ta_l_bpr_optionen_filter`, and `sof$ta_bpr_opt_text_<dynamic_date_variable>`. These will be migrated to BigQuery tables (e.g., `bert_staging.dwtk_meldungen`, `bert_master.sof_l_bpr_optionen_filter`, `bert_raw.sof_ta_bpr_opt_text_YYYYMMDD`).
    *   **Custom Functions**: `sof$ab_con.concatX` and `sof$ab_con.concatXr` functions. These will be replaced by BigQuery UDFs.
*   **Host `DWHDWH2P`**: This host was used by the UC4 job. In BigQuery, this will be abstracted by Google Cloud's managed services.
*   **Login `DW.UNIX.ISBERT`**: This Oracle database login will be replaced by appropriate service account or IAM roles for BigQuery access.
*   **Filesystem (`$HOME/.dw_init`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/...`)**: The `.dw_init` and various utility KornShell scripts are sourced. This environment setup and utility logic will be replaced by Python environment configurations, Python helper functions, or Airflow hooks/plugins within the PySpark job or Airflow tasks.

## 7. Unresolved / Risks

*   **Dynamic Table Naming**: The dynamic table `sof$ta_bpr_opt_text_&v_datum` implies a daily or periodic generation of source data. The migration strategy for this source needs to be confirmed (e.g., daily ingestion into a partitioned/sharded BigQuery table).
*   **Custom Oracle Functions**: The exact logic of the `sof$ab_con.concatX` functions is not fully known from the provided SQL. These need to be analyzed in detail to ensure accurate re-implementation as BigQuery UDFs.
*   **KornShell Script Logic**: The full complexity of the parameter parsing, date validation, and utility script invocations (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) in `r_ausd_bp_ta_tarifoption.ksh` and `k_ausd_bp_ta_tarifoption.ksh` needs to be mapped to Python within the Dataproc PySpark job. While the UC4 design suggests PySpark, a simpler Python script executed by an Airflow PythonOperator might suffice if the logic is primarily orchestration and parameter passing.
*   **Performance Tuning**: The Oracle script uses `parallel (degree 4)` hints and `nologging`. BigQuery handles parallelism automatically, but tablespace directives and nologging (which relate to redo logs in Oracle) need to be considered for equivalent BigQuery performance and cost optimization (e.g., clustering, partitioning). The `LEAD` function without a specific `ORDER BY` within its `OVER` clause relies on the subquery's `ORDER BY`, which is important to preserve.
*   **Error Handling**: The KornShell scripts include error handling (`f_alis_msgerr.ksh`, `WHENEVER SQLERROR EXIT FAILURE`). This needs to be translated to Airflow's retry mechanisms, Python exception handling, and appropriate logging within the Google Cloud ecosystem (Cloud Logging, Cloud Monitoring).

## 8. Build Plan

The migration will be implemented in the following ordered steps:

1.  **Define BigQuery UDFs for Oracle `sof$ab_con.concatX` functions (SQL)**:
    *   Analyze the original Oracle function definitions to understand their exact string manipulation logic.
    *   Implement equivalent BigQuery SQL or JavaScript UDFs.
    *   Deploy these UDFs to the target BigQuery project.

2.  **Migrate Oracle Source Tables to BigQuery**:
    *   Ingest `isbert_schema.dwtk_meldungen`, `isbert_schema.sof$ta_l_bpr_optionen_filter`, and the dynamic `sof$ta_bpr_opt_text_<DATE>` tables into BigQuery (e.g., using Dataflow, Datastream, or direct BigQuery loads).
    *   Establish appropriate partitioning and clustering for these tables.

3.  **Develop BigQuery SQL Transformation Script (SQL)**:
    *   Convert `d_ausd_bp_ta_tarifoption.sql` into a BigQuery-compatible SQL script, addressing `NVL`, `TO_CHAR`, dynamic table names, and incorporating the new BigQuery UDFs.
    *   Define target tables: `bert_staging.bpr_opt_filter` and `bert_reporting.tarifoption`, including schema definitions, partitioning, and clustering.

4.  **Develop PySpark/Python Script for Logic of `r_ausd_bp_ta_tarifoption.ksh` and `k_ausd_bp_ta_tarifoption.ksh` (Python)**:
    *   Reimplement parameter parsing, date validation, and environment setup logic.
    *   The script will dynamically generate the `v_datum` variable.
    *   It will then invoke the BigQuery SQL transformation script, passing necessary parameters (e.g., `v_datum`). This can be done via the `google-cloud-bigquery` client library or by constructing `bq` command-line calls.

5.  **Design and Implement Airflow DAG (Python)**:
    *   Create an Airflow DAG `dw_bert_ausd_bp_ta_tarifoption` (as per `uc4_to_airflow_dag_design`).
    *   Define tasks:
        *   A PythonOperator to determine the `v_datum`.
        *   BigQueryOperators or a DataprocSubmitJobOperator (if using PySpark) to execute the BigQuery SQL transformation, possibly split into separate tasks for `DROP`, `CREATE sof$ta_bpr_opt_filter`, and `CREATE sof$ta_tarifoption`.
        *   Include appropriate dependencies between tasks.
    *   Configure `start_date`, `schedule`, and error handling.

6.  **Deployment and Testing**:
    *   Deploy the Airflow DAG, PySpark/Python scripts, BigQuery SQL, and UDFs to the Google Cloud environment.
    *   Thoroughly test the end-to-end pipeline, verifying data accuracy and performance against the legacy system.

## 9. Next Steps

- Detail the schema for target BigQuery tables: `bert_staging.dwtk_meldungen`, `bert_master.sof_l_bpr_optionen_filter`, `bert_raw.sof_ta_bpr_opt_text_YYYYMMDD`, `bert_staging.bpr_opt_filter`, `bert_reporting.tarifoption`.
- Obtain the full source code for the custom Oracle concatenation functions (`sof$ab_con.concatX`) to accurately reimplement them as BigQuery UDFs.
- Clarify the exact parameter passing mechanisms and environment variables used by the KornShell scripts to ensure full functional parity in the Python/PySpark rewrite.
- Determine the scheduling frequency and dependency requirements for the Airflow DAG, especially regarding `EVNT_TIME` information that was not available.