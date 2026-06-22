# MIGRATION_NOTES.md

## 1. Summary

The `k_ausd_adressen.ksh` job, a KornShell orchestration script, along with its core Oracle SQL component `d_ausd_adressen.sql`, has been migrated. The original system was responsible for preparing and transforming business partner and address-related data within an Oracle database environment.

The migration target platform is Google Cloud Platform (GCP), specifically:
*   **BigQuery**: For data storage, staging, and executing the core data transformation logic.
*   **Cloud Composer (Apache Airflow)**: For orchestrating the data pipeline, replacing the KornShell script's control flow, parameter handling, and scheduling.

The migration involved converting Oracle SQL DDLs and DMLs to BigQuery SQL, encapsulating the transformation logic within a BigQuery Stored Procedure, and developing an Airflow DAG to manage its execution.

## 2. Generated artifacts

The migration process generated the following files:

*   **`sql/raw/create_cds_ta_bp_ref.sql`**: BigQuery DDL for the `raw.cds_ta_bp_ref` table. This table serves as the BigQuery equivalent of the original Oracle `cds$ta_bp_ref` source table.
*   **`sql/raw/create_cds_ta_inv_definition.sql`**: BigQuery DDL for the `raw.cds_ta_inv_definition` table. This table serves as the BigQuery equivalent of the original Oracle `cds$ta_inv_definition` source table.
*   **`sql/raw/create_glv_ta_country.sql`**: BigQuery DDL for the `raw.glv_ta_country` table. This table serves as the BigQuery equivalent of the original Oracle `glv$ta_country` source table.
*   **`sql/raw/create_glv_ta_description.sql`**: BigQuery DDL for the `raw.glv_ta_description` table. This table serves as the BigQuery equivalent of the original Oracle `glv$ta_description` source table.
*   **`sql/raw/create_bpd_ta_reachability.sql`**: BigQuery DDL for the `raw.bpd_ta_reachability` table. This table serves as the BigQuery equivalent of the original Oracle `bpd$ta_reachability` source table.
*   **`sql/raw/create_bpd_ta_business_partner.sql`**: BigQuery DDL for the `raw.bpd_ta_business_partner` table. This table serves as the BigQuery equivalent of the original Oracle `bpd$ta_business_partner` source table.
*   **`sql/raw/create_dwtk_meldungen.sql`**: BigQuery DDL for the `raw.dwtk_meldungen` table. This table serves as the BigQuery equivalent of the original Oracle `isbert_schema.dwtk_meldungen` source table, used for retrieving a date value.
*   **`sql/staging/create_sof_ta_bp_ref_gp.sql`**: BigQuery DDL for the `staging.sof_ta_bp_ref_gp` table. This is an intermediate staging table used during the data transformation process.
*   **`sql/staging/create_sof_ta_bp_ref_re.sql`**: BigQuery DDL for the `staging.sof_ta_bp_ref_re` table. Another intermediate staging table.
*   **`sql/staging/create_sof_ta_bp_ref_ev.sql`**: BigQuery DDL for the `staging.sof_ta_bp_ref_ev` table. Another intermediate staging table.
*   **`sql/staging/create_sof_ta_bp_ref_dn.sql`**: BigQuery DDL for the `staging.sof_ta_bp_ref_dn` table. Another intermediate staging table.
*   **`sql/staging/create_sof_ta_country.sql`**: BigQuery DDL for the `staging.sof_ta_country` table. Another intermediate staging table.
*   **`sql/staging/create_sof_ta_country_desc.sql`**: BigQuery DDL for the `staging.sof_ta_country_desc` table. Another intermediate staging table.
*   **`sql/staging/create_sof_ta_laender_kng.sql`**: BigQuery DDL for the `staging.sof_ta_laender_kng` table. Another intermediate staging table.
*   **`sql/staging/create_sof_ta_reachability.sql`**: BigQuery DDL for the `staging.sof_ta_reachability` table. Another intermediate staging table.
*   **`sql/staging/create_sof_ta_bp_ref_gp_nodp.sql`**: BigQuery DDL for the `staging.sof_ta_bp_ref_gp_nodp` table. Another intermediate staging table.
*   **`sql/staging/create_sof_ta_bp_ref_re_nodp.sql`**: BigQuery DDL for the `staging.sof_ta_bp_ref_re_nodp` table. Another intermediate staging table.
*   **`sql/staging/create_sof_ta_bp_ref_ev_nodp.sql`**: BigQuery DDL for the `staging.sof_ta_bp_ref_ev_nodp` table. Another intermediate staging table.
*   **`sql/staging/create_sof_ta_bp_ref_dn_nodp.sql`**: BigQuery DDL for the `staging.sof_ta_bp_ref_dn_nodp` table. Another intermediate staging table.
*   **`sql/staging/create_sof_ta_business_pt.sql`**: BigQuery DDL for the `staging.sof_ta_business_pt` table. Another intermediate staging table.
*   **`sql/target/create_sof_ta_e_reach_gp.sql`**: BigQuery DDL for the `target.sof_ta_e_reach_gp` table. This is a final target table for the transformed data.
*   **`sql/target/create_sof_ta_e_reach_re.sql`**: BigQuery DDL for the `target.sof_ta_e_reach_re` table. Another final target table.
*   **`sql/target/create_sof_ta_e_reach_ev.sql`**: BigQuery DDL for the `target.sof_ta_e_reach_ev` table. Another final target table.
*   **`sql/target/create_sof_ta_e_reach_dn.sql`**: BigQuery DDL for the `target.sof_ta_e_reach_dn` table. Another final target table.
*   **`sql/target/create_sof_ta_e_business_gp.sql`**: BigQuery DDL for the `target.sof_ta_e_business_gp` table. Another final target table.
*   **`sql/target/create_sof_ta_e_business_re.sql`**: BigQuery DDL for the `target.sof_ta_e_business_re` table. Another final target table.
*   **`sql/target/create_sof_ta_e_business_ev.sql`**: BigQuery DDL for the `target.sof_ta_e_business_ev` table. Another final target table.
*   **`sql/target/create_sof_ta_e_business_dn.sql`**: BigQuery DDL for the `target.sof_ta_e_business_dn` table. Another final target table.
*   **`sql/target/create_sof_ta_e_regulierer.sql`**: BigQuery DDL for the `target.sof_ta_e_regulierer` table. Another final target table.
*   **`sql/metrics/create_job_log.sql`**: BigQuery DDL for the `metrics.job_log` table. This table is used to log job execution details, status, and record counts, replacing the original job table management.
*   **`sql/stored_procedures/sp_ausd_adressen_main.sql`**: BigQuery Stored Procedure encapsulating the entire data transformation logic from `d_ausd_adressen.sql`. It handles parameter validation, truncation of staging/target tables, and all data insertion steps.
*   **`python/dags/k_ausd_adressen_dag.py`**: An Apache Airflow DAG responsible for orchestrating the execution of the `sp_ausd_adressen_main` BigQuery Stored Procedure. It handles parameter passing and defines the job's schedule.

## 3. Key design decisions

*   **BigQuery for Data Transformation**: BigQuery was chosen for its scalability, serverless nature, and powerful SQL engine, which is well-suited for large-scale data transformations. This replaces the Oracle database as the primary data processing environment.
*   **Cloud Composer (Airflow) for Orchestration**: Airflow provides robust scheduling, monitoring, and error handling capabilities, making it an ideal replacement for the KornShell script's orchestration logic. It allows for defining complex workflows as Directed Acyclic Graphs (DAGs).
*   **BigQuery Stored Procedure for Core Logic**: Consolidating the `d_ausd_adressen.sql` logic into a single BigQuery Stored Procedure (`sp_ausd_adressen_main`) simplifies deployment and execution from Airflow. It allows for parameterization, internal error handling, and atomic execution of the entire transformation sequence.
*   **Explicit Staging Tables in BigQuery**: The numerous `sof$ta_` temporary tables in Oracle were translated into persistent staging tables within a dedicated `staging` BigQuery dataset. This provides better visibility into intermediate data, simplifies debugging, and aligns with BigQuery's best practices for multi-step transformations, especially when intermediate results might be large.
*   **Native BigQuery Date and String Functions**: Oracle-specific functions (e.g., `TO_DATE`, `NVL`, `SUBSTR`) were replaced with their BigQuery equivalents (`PARSE_DATE`, `IFNULL`, `SUBSTR`). This leverages BigQuery's optimized functions and ensures compatibility.
*   **Removal of Oracle Hints**: Oracle performance hints (e.g., `parallel`, `use_hash`) were removed as BigQuery's query optimizer automatically handles execution plans and parallelization, making such hints unnecessary and potentially counterproductive.
*   **Centralized Job Logging**: The commented-out job management in the original script was implemented as a dedicated `metrics.job_log` BigQuery table. This provides a structured and queryable log of job executions, including status, parameters, and record counts, enhancing observability.
*   **Parameter Handling via Airflow and Stored Procedure**: The input parameters of the original KornShell script are now passed from the Airflow DAG to the BigQuery Stored Procedure. Airflow macros (like `ds_nodash`) are used for dynamic date parameters, ensuring flexibility and adherence to scheduling patterns.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Create the following BigQuery datasets in your GCP project:
        *   `PROJECT_ID.raw` (for source data)
        *   `PROJECT_ID.staging` (for intermediate tables)
        *   `PROJECT_ID.target` (for final output tables)
        *   `PROJECT_ID.metrics` (for job logging)
    *   Replace `PROJECT_ID` with your actual GCP project ID.

2.  **BigQuery Table Creation (DDLs)**:
    *   Execute all DDL scripts located in `sql/raw/`, `sql/staging/`, `sql/target/`, and `sql/metrics/` to create the necessary tables.
    *   Ensure `PROJECT_ID` placeholders are replaced with your actual GCP project ID in each DDL.

3.  **BigQuery Stored Procedure Deployment**:
    *   Deploy the `sp_ausd_adressen_main.sql` stored procedure to the `PROJECT_ID.dataset` BigQuery dataset (e.g., `PROJECT_ID.data_processing`).
    *   Ensure `PROJECT_ID` placeholders are replaced with your actual GCP project ID within the stored procedure definition.

4.  **Source Data Ingestion**:
    *   Ensure that all source Oracle tables (`cds$ta_bp_ref`, `glv$ta_country`, `glv$ta_description`, `bpd$ta_reachability`, `bpd$ta_business_partner`, `isbert_schema.dwtk_meldungen`) are ingested and populated into their corresponding BigQuery `PROJECT_ID.raw` tables. This is a prerequisite for the job to run successfully.

5.  **IAM Permissions**:
    *   Grant the service account used by your Cloud Composer environment (or the Airflow worker service account) the necessary BigQuery roles:
        *   `BigQuery Data Editor` on `PROJECT_ID.raw`, `PROJECT_ID.staging`, `PROJECT_ID.target`, and `PROJECT_ID.metrics` datasets.
        *   `BigQuery Job User` on the `PROJECT_ID` project.
        *   `BigQuery Data Viewer` on `PROJECT_ID.raw` dataset.

6.  **Airflow Connection**:
    *   Verify or create a `google_cloud_default` connection in your Airflow environment. This connection is typically pre-configured in Cloud Composer.

7.  **Airflow Variables**:
    *   Set the following Airflow Variables in your Cloud Composer environment:
        *   `BQ_PROJECT_ID`: Your GCP project ID (e.g., `your-gcp-project-id`).
        *   `BQ_DATASET_ID`: The BigQuery dataset ID where the stored procedure resides (e.g., `dataset` or `data_processing`).

8.  **Airflow DAG Deployment**:
    *   Upload the `k_ausd_adressen_dag.py` file to your Cloud Composer DAGs folder.
    *   Review and set the `schedule_interval` in the DAG to match the desired execution frequency.

## 5. Known gaps & unresolved references

The following items were identified during the migration design and remain as known gaps or require further follow-up:

*   **Commented-out Job Management**: The original KornShell script had commented-out calls to `FOSJobDeaktivate` and `FOSJobErzeugeEintrag`. While the migration design assumed these were intended and implemented a `metrics.job_log` table, a definitive decision on whether this functionality was truly desired or should be omitted needs to be confirmed.
*   **`v_carmen = "@pcrs1"` Clarification**: The exact nature and purpose of `@pcrs1` in the Oracle context is not fully clear. It was assumed to be an internal schema qualifier. If it represents a database link to an external system, that external system's integration with GCP needs to be addressed separately.
*   **Exact Error Code Semantics**: The original script used specific error numbers (`ErrNr=193`, `ErrNr=192`). The migrated solution uses BigQuery's `RAISE` and Airflow's logging. If precise error code mapping or custom error messages are required for downstream systems, a dedicated error handling mechanism with a lookup table might be necessary.
*   **Performance Optimization (Post-Migration)**: While BigQuery's optimizer is powerful, the original Oracle hints indicate a focus on performance. Post-migration, thorough performance testing and potential BigQuery-specific optimizations (e.g., partitioning, clustering, materialized views) might be required to match or exceed original performance, especially for large datasets.
*   **Idempotency and Restartability (`p_wiederanlaufWert`)**: The `p_wiederanlaufWert` parameter suggests a restart mechanism in the original script. The current BigQuery Stored Procedure truncates target tables before insertion, which provides a form of idempotency for a full run. However, if a partial restart from a specific point is required, the stored procedure logic might need to be enhanced to handle this more granularly.
*   **Data Type Precision**: Data types for boolean-like fields (`is_production`, `eu_indicator`, `valid`, `sales_tax_freed`) were mapped to `INT64` in BigQuery DDLs. While `INT64` can store `0` or `1`, `BOOL` might be a more semantically appropriate type if the source data strictly adheres to boolean values. This should be reviewed based on actual source data.
*   **Comprehensive Record Count Logging**: The current `job_log` only logs the record count for `sof_ta_e_regulierer`. For full validation, record counts for all target tables (`sof_ta_e_reach_gp`, `sof_ta_e_reach_re`, `sof_ta_e_reach_ev`, `sof_ta_e_reach_dn`, `sof_ta_e_business_gp`, `sof_ta_e_business_re`, `sof_ta_e_business_ev`, `sof_ta_e_business_dn`) should be captured and logged.

## 6. Validation

To validate the successful migration and execution of `k_ausd_adressen.ksh`:

### How to run the tests:

1.  **Manual BigQuery Stored Procedure Execution**:
    *   Open the BigQuery console.
    *   Navigate to your `PROJECT_ID.dataset` and find the `sp_ausd_adressen_main` stored procedure.
    *   Click "Run" and provide test parameters for `p_job_kennung`, `p_eintrags_nr`, `p_stichtag_str` (e.g., '20230101'), and `p_wiederanlauf_wert`.
    *   Monitor the job execution in the BigQuery UI.

2.  **Airflow DAG Trigger**:
    *   Access your Cloud Composer environment's Airflow UI.
    *   Find the `k_ausd_adressen_dag`.
    *   Manually trigger the DAG. You can specify a `logical_date` (execution date) if needed, which will influence the `p_stichtag_str` parameter.
    *   Monitor the DAG run in the Airflow UI, checking task logs for any errors.

### What "passing" means:

*   **Airflow DAG Success**: The `k_ausd_adressen_dag` completes successfully without any failed tasks.
*   **BigQuery Stored Procedure Success**: The `sp_ausd_adressen_main` stored procedure executes without errors in BigQuery.
*   **Data Integrity and Accuracy**:
    *   **Record Counts**: Compare the record counts in the BigQuery target tables (`PROJECT_ID.target.sof_ta_e_reach_gp`, `_re`, `_ev`, `_dn`, `sof_ta_e_business_gp`, `_re`, `_ev`, `_dn`, `sof_ta_e_regulierer`) with the expected counts from the original Oracle system for the same `p_stichtag`.
    *   **Data Samples**: Perform spot checks on data samples from the BigQuery target tables and compare them against corresponding data from the Oracle source system to ensure data transformation logic is correctly applied.
    *   **Schema Match**: Verify that the schema (column names, data types) of the BigQuery target tables matches the expected output schema.
*   **Job Logging**:
    *   Verify that an entry is created in the `PROJECT_ID.metrics.job_log` table for each execution, with the correct `job_id`, `key_date`, `status` ('SUCCESS'), `start_timestamp`, `end_timestamp`, and `record_count` (for `sof_ta_e_regulierer` at minimum, and ideally for all target tables).
*   **Error Handling**: Test with invalid `p_stichtag_str` to ensure the stored procedure raises an error and logs a 'FAILED' status in `metrics.job_log`.

## 7. Rollback procedure

In case of critical issues or failure during go-live, the following rollback procedure can be followed:

1.  **Deactivate Airflow DAG**:
    *   In the Airflow UI, toggle off the `k_ausd_adressen_dag` to prevent further scheduled executions.

2.  **Revert to Original System**:
    *   Resume the execution of the original `k_ausd_adressen.ksh` script in the Oracle environment. Ensure its scheduling and dependencies are re-established.

3.  **BigQuery Data Cleanup (Optional but Recommended)**:
    *   If the BigQuery target tables (`PROJECT_ID.target.*`) were populated incorrectly or with corrupted data, they can be truncated or dropped to prepare for a clean re-run after issues are resolved.
    *   `TRUNCATE TABLE PROJECT_ID.target.sof_ta_e_reach_gp;` (and for all other target tables)
    *   The staging tables (`PROJECT_ID.staging.*`) are truncated at the beginning of each stored procedure run, so explicit cleanup is less critical for them.

4.  **Investigate and Rectify**:
    *   Analyze the Airflow logs, BigQuery job logs, and `metrics.job_log` entries to identify the root cause of the failure.
    *   Address any identified issues in the BigQuery Stored Procedure, Airflow DAG, DDLs, or data ingestion process.

5.  **Re-deploy and Re-validate**:
    *   Once issues are resolved, re-deploy the corrected artifacts (SQL, Python DAG).
    *   Perform thorough validation steps again before attempting another go-live.