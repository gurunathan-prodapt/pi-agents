# MIGRATION_NOTES.md

## 1. Summary

The ETL job `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh` has been migrated from its original KornShell and Oracle SQL*Plus implementation to Google Cloud Platform. The target platform leverages **Google BigQuery** for data storage and transformation, and **Apache Airflow on Cloud Composer** for orchestration. This migration re-implements the process of extracting address-related data from the Customer Relationship System (CRS) for business partners and invoice recipients, providing a foundation for subsequent data processing.

## 2. Generated artifacts

The migration produced the following artifacts:

*   **`dags/r_ausd_adressen_ksh_to_bq_dag.py`**
    *   **Role:** This Python script defines an Apache Airflow DAG (Directed Acyclic Graph). It serves as the new orchestrator, replacing the functionality of the original `r_ausd_adressen.ksh` and `k_ausd_adressen.ksh` KornShell scripts. It handles parameter parsing, date calculation, and sequential execution of BigQuery SQL tasks.
*   **`sql/d_ausd_adressen_step01_truncates.sql`**
    *   **Role:** BigQuery Standard SQL script. This script performs the initial truncation of all intermediate and final target tables in BigQuery, mirroring the `TRUNCATE TABLE` operations from the original Oracle SQL*Plus script. It is executed as the first data manipulation step in the Airflow DAG.
*   **`sql/d_ausd_adressen_step02_populate_bp_ref.sql`**
    *   **Role:** BigQuery Standard SQL script. This script populates the temporary `sof_ta_bp_ref_gp`, `sof_ta_bp_ref_re`, `sof_ta_bp_ref_ev`, and `sof_ta_bp_ref_dn` tables in BigQuery. It translates the logic from Oracle SQL*Plus sections 2a-2d, extracting data for various business partner roles based on the `stichtag` parameter.
*   **`sql/d_ausd_adressen_step03_populate_country_reachability_part1.sql`**
    *   **Role:** BigQuery Standard SQL script. This script populates intermediate tables related to country information (`sof_ta_country`, `sof_ta_country_desc`, `sof_ta_laender_kng`) and reachability data (`sof_ta_reachability`). It translates Oracle SQL*Plus sections 3a, 3b, 3c, and 3e.
*   **`sql/d_ausd_adressen_step03_populate_reach_final.sql`**
    *   **Role:** BigQuery Standard SQL script. This script populates the final `sof_ta_e_reach_gp`, `sof_ta_e_reach_re`, `sof_ta_e_reach_ev`, and `sof_ta_e_reach_dn` tables. It translates Oracle SQL*Plus sections 3f, 3g, 3h, and 3i, joining intermediate `bp_ref` and `reachability` data.
*   **`sql/d_ausd_adressen_step03_cleanup_country_reachability_part2.sql`**
    *   **Role:** BigQuery Standard SQL script. This script truncates the intermediate country and reachability tables after they have been used to populate the final reachability tables. It translates Oracle SQL*Plus section 3j.
*   **`sql/d_ausd_adressen_step04_populate_business_partner_part1_gp.sql`**
    *   **Role:** BigQuery Standard SQL script. This script populates the `sof_ta_business_pt` table and the `sof_ta_bp_ref_gp_nodp` intermediate table, then the final `sof_ta_e_business_gp` table. It translates Oracle SQL*Plus sections 4a, 4b_nodp, and 4b_final.
*   **`sql/d_ausd_adressen_step04_cleanup_gp_tables.sql`**
    *   **Role:** BigQuery Standard SQL script. This script truncates intermediate tables related to contract partners (`sof_ta_bp_ref_gp_nodp`, `sof_ta_bp_ref_gp`). It translates Oracle SQL*Plus section 4c.
*   **`sql/d_ausd_adressen_step04_populate_business_partner_part2_re.sql`**
    *   **Role:** BigQuery Standard SQL script. This script populates the `sof_ta_bp_ref_re_nodp` intermediate table and then the final `sof_ta_e_business_re` table. It translates Oracle SQL*Plus sections 4d_nodp and 4d_final.
*   **`sql/d_ausd_adressen_step04_cleanup_re_tables.sql`**
    *   **Role:** BigQuery Standard SQL script. This script truncates intermediate tables related to invoice recipients (`sof_ta_bp_ref_re_nodp`, `sof_ta_bp_ref_re`). It translates Oracle SQL*Plus section 4e.
*   **`sql/d_ausd_adressen_step04_populate_business_partner_part3_ev.sql`**
    *   **Role:** BigQuery Standard SQL script. This script populates the `sof_ta_bp_ref_ev_nodp` intermediate table and then the final `sof_ta_e_business_ev` table. It translates Oracle SQL*Plus sections 4f_nodp and 4f_final.
*   **`sql/d_ausd_adressen_step04_cleanup_ev_tables.sql`**
    *   **Role:** BigQuery Standard SQL script. This script truncates intermediate tables related to EVN recipients (`sof_ta_bp_ref_ev_nodp`, `sof_ta_bp_ref_ev`). It translates Oracle SQL*Plus section 4g.
*   **`sql/d_ausd_adressen_step04_populate_business_partner_part4_dn.sql`**
    *   **Role:** BigQuery Standard SQL script. This script populates the `sof_ta_bp_ref_dn_nodp` intermediate table and then the final `sof_ta_e_business_dn` table. It translates Oracle SQL*Plus sections 4h_nodp and 4h_final.
*   **`sql/d_ausd_adressen_step04_cleanup_dn_tables.sql`**
    *   **Role:** BigQuery Standard SQL script. This script truncates intermediate tables related to service users and the main business partner table (`sof_ta_business_pt`, `sof_ta_bp_ref_dn_nodp`, `sof_ta_bp_ref_dn`). It translates Oracle SQL*Plus section 4i.
*   **`sql/d_ausd_adressen_step05_populate_regulierer.sql`**
    *   **Role:** BigQuery Standard SQL script. This script populates the final `sof_ta_e_regulierer` table. It translates Oracle SQL*Plus section 5.

## 3. Key design decisions

*   **Orchestration from KornShell to Airflow (Cloud Composer)**:
    *   **Why:** Cloud Composer provides a fully managed, scalable, and highly available Airflow environment. It allows for Python-based DAGs, offering greater flexibility, maintainability, and integration with other GCP services compared to shell scripting. This replaces the complex parameter parsing, error handling, and sequential execution logic previously handled by `r_ausd_adressen.ksh` and `k_ausd_adressen.ksh`.
    *   **Trade-offs:** Introduces a new technology stack (Python/Airflow) and requires learning new operational patterns.
*   **Data Storage and Transformation from Oracle SQL*Plus to BigQuery**:
    *   **Why:** BigQuery is a serverless, highly scalable, and cost-effective data warehouse designed for analytical workloads. It supports standard SQL, simplifying the translation of Oracle SQL logic. This eliminates the need for an on-premise Oracle database and its associated operational overhead.
    *   **Trade-offs:** Requires careful translation of Oracle-specific SQL constructs and performance tuning for BigQuery's columnar storage and distributed query engine.
*   **Parameter Handling via Airflow DAG Parameters and XComs**:
    *   **Why:** Airflow provides robust mechanisms for defining DAG parameters and passing values between tasks using XComs. This replaces the `getopts` and shell variable management from the original KornShell scripts, offering better visibility, type safety, and structured data flow.
*   **Date Logic Re-implementation in Python and BigQuery**:
    *   **Why:** Custom shell scripts like `gestern.ksh` and `h_alis_date.ksh` are replaced by native Python date functions within the Airflow DAG and BigQuery's rich set of date/time functions. This standardizes date handling and removes external script dependencies.
*   **Intermediate Tables as BigQuery Temporary Tables/Views**:
    *   **Why:** The `sof$ta_` intermediate tables in Oracle are re-created as temporary tables or views in BigQuery. BigQuery is optimized for handling large intermediate datasets efficiently, and this approach maintains the logical separation of processing steps.
*   **Error Handling and Logging via Cloud Logging/Monitoring**:
    *   **Why:** Airflow integrates seamlessly with Cloud Logging and Cloud Monitoring, providing centralized, managed logging and alerting capabilities. This replaces the custom error handling functions (`f_alis_msgerr.ksh`) from the original KornShell scripts, offering a more robust and observable solution.
*   **Removal/Replacement of Oracle SQL*Plus Specifics**:
    *   **Why:** Oracle-specific constructs such as `WHENEVER SQLERROR`, `DEFINE`, `COLUMN new_value`, `start ../trace.sql.cfg`, `spool`, `&v_carmen`, and `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` are not applicable in BigQuery. They are either removed, replaced by Airflow task management (e.g., error handling, parameter passing), or translated to direct BigQuery DDL/DML statements.
*   **BigQuery Naming Conventions**:
    *   **Why:** Oracle table names like `sof$ta_` are converted to `sof_ta_` (e.g., `sof_ta_bp_ref_gp`) to adhere to BigQuery naming best practices and avoid special characters that might require quoting.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Create the necessary BigQuery datasets:
        *   `your-gcp-project-id.staging` (for source data ingested from Oracle)
        *   `your-gcp-project-id.temp_address_processing` (for intermediate tables)
        *   `your-gcp-project-id.reporting_address_data` (for final output tables)
    *   Replace `your-gcp-project-id` with the actual GCP project ID.

2.  **IAM/Permissions Configuration**:
    *   Ensure the Cloud Composer service account (or the service account associated with the Airflow environment) has the following BigQuery roles:
        *   `BigQuery Data Editor` on the `staging`, `temp_address_processing`, and `reporting_address_data` datasets.
        *   `BigQuery Job User` to run BigQuery jobs.
    *   Verify the service account has permissions to write logs to Cloud Logging.

3.  **Source Data Ingestion**:
    *   **Crucial Prerequisite**: Establish and verify continuous data ingestion pipelines for the Oracle source tables into the BigQuery `staging` dataset. This includes:
        *   `stg_cds_bp_ref` (from `cds$ta_bp_ref`)
        *   `stg_glv_country` (from `glv$ta_country`)
        *   `stg_glv_description` (from `glv$ta_description`)
        *   `stg_bpd_reachability` (from `bpd$ta_reachability`)
        *   `stg_bpd_business_partner` (from `bpd$ta_business_partner`)
        *   `stg_cds_inv_definition` (from `cds$ta_inv_definition`)
        *   `stg_isbert_dwtk_meldungen` (from `isbert_schema.dwtk_meldungen`) - *Note: This table is critical for `stichtag` derivation and needs to be present and up-to-date.*

4.  **Airflow Connection**:
    *   Verify that the `google_cloud_default` connection is correctly configured in your Airflow environment. This connection is used by the `BigQueryExecuteQueryOperator` to authenticate with BigQuery.

5.  **Secrets Management (if applicable)**:
    *   If any sensitive parameters or credentials were used in the original KornShell scripts (beyond what's handled by `google_cloud_default`), ensure they are securely stored in Google Secret Manager and accessed by the Airflow DAG. (No explicit secrets were identified in the provided design, but this is a general best practice).

6.  **Airflow DAG Deployment**:
    *   Upload the `dags/r_ausd_adressen_ksh_to_bq_dag.py` file to the DAGs folder of your Cloud Composer environment.
    *   Upload all `sql/*.sql` files to a designated folder within your Cloud Composer environment (e.g., `dags/sql/`) and ensure the DAG references them correctly, or embed them directly as strings in the DAG as done in the generated code.

7.  **Scheduling**:
    *   Configure the `schedule_interval` in the Airflow DAG (`r_ausd_adressen_ksh_to_bq_dag.py`) to match the desired execution frequency of the original job (e.g., `@daily`).

## 5. Known gaps & unresolved references

The following items were identified during migration as requiring further attention or represent potential risks:

*   **`isbert_schema.dwtk_meldungen` logic for `stichtag`**:
    *   **Gap:** The original Oracle script derived the `v_datum` (snapshot date) from `isbert_schema.dwtk_meldungen` based on `job_kennung = 'BERT_DROP_TEMP_TABLE'`. The generated Airflow DAG currently defaults `stichtag` to the Airflow `logical_date`.
    *   **Follow-up:** A concrete implementation is needed for the `_get_processing_dates` Python function to query the `stg_isbert_dwtk_meldungen` BigQuery table (or an equivalent metadata table) to accurately replicate the `stichtag` derivation logic. This is a critical business rule.
*   **`wiederanlaufwert` parameter (`-l`)**:
    *   **Gap:** The original script supported a `-l` parameter for `wiederanlaufwert` (restart value), filtering records based on `DWH_VERTRAG_ID > Wiederanlaufwert`. The generated BigQuery SQL does not currently incorporate this dynamic filter.
    *   **Follow-up:** Confirm if this restart logic is still required. If so, the Airflow DAG needs to accept this parameter and pass it to the relevant BigQuery SQL queries as a `query_param`. This would likely involve adding a conditional `WHERE` clause to the BigQuery `INSERT` statements.
*   **`AL??` comments / FOS-Jobverwaltung**:
    *   **Gap:** The original `k_ausd_adressen.ksh` contained commented-out references to `h_alis_job.ksh` and functions like `FOSJobDeaktivate`, `FOSJobErzeugeEintrag`. These suggest a job management system.
    *   **Follow-up:** A decision is needed on whether this "FOS-Jobverwaltung" functionality is still relevant and needs to be re-implemented in the GCP environment. If so, it would likely involve a BigQuery metadata table and corresponding Airflow tasks to manage job status.
*   **Performance Optimization**:
    *   **Gap:** The original Oracle SQL used hints like `/*+ parallel(table,4) */`. While BigQuery handles parallelism automatically, the performance of the translated BigQuery SQL needs thorough validation.
    *   **Follow-up:** Monitor BigQuery query execution times and costs. Optimize queries if necessary by reviewing table partitioning/clustering, query structure, and data types.
*   **Data Validation and Error Handling**:
    *   **Gap:** The original KornShell scripts included functions like `pruefeParameterGesetzt` and `DWDate_Datum_Check`. While basic parameter parsing is in the DAG, comprehensive data validation and error handling (e.g., for invalid dates or missing parameters) should be robustly implemented in Python within the Airflow DAG.
    *   **Follow-up:** Enhance the `_get_processing_dates` function and potentially add dedicated Python tasks for parameter validation and data quality checks before BigQuery operations.
*   **Code Ownership and Business Rules**:
    *   **Gap:** The `d_ausd_adressen.sql` was marked as 'Complex' and 'Manual' migration. This implies that some business rules might be deeply embedded and required careful manual translation.
    *   **Follow-up:** Engage with business users or original developers to confirm the functional equivalence of the translated BigQuery SQL, especially for complex joins, filters, and derived columns.

## 6. Validation

To validate the successful migration and operation of the `r_ausd_adressen_ksh_to_bq_dag` job:

1.  **How to run the tests**:
    *   **Test Environment**: Deploy the DAG and SQL scripts to a dedicated non-production Cloud Composer environment with representative BigQuery staging data.
    *   **Manual Trigger**: Trigger the Airflow DAG manually from the Airflow UI.
        *   Provide a specific `logical_date` (e.g., `2023-10-26`) to simulate a daily run.
        *   If the `wiederanlaufwert` parameter is re-implemented, test with and without it.
    *   **Scheduled Run**: Enable the DAG's schedule (`@daily`) in the test environment and observe its execution over several days.
    *   **Backfill (Optional)**: If historical data processing is required, test backfilling the DAG for a range of dates.

2.  **What "passing" means**:
    *   **DAG Execution**: The Airflow DAG completes successfully without any failed tasks. All tasks should show a "success" status in the Airflow UI.
    *   **BigQuery Job Status**: All BigQuery jobs initiated by the `BigQueryExecuteQueryOperator` tasks complete successfully. This can be verified in the BigQuery UI (Job History) or Cloud Logging.
    *   **Row Counts**:
        *   Compare the row counts of the final target tables in BigQuery (e.g., `sof_ta_e_reach_gp`, `sof_ta_e_business_re`) with the corresponding output from the original Oracle job for the same `stichtag`.
        *   Verify that intermediate table truncations and populations result in expected row counts.
    *   **Data Content**:
        *   Perform spot checks on key columns and records in the final BigQuery output tables.
        *   Compare a sample of records from the BigQuery output with the corresponding records from the original Oracle job's output for functional equivalence.
        *   Focus on complex joins, conditional logic, and date transformations.
    *   **Performance Metrics**:
        *   Monitor the execution time of the overall DAG and individual BigQuery tasks. Ensure they are within acceptable SLAs.
        *   Monitor BigQuery costs (bytes processed) to ensure efficiency.
    *   **Logging and Alerting**:
        *   Verify that logs are correctly generated in Cloud Logging for both Airflow tasks and BigQuery operations.
        *   Confirm that any configured alerts (e.g., for task failures) are triggered as expected during simulated failures.

## 7. Rollback procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure should be followed:

1.  **Immediate Action: Deactivate New Job**:
    *   Access the Airflow UI for the Cloud Composer environment.
    *   Locate the `r_ausd_adressen_ksh_to_bq_dag` DAG and toggle its status to "Off" (pause the DAG). This will prevent any further runs of the new job.

2.  **Revert to Original Job**:
    *   Re-enable the original KornShell job (`r_ausd_adressen.ksh`) in the legacy environment.
    *   Ensure its scheduler is active and configured to run as before.

3.  **Data Rollback (if necessary)**:
    *   The migrated job performs truncates and inserts into its target tables (`reporting_address_data` dataset). If the new job has written incorrect data to these tables, a data rollback might be necessary.
    *   **Option A (Preferred for this job type)**: Since the original job also performs a full snapshot/overwrite, simply re-running the original KornShell job will overwrite the potentially incorrect data written by the new BigQuery job with the correct data from the Oracle source.
    *   **Option B (If Option A is not feasible or for specific tables)**:
        *   BigQuery supports table snapshots and point-in-time recovery. If a snapshot was taken before the new job ran, or if the tables are partitioned/clustered by date, it might be possible to restore specific partitions or revert the table to a previous state.
        *   Alternatively, if the impact is limited to specific tables, a targeted `DELETE` and re-insert from a known good source (e.g., a backup or the original Oracle system) could be performed.

4.  **Investigation and Remediation**:
    *   Once the rollback is complete and the original job is running, thoroughly investigate the root cause of the issue using Cloud Logging, Cloud Monitoring, and BigQuery job history.
    *   Address the identified issues in the Airflow DAG or BigQuery SQL.
    *   Re-test the corrected migration in a non-production environment before attempting another go-live.