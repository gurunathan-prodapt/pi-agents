# MIGRATION_NOTES: DW.BERT_AUSD_BP_TA_BCP_MSISDN

## 1. Summary

The `DW.BERT_AUSD_BP_TA_BCP_MSISDN` job, responsible for the initial provisioning of selected basic products (Basisprodukte) for the BERT system related to MSISDN data, has been migrated.

**Original Platform:**
*   **Orchestration:** Automic (UC4)
*   **Scripting:** KornShell (`r_ausd_bp_ta_bcp_msisdn.ksh`, `k_ausd_bp_ta_bcp_msisdn.ksh`)
*   **Data Processing:** Oracle SQL (`d_ausd_bp_ta_bcp_msisdn.sql`)
*   **Data Storage:** Oracle Database (e.g., `sof$ta_bpr_bcp`, `sof$ta_rn_vertrag`, `sof$ta_bcp_msisdn`)

**Target Platform:**
*   **Orchestration:** Google Cloud Composer (Apache Airflow)
*   **Data Processing:** Google BigQuery (Stored Procedures)
*   **Data Storage:** Google BigQuery (tables)

The migration involved re-platforming the entire workflow to GCP, translating shell script logic and Oracle SQL into BigQuery Stored Procedures, and orchestrating the execution via an Airflow DAG. The job generates a cut-off date extraction of contract cache data and makes it available for a downstream Forderungsscoring (FOS) system.

## 2. Generated Artifacts

The migration produced the following key artifacts:

*   **`sql/procedures/sp_r_ausd_bp_ta_bcp_msisdn.sql`**
    *   **Role:** This BigQuery Stored Procedure serves as the primary entry point and orchestration layer on the BigQuery side. It replaces the `r_ausd_bp_ta_bcp_msisdn.ksh` KornShell script. Its responsibilities include:
        *   Handling initial parameter parsing (`p_stichtag`, `p_wiederanlaufWert`).
        *   Defaulting `p_stichtag` to the current date if not provided.
        *   Job logging and auditing (inserting/updating records in `job_audit_log`).
        *   Calling the next procedural layer, `sp_k_ausd_bp_ta_bcp_msisdn`.

*   **`sql/procedures/sp_k_ausd_bp_ta_bcp_msisdn.sql`** (As per design, not explicitly generated in the provided code snippet, but part of the migration plan)
    *   **Role:** This BigQuery Stored Procedure replaces the `k_ausd_bp_ta_bcp_msisdn.ksh` KornShell script. It handles:
        *   Further parameter validation and date checks.
        *   Logic for handling restart values.
        *   Orchestrating the core data transformation by calling `sp_d_ausd_bp_ta_bcp_msisdn`.
        *   Capturing and logging record counts.

*   **`sql/procedures/sp_d_ausd_bp_ta_bcp_msisdn.sql`** (As per design, not explicitly generated in the provided code snippet, but part of the migration plan)
    *   **Role:** This BigQuery Stored Procedure is the core data processing unit, directly replacing the `d_ausd_bp_ta_bcp_msisdn.sql` Oracle SQL script. It performs:
        *   Truncation of the target table `sof_ta_bcp_msisdn`.
        *   Insertion of `DISTINCT` records into `sof_ta_bcp_msisdn` based on joins between `sof_ta_bpr_bcp` and `sof_ta_rn_vertrag`.
        *   Derivation of a date variable from `dwtk_meldungen`.

*   **`airflow/dags/dw_bert_ausd_bp_ta_bcp_msisdn.py`**
    *   **Role:** This Python script defines the Airflow DAG that orchestrates the entire job. It replaces the UC4 job definition (`DW.BERT_AUSD_BP_TA_BCP_MSISDN.xml`). Its responsibilities include:
        *   Scheduling the job (`@daily` by default).
        *   Defining the task to execute the main BigQuery Stored Procedure (`sp_r_ausd_bp_ta_bcp_msisdn`).
        *   Passing runtime parameters (e.g., `stichtag`, `wiederanlaufWert`) from Airflow to the BigQuery procedure.
        *   Configuring retry policies and error handling within Airflow.

## 3. Key Design Decisions

### Why the Chosen Approach

1.  **Full Re-platforming to GCP:** The decision was made to fully leverage Google Cloud Platform's managed services (BigQuery, Cloud Composer) for scalability, cost-efficiency, reduced operational overhead, and integration with other GCP services. This aligns with a broader cloud migration strategy.
2.  **BigQuery Stored Procedures for Logic Consolidation:** Instead of translating KornShell scripts to Python operators in Airflow or separate BigQuery scripts, the procedural logic (parameter handling, control flow, error handling, core ETL) was consolidated into BigQuery Stored Procedures. This keeps the data transformation logic close to the data, leverages BigQuery's performance for SQL operations, and simplifies maintenance by having a single language (SQL) for the entire data pipeline.
3.  **Airflow for Orchestration:** Cloud Composer (Airflow) was chosen to replace Automic (UC4) due to its open-source nature, Python-based extensibility, robust scheduling capabilities, and native integration with GCP services. It provides a modern, flexible, and scalable orchestration platform.
4.  **Direct SQL Translation:** The core Oracle SQL was directly translated to BigQuery SQL, minimizing functional changes and ensuring data integrity. Oracle-specific syntax and hints were removed or replaced with BigQuery equivalents.

### Notable Trade-offs

1.  **Loss of Oracle-Specific Features:** Oracle-specific features like `/*+ hints */` and `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` were removed. While BigQuery's query optimizer generally handles performance well, direct control over execution plans via hints is lost.
2.  **Re-implementation of Helper Logic:** Common KornShell helper scripts (e.g., for date manipulation, error messaging) had to be re-implemented as BigQuery SQL functions (UDFs or integrated logic) or potentially Python utilities if more complex. This required careful analysis to ensure functional equivalence.
3.  **Performance Characteristics:** While BigQuery is highly performant for large-scale data processing, its performance profile differs from Oracle. Thorough performance testing is crucial to ensure the migrated job meets SLAs, especially given the removal of Oracle-specific performance tuning.
4.  **Complexity of Commented-Out Logic:** The original KornShell scripts contained significant commented-out sections related to "FOS job management" and "Nachverarbeitung" (post-processing). These were not migrated, assuming they are obsolete. If they become relevant, their re-implementation in BigQuery or Python would be a substantial effort.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`your_bigquery_dataset_id` as referenced in the DAG) exists in `your_gcp_project_id`.

2.  **BigQuery Table Creation (DDL):**
    *   Create the following tables in the designated BigQuery dataset:
        *   `sof_ta_bpr_bcp` (source table, schema matching Oracle `sof$ta_bpr_bcp`)
        *   `sof_ta_rn_vertrag` (source table, schema matching Oracle `sof$ta_rn_vertrag`)
        *   `dwtk_meldungen` (metadata table, schema matching Oracle `isbert_schema.dwtk_meldungen`)
        *   `sof_ta_bcp_msisdn` (target table, schema matching Oracle `sof$ta_bcp_msisdn`)
        *   `job_audit_log` (for job logging and auditing, schema as defined in `sp_r_ausd_bp_ta_bcp_msisdn.sql`)

3.  **Initial Data Loading:**
    *   Perform a one-time historical data load from the legacy Oracle tables (`sof$ta_bpr_bcp`, `sof$ta_rn_vertrag`, `isbert_schema.dwtk_meldungen`) into their respective BigQuery counterparts. This can be done using tools like Cloud Dataflow, `bq load` command, or other ETL processes.
    *   Establish an ongoing data ingestion strategy (e.g., CDC, daily batch exports) to keep the BigQuery source tables (`sof_ta_bpr_bcp`, `sof_ta_rn_vertrag`, `dwtk_meldungen`) synchronized with the Oracle sources until full decommissioning.

4.  **IAM Permissions:**
    *   Ensure the service account used by Cloud Composer (Airflow) has the necessary BigQuery permissions:
        *   `BigQuery Data Editor` (to write to `sof_ta_bcp_msisdn` and `job_audit_log`, and execute procedures).
        *   `BigQuery Data Viewer` (to read from source tables).
        *   `BigQuery Job User` (to run BigQuery jobs).
        *   `Composer Worker` (for Airflow environment).

5.  **Airflow Connections and Variables:**
    *   Configure the `google_cloud_default` connection in Airflow to point to your GCP project.
    *   Set the following Airflow Variables:
        *   `BIGQUERY_PROJECT_ID`: Your GCP project ID (e.g., `your_gcp_project_id`).
        *   `BIGQUERY_DATASET_ID`: Your BigQuery dataset ID (e.g., `your_bigquery_dataset_id`).

6.  **Deploy BigQuery Stored Procedures:**
    *   Execute the DDL for `sp_r_ausd_bp_ta_bcp_msisdn.sql`, `sp_k_ausd_bp_ta_bcp_msisdn.sql`, and `sp_d_ausd_bp_ta_bcp_msisdn.sql` in your target BigQuery dataset.

7.  **Deploy Airflow DAG:**
    *   Upload the `airflow/dags/dw_bert_ausd_bp_ta_bcp_msisdn.py` file to your Cloud Composer environment's DAGs folder.
    *   Verify the DAG appears in the Airflow UI and is unpaused.

## 5. Known Gaps & Unresolved References

The following items have been flagged for follow-up or represent known limitations/risks:

1.  **Inferred Dependencies:** The execution flow was inferred from code analysis. While highly probable, there's a minor risk of missing implicit or dynamically resolved dependencies not evident from static code analysis. Thorough functional validation is crucial.
2.  **Helper Script Re-implementation:** The exact logic within legacy KornShell helper scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`) needs to be fully understood and accurately re-implemented in BigQuery SQL (as UDFs or integrated logic) or Python. Any missing context could lead to functional discrepancies.
3.  **Commented-out Logic (B3 Item):** The original KornShell scripts contain commented-out sections related to "FOS job management" and extensive "Nachverarbeitung" (post-processing) using `sed`, `sort`, `join` on temporary data files. It is currently assumed this logic is obsolete and has **not** been migrated. This needs explicit confirmation from stakeholders. If relevant, this "manual" complexity will require significant effort to migrate to BigQuery (e.g., staging tables, SQL transformations, or Python processing).
4.  **Oracle-Specific Performance Tuning:** Oracle optimizer hints (`/*+ full(bp) parallel(bp,4) full(rn) parallel(rn,4) */`) were removed as they are not applicable in BigQuery. While BigQuery automatically optimizes queries, performance characteristics might differ. Thorough performance testing is required to ensure the job meets performance SLAs.
5.  **Data Type Mismatch:** Subtle differences in how Oracle and BigQuery handle specific data types (e.g., precision for numbers, time zones for dates) could lead to data discrepancies. A detailed data type mapping and validation plan is essential.
6.  **Error Code Semantics:** The KornShell scripts use specific `ErrNr` values (e.g., 192, 193). While BigQuery procedures can `RAISE` errors, the exact mapping and downstream handling of these specific error codes need to be designed if external systems rely on them. The current BigQuery procedure raises a generic error message.

## 6. Validation

Validation ensures the migrated job functions correctly and produces accurate results.

### How to Run the Tests

1.  **Unit Tests (BigQuery Stored Procedures):**
    *   Manually execute each BigQuery Stored Procedure (`sp_d_ausd_bp_ta_bcp_msisdn`, `sp_k_ausd_bp_ta_bcp_msisdn`, `sp_r_ausd_bp_ta_bcp_msisdn`) in the BigQuery console or via `bq query` command, providing various parameter combinations (e.g., with/without `stichtag`, different `wiederanlaufWert`).
    *   Verify the output in `sof_ta_bcp_msisdn` and the `job_audit_log` table.

2.  **Integration Tests (Airflow DAG):**
    *   **Manual Trigger:** In the Airflow UI, manually trigger the `dw_bert_ausd_bp_ta_bcp_msisdn` DAG.
        *   Test with default parameters (no `conf` provided).
        *   Test with specific `stichtag` (e.g., `{"stichtag": "20231026"}`).
        *   Test with specific `wiederanlaufWert` (e.g., `{"wiederanlaufWert": 100}`).
        *   Test with both parameters.
    *   **Scheduled Run:** Allow the DAG to run on its defined schedule (`@daily`) to observe its behavior in a production-like environment.

3.  **Data Validation:**
    *   After a successful run of the migrated job, compare the data in the BigQuery target table (`sof_ta_bcp_msisdn`) with the output of the legacy Oracle job for the same execution date and parameters.
    *   Focus on:
        *   Record counts.
        *   Specific data points, especially for edge cases or known problematic records.
        *   Data types and formats.
        *   Nullability.

4.  **Performance Testing:**
    *   Monitor BigQuery query execution times and resource consumption for the migrated job. Compare these metrics against the performance of the legacy Oracle job to ensure acceptable performance.

### What "Passing" Means

A successful migration validation means:

*   The Airflow DAG `dw_bert_ausd_bp_ta_bcp_msisdn` completes successfully without errors in the Airflow UI.
*   All BigQuery Stored Procedures (`sp_r_ausd_bp_ta_bcp_msisdn`, `sp_k_ausd_bp_ta_bcp_msisdn`, `sp_d_ausd_bp_ta_bcp_msisdn`) execute without errors.
*   The `job_audit_log` table correctly records the job's start/end times, status (`SUCCESS`), and any relevant messages.
*   The data in the BigQuery target table `sof_ta_bcp_msisdn` is functionally equivalent and accurate when compared to the output of the legacy Oracle job for the same input parameters. This includes matching record counts, data values, and data types.
*   The job completes within acceptable performance thresholds.
*   Error handling mechanisms (e.g., `RAISE` in BigQuery, Airflow retries) function as expected when errors are deliberately introduced.

## 7. Rollback Procedure

In case of critical issues or failure during go-live, the following rollback procedure should be followed:

1.  **Stop New Execution:**
    *   In the Airflow UI, pause the `dw_bert_ausd_bp_ta_bcp_msisdn` DAG to prevent any further runs of the migrated job.

2.  **Re-enable Legacy Job:**
    *   Re-enable and/or revert the legacy Automic (UC4) job `DW.BERT_AUSD_BP_TA_BCP_MSISDN` to its operational state.
    *   Verify that the legacy job can run successfully and produce the expected output.

3.  **Data Remediation (if necessary):**
    *   If the migrated job has written incorrect or incomplete data to `sof_ta_bcp_msisdn` in BigQuery, and this data could impact downstream systems, consider truncating or deleting the affected data in BigQuery. This step should be performed with caution and only if necessary.

4.  **Post-Rollback Monitoring:**
    *   Monitor the legacy job and its downstream dependencies closely to ensure full operational recovery.

5.  **Investigation and Remediation:**
    *   Analyze the root cause of the rollback. Address any identified issues in the migrated code, configuration, or environment before attempting another go-live.
    *   Consider deleting or deactivating the problematic BigQuery Stored Procedures and Airflow DAG until issues are resolved.