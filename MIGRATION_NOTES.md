# MIGRATION_NOTES.md: DW.BERT_AUSD_BP_TA_BCP_MSISDN

## 1. Summary

The `DW.BERT_AUSD_BP_TA_BCP_MSISDN` job, responsible for the "preparation of instantiated basic products" related to MSISDN data for the BERT system, has been re-platformed.

**Original Platform:** Legacy Oracle database, UNIX environment, and UC4/Automic for orchestration. The job involved KornShell scripts (`r_ausd_bp_ta_bcp_msisdn.ksh`, `k_ausd_bp_ta_bcp_msisdn.ksh`) orchestrating an Oracle SQL script (`d_ausd_bp_ta_bcp_msisdn.sql`) to join data from `sof$ta_bpr_bcp` and `sof$ta_rn_vertrag` into `sof$ta_bcp_msisdn`.

**Target Platform:** Google Cloud Platform (GCP), leveraging BigQuery for data storage and transformation, and Airflow for job orchestration.

## 2. Generated Artifacts

The migration process resulted in the creation of the following files:

*   **`dags/bert_ausd_bp_ta_bcp_msisdn_dag.py`**
    *   **Role:** This is the Airflow Directed Acyclic Graph (DAG) file. It defines the entire workflow, including task dependencies, scheduling, parameter handling, and execution logic for the migrated job. It replaces the UC4 job definition and the orchestration logic previously handled by the KornShell scripts.

*   **`bigquery/d_ausd_bp_ta_bcp_msisdn.sql`**
    *   **Role:** This file contains the core data transformation logic, translated from the original Oracle SQL to BigQuery Standard SQL. It is executed by a `BigQueryOperator` within the Airflow DAG to truncate and repopulate the target `sof_ta_bcp_msisdn` table in BigQuery.

## 3. Key Design Decisions

*   **Cloud-Native Re-platforming:** The decision to migrate to GCP (BigQuery and Airflow) was driven by the need for a scalable, managed, and cost-effective data processing environment. This aligns with the broader strategy of moving away from legacy on-premise infrastructure.
*   **Airflow for Orchestration:** Airflow was chosen to replace UC4 and KornShell scripts due to its robust scheduling capabilities, Python-native environment for complex logic, extensive monitoring features, and seamless integration with other GCP services. This provides a more maintainable and observable workflow.
*   **BigQuery for Data Processing:** BigQuery was selected to replace Oracle for data storage and transformation. Its serverless architecture, automatic scaling, and high performance for analytical queries make it ideal for this ETL workload. It eliminates the need for database administration and provides cost efficiencies for large datasets.
*   **Python for Control Flow, BigQuery SQL for Data Transformation:** This approach separates concerns effectively. Python within Airflow handles job orchestration, parameter passing, date calculations, and error handling (replacing KornShell logic). BigQuery Standard SQL directly performs the data manipulation, leveraging BigQuery's optimized query engine.
*   **Removal of Oracle-Specific Features:** Oracle hints (e.g., `/*+ full(bp) parallel(bp,4) */`) and `COMMIT` statements were removed as they are not applicable or necessary in BigQuery, which handles query optimization and transaction atomicity differently.
*   **Trade-offs:**
    *   **Initial Data Migration Effort:** A significant upfront effort was required to migrate historical data from Oracle to BigQuery for all source and target tables.
    *   **Re-implementation of Utilities:** Legacy KornShell utilities (e.g., date functions, error handling) had to be re-implemented in Python, requiring careful analysis to ensure functional parity.
    *   **Loss of Direct Oracle Hints:** While BigQuery's optimizer is powerful, the explicit control offered by Oracle hints is not directly transferable, relying instead on BigQuery's internal query planning.

## 4. Manual Steps Before Go-Live

The following steps must be completed manually before the migrated job can be enabled in a production environment:

1.  **BigQuery Dataset Creation:**
    *   Create the BigQuery dataset, e.g., `your_project_id.bert_raw`, if it doesn't already exist.

2.  **BigQuery Table Schema Definition:**
    *   Define the schemas for the following tables in the `bert_raw` dataset to mirror their Oracle counterparts:
        *   `your_project_id.bert_raw.sof_ta_bpr_bcp`
        *   `your_project_id.bert_raw.sof_ta_rn_vertrag`
        *   `your_project_id.bert_raw.sof_ta_bcp_msisdn`
        *   `your_project_id.bert_raw.dwtk_meldungen`

3.  **Initial Data Migration:**
    *   Perform a one-time bulk migration of historical data from the Oracle source tables (`sof$ta_bpr_bcp`, `sof$ta_rn_vertrag`, `sof$ta_bcp_msisdn`, `isbert_schema.dwtk_meldungen`) to their respective BigQuery tables.
    *   Establish a continuous data synchronization mechanism (if required) for `dwtk_meldungen` or any other tables that are dynamically updated outside this specific job's scope.

4.  **IAM Permissions:**
    *   Ensure the Airflow service account (or the service account used by the Airflow worker/scheduler) has the necessary BigQuery permissions. This typically includes:
        *   `BigQuery Data Editor` role on the `bert_raw` dataset (for `sof_ta_bcp_msisdn`).
        *   `BigQuery Data Viewer` role on the `bert_raw` dataset (for `sof_ta_bpr_bcp`, `sof_ta_rn_vertrag`, `dwtk_meldungen`).
        *   `BigQuery Job User` role on the project.

5.  **Airflow Connections:**
    *   Verify or create an Airflow Connection for BigQuery (e.g., `google_cloud_default`) that uses the appropriate GCP Project ID and authentication method (e.g., service account key file or default application credentials).

6.  **Airflow Variables:**
    *   Define the following Airflow Variables:
        *   `bigquery_project_id`: Your GCP Project ID (e.g., `your_project_id`).
        *   `bigquery_dataset_id`: The BigQuery dataset name (e.g., `bert_raw`).
        *   `dwtk_meldungen_job_kennung_filter`: Any specific filter value if required for `dwtk_meldungen` (as per original script logic).

7.  **DAG Deployment and Scheduling:**
    *   Deploy the `dags/bert_ausd_bp_ta_bcp_msisdn_dag.py` file to your Airflow environment's DAGs folder.
    *   Enable the DAG in the Airflow UI.
    *   Confirm the `schedule_interval` in the DAG matches the desired production schedule (e.g., daily, hourly).

## 5. Known Gaps & Unresolved References

*   **`isbert_schema.dwtk_meldungen` Table Lifecycle:** The full lifecycle and update mechanism of the `dwtk_meldungen` table are not entirely visible within the scope of this job's migration. Ensuring its integrity, availability, and how it is populated and maintained in BigQuery is critical and requires further investigation.
*   **`starteSQLSkript` Complexity:** The `starteSQLSkript` function in the legacy `h_alis_sqlplus.ksh` script might contain complex logic beyond simple SQL execution (e.g., dynamic SQL generation, intricate error parsing, environment manipulation). While the core SQL was translated, any such hidden complexities would need careful analysis and replication in Python if they become apparent.
*   **Date Logic Nuances:** While Python's `datetime` module replaces legacy shell date utilities, specific edge cases (e.g., custom holiday calendars, non-standard time zone handling, or very specific date calculations) present in the original `gestern.ksh` or `DWDate_Datum_Check` might require explicit testing and verification.
*   **Error Code Mapping:** The legacy system uses specific numeric error codes (`ErrNr=193`, `ErrNr=192`). A formal mapping or strategy for how these legacy error codes translate to Airflow/Python exceptions and notification mechanisms should be defined for consistent error reporting.
*   **Restart/Recovery Mechanism (`p_wiederanlaufWert`):** The `p_wiederanlaufWert` parameter in the legacy scripts suggests a specific restart/recovery mechanism. While Airflow's retry mechanisms and BigQuery's transactional DML operations simplify recovery, the exact logic of how `p_wiederanlaufWert` prevents duplicate processing or resumes partially completed work needs to be fully understood and replicated if it implies more than standard retries.
*   **FOS Integration:** Calls to an external Job Management System (FOS - `FOSJobDeaktivate`, `FOSJobErzeugeEintrag`) were commented out in the original KornShell script and thus not migrated. If this functionality is ever required in the future, a separate integration component would need to be developed.

## 6. Validation

To ensure the successful migration and correct operation of the `DW.BERT_AUSD_BP_TA_BCP_MSISDN` job:

**How to Run Tests:**

1.  **Unit Testing:** Execute unit tests for any custom Python logic within the DAG (e.g., parameter parsing, date calculations).
2.  **Manual DAG Trigger:** In the Airflow UI, manually trigger the `bert_ausd_bp_ta_bcp_msisdn_dag` for a specific execution date.
3.  **Scheduled Run:** Allow the DAG to run according to its defined `schedule_interval`.
4.  **BigQuery Data Inspection:** After a successful DAG run, query the target BigQuery table (`your_project_id.bert_raw.sof_ta_bcp_msisdn`) to inspect the loaded data.
5.  **Log Review:** Monitor Airflow task logs and Cloud Logging for any errors, warnings, or unexpected behavior.

**What "Passing" Means:**

*   **DAG Success:** The `bert_ausd_bp_ta_bcp_msisdn_dag` completes all its tasks successfully in Airflow, with a "success" status.
*   **Data Integrity:** The `your_project_id.bert_raw.sof_ta_bcp_msisdn` table in BigQuery is truncated and repopulated with data.
*   **Data Accuracy:** The data loaded into `your_project_id.bert_raw.sof_ta_bcp_msisdn` exactly matches the expected output from the legacy Oracle job for the same execution date and input data. This typically involves comparing record counts and a sample of key data points.
*   **No Errors in Logs:** Airflow task logs and Cloud Logging show no critical errors or unhandled exceptions.
*   **Expected Record Counts:** If implemented, post-processing tasks confirm expected record counts (e.g., number of rows inserted) match the legacy job's output.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Disable New Job:** Immediately disable the `bert_ausd_bp_ta_bcp_msisdn_dag` in the Airflow UI to prevent further execution.
2.  **Re-enable Legacy Job:** Reactivate the original `DW.BERT_AUSD_BP_TA_BCP_MSISDN` job in the UC4/Automic scheduler.
3.  **Data State Consideration:**
    *   Since the migrated job performs a `TRUNCATE` and `INSERT`, any data written by the new job into `sof_ta_bcp_msisdn` will be overwritten by the next successful run of the legacy job.
    *   If the new job ran partially or incorrectly, and the data in `sof_ta_bcp_msisdn` is in an inconsistent state, consider manually truncating the BigQuery table before re-enabling the legacy job, or ensure the legacy job's first run will correctly clean up and repopulate the table.
4.  **Communication:** Inform all relevant stakeholders (e.g., data consumers, operations teams) about the rollback and the status of the job.
5.  **Investigation:** Begin a thorough investigation into the root cause of the issue that necessitated the rollback.