# MIGRATION_NOTES.md

## 1. Summary

The `DW.BERT_AUSD_BP_TA_APN_VERTRAG` job, responsible for preparing instantiated base products by processing APN (Access Point Name) and contract reference data, has been migrated.

**Original System:**
*   **Orchestration:** UC4/Automic with KornShell scripts.
*   **Data Processing:** Oracle PL/SQL.
*   **Data Storage:** Oracle Database.
*   **Execution Environment:** UNIX host.

**Target Platform:**
*   **Orchestration:** Google Cloud Composer (Apache Airflow).
*   **Data Processing:** Google BigQuery SQL.
*   **Data Storage:** Google BigQuery.
*   **Execution Environment:** Google Cloud Platform.

The migration involved re-engineering the UC4 job and KornShell scripts into an Airflow DAG, and translating the Oracle PL/SQL logic into optimized BigQuery SQL using `STRING_AGG` for efficient set-based operations.

## 2. Generated Artifacts

The migration process generates the following key artifacts:

*   **`dags/dw_bert_ausd_bp_ta_apn_vertrag_dag.py`**:
    *   **Role:** The main Airflow Directed Acyclic Graph (DAG) file. It orchestrates the entire workflow, including environment setup, parameter parsing, BigQuery table truncation, data transformation, and status updates.
*   **`bigquery/ddl/bert_stammdaten_dataset.sql`**:
    *   **Role:** BigQuery Data Definition Language (DDL) script to create the `bert_stammdaten` dataset.
*   **`bigquery/ddl/stg_sof_ta_bpr_apn_table.sql`**:
    *   **Role:** BigQuery DDL script to define the schema for the staging table `bert_stammdaten.stg_sof_ta_bpr_apn`, mirroring the Oracle `isbert_schema.sof$ta_bpr_apn` table.
*   **`bigquery/ddl/ta_apn_vertrag_table.sql`**:
    *   **Role:** BigQuery DDL script to define the schema for the target table `bert_stammdaten.ta_apn_vertrag`.
*   **`bigquery/sql/transform_apn_vertrag.sql`**:
    *   **Role:** BigQuery SQL script containing the core transformation logic. This script first truncates `bert_stammdaten.ta_apn_vertrag` and then inserts aggregated APN and contract reference data from `bert_stammdaten.stg_sof_ta_bpr_apn` using `STRING_AGG`.
*   **`python/utils/bert_apn_vertrag_helpers.py`**:
    *   **Role:** A Python module containing helper functions and classes that translate the functionalities of the original KornShell utility scripts (e.g., date calculations, parameter processing, error handling) for use within the Airflow DAG.
*   **Data Ingestion Pipeline Configuration/Scripts**:
    *   **Role:** Configuration files or scripts (e.g., DataStream job definition, Fivetran connector setup, or a custom Python script) responsible for continuously replicating or batch loading data from the Oracle source `isbert_schema.sof$ta_bpr_apn` into BigQuery `bert_stammdaten.stg_sof_ta_bpr_apn`.

## 3. Key Design Decisions

*   **Orchestration Centralization:** The disparate UC4 job and multiple KornShell scripts were consolidated into a single, cohesive Airflow DAG. This simplifies scheduling, dependency management, monitoring, and logging within a unified cloud-native framework.
*   **Set-Based BigQuery Transformation:** The original Oracle PL/SQL's cursor-based row-by-row processing with string concatenation was re-engineered into an efficient, set-based BigQuery SQL query utilizing `STRING_AGG`. This leverages BigQuery's columnar storage and distributed processing capabilities for superior performance and scalability.
*   **Deterministic Aggregation:** The `ORDER BY` clause within `STRING_AGG` was explicitly included (`ORDER BY access_point_name`, `ORDER BY cntrct_id_ref`) to ensure deterministic output for the concatenated strings, mimicking predictable behavior even if the original Oracle implicit order was stable.
*   **Character Limit Emulation:** `SUBSTR` was used in BigQuery to truncate the aggregated strings to 100 characters, directly replicating the original Oracle PL/SQL's behavior for `v_apn` and `v_cntrct_ref`.
*   **Staging Table Approach:** A dedicated staging table (`bert_stammdaten.stg_sof_ta_bpr_apn`) was introduced in BigQuery to decouple the ingestion of source Oracle data from the transformation logic. This allows for flexible data loading strategies and provides a consistent input for the Airflow DAG.
*   **Python for Utility Translation:** Instead of attempting to port KornShell scripts directly, their core functionalities (parameter parsing, date calculations, custom error handling) were reimplemented in Python. This integrates seamlessly with Airflow's Python-native environment and allows for better maintainability and testability.
*   **Cloud-Native Services:** Leveraging Google Cloud Composer for orchestration and BigQuery for data storage and processing aligns with a cloud-native strategy, providing managed services, scalability, and integration with other GCP tools.

## 4. Manual Steps Before Go-Live

The following steps must be completed manually before the migrated job can go live:

1.  **BigQuery Dataset Creation:**
    *   Create the `bert_stammdaten` dataset in the target Google Cloud project.
    *   Execute the DDL for `bert_stammdaten.stg_sof_ta_bpr_apn` and `bert_stammdaten.ta_apn_vertrag` to define the table schemas.
2.  **IAM & Permissions Configuration:**
    *   Ensure the Google Cloud service account associated with the Airflow Composer environment has the necessary BigQuery roles (e.g., `BigQuery Data Editor`, `BigQuery Job User`) to create tables, truncate, insert, and query data in the `bert_stammdaten` dataset.
    *   If using DataStream or another ingestion tool, ensure its service account has appropriate permissions to read from the Oracle source and write to BigQuery.
3.  **Oracle Source Connection & Secrets:**
    *   Configure secure access to the Oracle source database for the data ingestion pipeline. This may involve setting up connection strings, firewall rules, and storing credentials securely (e.g., in Google Secret Manager) for the ingestion tool.
4.  **Data Ingestion Pipeline Setup:**
    *   Deploy and configure the chosen data ingestion pipeline (e.g., DataStream, Fivetran, custom script) to continuously or regularly load data from Oracle `isbert_schema.sof$ta_bpr_apn` into BigQuery `bert_stammdaten.stg_sof_ta_bpr_apn`. This pipeline must be operational and verified before the Airflow DAG runs.
5.  **Airflow DAG Deployment:**
    *   Upload the `dags/dw_bert_ausd_bp_ta_apn_vertrag_dag.py` file and any dependent Python modules (`python/utils/bert_apn_vertrag_helpers.py`) to the Airflow DAGs folder in the Cloud Composer environment.
6.  **Airflow Variables/Connections:**
    *   Configure any necessary Airflow Variables (e.g., `project_id`, `oracle_connection_id` if a custom ingestion is part of the DAG) or Airflow Connections (e.g., for BigQuery) that the DAG might use.
7.  **Scheduling Configuration:**
    *   Verify that the `schedule_interval` defined in the Airflow DAG matches the original UC4 job's execution frequency.
8.  **Monitoring & Alerting Setup:**
    *   Configure appropriate monitoring and alerting for the Airflow DAG, integrating with Google Cloud Monitoring and Logging, to ensure timely notification of failures or performance issues.

## 5. Known Gaps & Unresolved References

The following items have been identified as potential gaps or require further investigation/resolution:

*   **Complex PL/SQL Conversion (B4 Item):** The original Oracle PL/SQL script was flagged as `complex` and `manual`. While the `STRING_AGG` approach addresses the primary aggregation, there might be subtle business logic, edge cases, or specific error handling within the original cursor loop that is not fully captured by the provided snippet. A thorough code review of the original PL/SQL by a domain expert is recommended to ensure complete functional parity.
*   **`p_wiederanlaufWert` (Resume Value) Strategy:** The mechanism for `p_wiederanlaufWert` (resume value) for restartability needs a robust BigQuery/Airflow equivalent. This could involve implementing a BigQuery watermark table, leveraging Airflow's XComs for state, or a custom resume logic within the Python operators. This is a critical aspect for operational stability.
*   **Oracle-Specific Function Mapping:** The design document mentions `NVL`, `TO_CHAR`, and `MAX(m.timecreated)` from `isbert_schema.dwtk_meldungen`. The exact usage and context of these functions, especially `MAX(m.timecreated)` for `v_datum`, need to be fully understood and accurately mapped to BigQuery's equivalent date/time functions and the corresponding BigQuery schema for `dwtk_meldungen` (if it's also migrated).
*   **`starteSQLSkript` Full Functionality:** The `starteSQLSkript` function within `h_alis_sqlplus.ksh` likely encapsulates more than just SQL execution (e.g., specific error code handling, logging, parameter sanitization). A detailed analysis of this function is required to ensure all its functionalities are faithfully replicated in the Python operators or BigQuery tasks.
*   **Data Volume/Performance for `STRING_AGG`:** While `STRING_AGG` is efficient, if `sof$ta_bpr_apn` contains an extremely high number of rows per `cntrct_id` or the overall table size is massive, the performance of the `STRING_AGG` operation should be closely monitored. Partitioning or clustering `stg_sof_ta_bpr_apn` by `cntrct_id` might be beneficial for very large datasets.
*   **Custom Error Handling and Logging Framework:** The original KornShell scripts used a custom error handling (`f_alis_msgerr.ksh`, `DWMSG_MeldeFehler`) and logging (`DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`) framework. While Airflow's native logging and alerting are used, ensuring that all critical error conditions and log messages from the original system are captured and translated appropriately is important for operational continuity.
*   **`file_complexity` and `automation_rate` Gaps:** The absence of detailed automated analysis for `file_complexity` and `automation_rate` implies that some migration challenges might only become apparent during manual code review and testing. This necessitates a more thorough manual validation effort.

## 6. Validation

Validation ensures the migrated job functions correctly and produces accurate results.

**How to Run Tests:**

1.  **Unit Tests:** Execute Python unit tests for `python/utils/bert_apn_vertrag_helpers.py` and any other custom Python code within the DAG to verify individual component logic.
2.  **Airflow DAG Local Test:** Use Airflow's local testing capabilities (`airflow dags test`) to simulate DAG runs and verify task dependencies and basic execution flow.
3.  **Integration Testing (Development Environment):**
    *   Deploy the Airflow DAG to a development Cloud Composer environment.
    *   Ensure the data ingestion pipeline is populating `bert_stammdaten.stg_sof_ta_bpr_apn` with representative data from the Oracle source.
    *   Trigger the Airflow DAG manually or allow it to run on its schedule.
    *   Monitor Airflow logs and BigQuery job history for successful completion and any errors.
4.  **Data Validation (Comparison):**
    *   After a successful run in the development environment, extract data from the BigQuery target table (`bert_stammdaten.ta_apn_vertrag`).
    *   Concurrently, run the original Oracle job and extract data from `isbert_schema.sof$ta_apn_vertrag` for the same processing period/parameters.
    *   Perform a row-by-row comparison or aggregate checksums between the BigQuery and Oracle outputs to verify data accuracy and completeness. Tools like `dbt_utils.audit_helper` or custom Python scripts can facilitate this.
5.  **Performance Testing:**
    *   Run the Airflow DAG with production-like data volumes in a staging environment.
    *   Monitor BigQuery query execution times and Airflow task durations to ensure performance meets or exceeds original Oracle job SLAs.

**What "Passing" Means:**

*   **Successful DAG Runs:** The `dw_bert_ausd_bp_ta_apn_vertrag_dag` completes successfully in Airflow without any task failures or retries.
*   **Data Accuracy:** The data in BigQuery `bert_stammdaten.ta_apn_vertrag` is identical (or functionally equivalent, considering data type mappings) to the data produced by the original Oracle job in `isbert_schema.sof$ta_apn_vertrag` for the same input data and processing period.
*   **Data Completeness:** All expected records are present in the BigQuery target table.
*   **Performance:** The Airflow DAG and BigQuery transformations complete within acceptable timeframes, ideally matching or improving upon the original job's execution duration.
*   **Logging & Alerting:** All expected logs are generated in Google Cloud Logging, and any configured alerts for failures or anomalies are triggered correctly.
*   **Resource Utilization:** BigQuery slot usage and Airflow worker resources are within expected and cost-effective limits.

## 7. Rollback Procedure

In the event of critical issues identified post-migration or during the cutover phase, the following rollback procedure can be initiated:

1.  **Stop Airflow DAG:** Immediately pause or delete the `dw_bert_ausd_bp_ta_apn_vertrag_dag` in Google Cloud Composer to prevent further execution of the migrated job.
2.  **Revert Scheduling to Original System:** Ensure the original UC4 job `DW.BERT_AUSD_BP_TA_APN_VERTRAG.xml` is re-enabled and scheduled to run according to its original frequency.
3.  **Verify Original System Operation:** Confirm that the Oracle job is running successfully and producing the expected output in `isbert_schema.sof$ta_apn_vertrag`.
4.  **Data Consistency Check (Optional but Recommended):** If the migrated job ran for a period, compare the data in the Oracle target table with the BigQuery target table to understand any discrepancies that might have occurred. This helps in diagnosing the rollback reason.
5.  **Cleanup (Post-Rollback):**
    *   If the rollback is deemed permanent, the BigQuery dataset `bert_stammdaten` and its tables (`stg_sof_ta_bpr_apn`, `ta_apn_vertrag`) can be deleted.
    *   The Airflow DAG and associated Python files can be removed from the Composer environment.
    *   Any associated IAM roles, service accounts, or ingestion pipeline configurations created specifically for this migration can be de-provisioned.

**Note:** The original Oracle system should remain fully operational and capable of running the job until the migrated solution is thoroughly validated and a successful cutover is confirmed. This "dark launch" or "parallel run" strategy minimizes risk during the migration.