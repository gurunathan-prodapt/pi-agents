# MIGRATION_NOTES.md

## 1. Summary

The KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh` has been migrated. This script served as an orchestration wrapper for a core data reconciliation process related to the `ta_p_discount_rr` table, handling environment setup, parameter parsing, custom logging, and invoking a core processing script (`k_ausd_v_ta_p_discount_rr.ksh`).

The migration target is Google Cloud Platform, leveraging **BigQuery Stored Procedures** for the core logic and wrapper functionality, and **Cloud Composer (Apache Airflow)** for orchestration, scheduling, and monitoring. The file-based logging mechanism has been replaced by a centralized BigQuery logging table.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`sql/ddl/job_log.sql`**
    *   **Role:** Data Definition Language (DDL) script to create the `job_log` BigQuery table. This table serves as the centralized logging repository for all job executions, capturing status, parameters, and errors, replacing the legacy file-based logging.
*   **`sql/procedures/DWMSG_ErmittleNr.sql`**
    *   **Role:** BigQuery Stored Procedure (BSP) that mimics the legacy `DWMSG_ErmittleNr` function. It generates a unique identifier for each job run, which is used as `job_id` in the `job_log` table.
*   **`sql/procedures/DWMSG_Logdateiname.sql`**
    *   **Role:** BigQuery Stored Procedure that provides a conceptual log identifier. In the BigQuery context, there are no physical log files, so this procedure generates a string that can be used to query relevant entries from the `job_log` table.
*   **`sql/procedures/DWMSG_ErzeugeEintrag.sql`**
    *   **Role:** BigQuery Stored Procedure for logging the start of a job or significant events into the `job_log` table with an 'Info' severity.
*   **`sql/procedures/DWMSG_SetzeStichtagInfo.sql`**
    *   **Role:** BigQuery Stored Procedure to log critical reference date information (`Stichtag`) for the job into the `job_log` table.
*   **`sql/procedures/DWMSG_SetzeStatusOK.sql`**
    *   **Role:** BigQuery Stored Procedure to log a successful completion status for a job run into the `job_log` table.
*   **`sql/procedures/DWMSG_Fehlerbehandlung.sql`**
    *   **Role:** BigQuery Stored Procedure designed to handle and log errors. It captures BigQuery's native error information (code, message) and inserts it into the `job_log` table with an 'Error' severity.
*   **`sql/procedures/k_ausd_v_ta_p_discount_rr.sql`**
    *   **Role:** Placeholder BigQuery Stored Procedure for the core business logic originally contained in `k_ausd_v_ta_p_discount_rr.ksh`. This procedure currently only logs its invocation and requires detailed implementation based on the analysis of the original core script.
*   **`sql/procedures/Vertragsdatenabgleich.sql`**
    *   **Role:** The main BigQuery Stored Procedure that encapsulates the wrapper logic of `r_ausd_v_ta_p_discount_rr.ksh`. It handles parameter validation, orchestrates calls to the `DWMSG_` logging procedures, and invokes the core `k_ausd_v_ta_p_discount_rr` BSP.
*   **`dags/r_ausd_v_ta_p_discount_rr_dag.py`**
    *   **Role:** An Apache Airflow DAG (Directed Acyclic Graph) written in Python. This DAG is responsible for scheduling, orchestrating, and monitoring the execution of the `Vertragsdatenabgleich` BigQuery Stored Procedure, passing necessary parameters.

## 3. Key Design Decisions

*   **Orchestration with Cloud Composer (Airflow):**
    *   **Why:** Airflow provides robust scheduling capabilities, dependency management, retry mechanisms, and a rich UI for monitoring job status. It allows for centralized control and visibility over data pipelines, replacing the ad-hoc shell-based scheduling.
    *   **Trade-offs:** Introduces a new technology stack (Python, Airflow concepts) and requires managing a Composer environment.
*   **Logic Migration to BigQuery Stored Procedures (BSPs):**
    *   **Why:** BigQuery's SQL-native stored procedures are ideal for encapsulating data transformation logic directly within the data warehouse. This leverages BigQuery's performance, scalability, and eliminates the need to move data out for processing. It also provides transactional error handling (`BEGIN...EXCEPTION`).
    *   **Trade-offs:** Requires translating shell scripting logic and custom functions into BigQuery SQL, which can be complex for intricate shell operations or external system interactions.
*   **Centralized Logging in BigQuery Table (`job_log`):**
    *   **Why:** Replaces disparate file-based logs with a structured, queryable, and scalable logging solution. This enables easier auditing, monitoring, and analysis of job execution history and errors using BigQuery's analytical capabilities.
    *   **Trade-offs:** Requires re-implementing all `DWMSG_` functions as BigQuery Stored Procedures that interact with this table.
*   **Parameter Handling:**
    *   **Why:** Command-line parameters (`getopts`) from the KSH script are directly mapped to input parameters of the BigQuery Stored Procedure. Airflow DAG parameters provide a structured way to pass these values at runtime, enhancing clarity and validation.
    *   **Trade-offs:** Requires careful mapping of data types and validation logic from shell to BigQuery SQL.
*   **Error Handling:**
    *   **Why:** The `trap` statements and custom `DWMSG_` error functions are replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN ... END` blocks, combined with dedicated `DWMSG_Fehlerbehandlung` BSP. This provides structured error capture and logging within the BigQuery environment.
    *   **Trade-offs:** Requires understanding BigQuery's error handling semantics and ensuring all potential failure points are covered.
*   **Core Logic (`k_ausd_v_ta_p_discount_rr.ksh`) as a Separate BSP:**
    *   **Why:** Decoupling the orchestration logic from the core business logic promotes modularity and reusability. The wrapper BSP (`Vertragsdatenabgleich`) can call the core BSP (`k_ausd_v_ta_p_discount_rr`), making the overall solution cleaner and easier to maintain.
    *   **Trade-offs:** The core script's migration is a prerequisite and its complexity directly impacts the overall migration effort.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Ensure the BigQuery dataset `my_bq_dataset` exists within `my_gcp_project`. If not, create it:
        ```bash
        bq mk --project_id=my_gcp_project my_bq_dataset
        ```
2.  **`job_log` Table Deployment:**
    *   Execute the DDL script `sql/ddl/job_log.sql` in BigQuery to create the centralized logging table.
        ```bash
        bq query --project_id=my_gcp_project --use_legacy_sql=false < sql/ddl/job_log.sql
        ```
3.  **`DWMSG_` Procedures Deployment:**
    *   Deploy all `DWMSG_` BigQuery Stored Procedures (`DWMSG_ErmittleNr.sql`, `DWMSG_Logdateiname.sql`, `DWMSG_ErzeugeEintrag.sql`, `DWMSG_SetzeStichtagInfo.sql`, `DWMSG_SetzeStatusOK.sql`, `DWMSG_Fehlerbehandlung.sql`) to `my_gcp_project.my_bq_dataset`.
        ```bash
        bq query --project_id=my_gcp_project --use_legacy_sql=false < sql/procedures/DWMSG_ErmittleNr.sql
        # ... repeat for all DWMSG_ procedures ...
        ```
4.  **Core Logic (`k_ausd_v_ta_p_discount_rr`) Implementation and Deployment:**
    *   **Crucially, the `sql/procedures/k_ausd_v_ta_p_discount_rr.sql` file is currently a placeholder.** The actual business logic from the original `k_ausd_v_ta_p_discount_rr.ksh` must be fully analyzed, translated into BigQuery SQL, and implemented within this stored procedure.
    *   Once implemented, deploy this procedure to `my_gcp_project.my_bq_dataset`.
        ```bash
        bq query --project_id=my_gcp_project --use_legacy_sql=false < sql/procedures/k_ausd_v_ta_p_discount_rr.sql
        ```
5.  **Main Procedure (`Vertragsdatenabgleich`) Deployment:**
    *   Deploy the `sql/procedures/Vertragsdatenabgleich.sql` BigQuery Stored Procedure to `my_gcp_project.my_bq_dataset`.
        ```bash
        bq query --project_id=my_gcp_project --use_legacy_sql=false < sql/procedures/Vertragsdatenabgleich.sql
        ```
6.  **IAM / Permissions:**
    *   Ensure the Google Cloud Service Account used by your Cloud Composer environment has the necessary BigQuery roles:
        *   `BigQuery Data Editor` (for `my_gcp_project.my_bq_dataset`) to create/update tables and insert data.
        *   `BigQuery Job User` to run queries and stored procedures.
        *   `BigQuery Metadata Viewer` to view dataset/table metadata.
7.  **Cloud Composer Environment Setup:**
    *   Upload the `dags/r_ausd_v_ta_p_discount_rr_dag.py` file to the DAGs folder of your Cloud Composer environment.
    *   **Airflow Variables:** Configure Airflow Variables or connections if `PROJECT_ID` and `DATASET_ID` are not hardcoded in the DAG or need to be dynamic.
    *   **Scheduling:** Configure the desired schedule for the `r_ausd_v_ta_p_discount_rr` DAG in the Airflow UI.
8.  **Secrets Management:**
    *   If any sensitive parameters (e.g., API keys, database credentials) were part of the original script or are introduced in the core logic, ensure they are securely managed using Airflow Secrets Backend (e.g., Google Secret Manager).

## 5. Known Gaps & Unresolved References

*   **Core Logic Implementation (`k_ausd_v_ta_p_discount_rr.sql`):** This is the most significant gap. The generated `k_ausd_v_ta_p_discount_rr.sql` is a placeholder. A detailed analysis and full implementation of the original `k_ausd_v_ta_p_discount_rr.ksh` script's business logic into BigQuery SQL is absolutely required before go-live. The complexity of this implementation is currently unknown.
*   **Full `DWMSG_` Framework Fidelity:** While the generated `DWMSG_` procedures provide basic logging, a deeper dive into the exact behavior and any complex logic within the original `DWMSG_` shell functions might reveal nuances not fully captured. Further review of the original `DWMSG_` scripts is recommended for 100% functional parity.
*   **`BERT_DIR_ROOT` Resolution:** The original script uses `$BERT_DIR_ROOT`. The exact configuration and usage of this variable across the legacy system need to be fully understood to ensure correct parameterization or environment setup in the target GCP environment (e.g., as an Airflow Variable or a parameter to the BigQuery Stored Procedure).
*   **Missing `file_complexity` data:** The migration design document noted the absence of complexity tier for the wrapper script. While the migration has proceeded, there might be hidden complexities in the original script that were not fully captured by automated analysis.
*   **`ta_p_discount_rr` Target Table:** The target table `ta_p_discount_rr` (where the reconciled data ultimately resides) must be migrated to BigQuery (e.g., `my_gcp_project.my_bq_dataset.ta_p_discount_rr`) and its schema defined before the core logic can write to it.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Unit Testing BigQuery Stored Procedures:**
    *   **`Vertragsdatenabgleich`:** Test with various valid and invalid combinations of `p_s` (Stichtag) and `p_l` (Laufnummer) parameters. Verify that parameter validation works correctly and raises appropriate errors. Test the help (`p_h`) flag.
    *   **`DWMSG_` Procedures:** Individually call each `DWMSG_` procedure and verify that the correct entries are inserted into the `my_gcp_project.my_bq_dataset.job_log` table with the expected `job_id`, `severity`, and `message`.
    *   **`k_ausd_v_ta_p_discount_rr` (after implementation):** Thoroughly test the implemented core logic with various input data scenarios to ensure it produces the correct output in the target `ta_p_discount_rr` table.
2.  **Integration Testing (Airflow DAG):**
    *   **Manual Trigger:** Manually trigger the `r_ausd_v_ta_p_discount_rr` DAG in the Airflow UI.
    *   **Parameter Passing:** Provide valid `stichtag` and `laufnummer` parameters via the Airflow UI's trigger form.
    *   **Success Scenario:** Verify that the DAG runs successfully, all tasks complete, and the `call_vertragsdatenabgleich_procedure` task executes without errors.
    *   **Error Scenario:** Introduce an intentional error (e.g., by modifying `k_ausd_v_ta_p_discount_rr` to `RAISE` an error) and verify that the DAG fails gracefully, and the `DWMSG_Fehlerbehandlung` procedure logs the error correctly in `job_log`.
3.  **Logging Verification:**
    *   After each test run (success and failure), query the `my_gcp_project.my_bq_dataset.job_log` table.
    *   **Passing Criteria:**
        *   For successful runs: The `job_log` table should contain entries with `severity = 'I'` (Info) for job start, parameter info, core logic invocation, and a final "Job completed successfully" message. No entries with `severity = 'E'` (Error) should be present.
        *   For failed runs: The `job_log` table should contain entries with `severity = 'E'` detailing the error, and the final message should indicate job failure.
4.  **Data Verification (after `k_ausd_v_ta_p_discount_rr` implementation):**
    *   Query the target `my_gcp_project.my_bq_dataset.ta_p_discount_rr` table to ensure that the data processed by the core logic is correct and complete according to the business requirements.

## 7. Rollback Procedure

In case of issues or unexpected behavior after deployment, follow these steps to roll back to the legacy system:

1.  **Pause/Un-deploy Airflow DAG:**
    *   Immediately pause or un-deploy the `r_ausd_v_ta_p_discount_rr` DAG in the Cloud Composer Airflow UI to prevent further executions of the migrated job.
2.  **Revert BigQuery Stored Procedures (Optional but Recommended):**
    *   If any BigQuery Stored Procedures were modified or deployed incorrectly, revert them to a known good state. This can be done by re-deploying previous versions from source control or dropping and recreating them if no prior versioning is in place.
    *   **Note:** If the `k_ausd_v_ta_p_discount_rr` procedure has already modified data in `ta_p_discount_rr`, a data rollback strategy might be necessary (see point 3).
3.  **Data Rollback (If Applicable):**
    *   If the core logic (`k_ausd_v_ta_p_discount_rr` BSP) has written or modified data in `my_gcp_project.my_bq_dataset.ta_p_discount_rr` and this data is incorrect or corrupted, a data rollback procedure must be executed. This could involve:
        *   Restoring the `ta_p_discount_rr` table from a BigQuery snapshot or a point-in-time recovery.
        *   Executing specific `DELETE` or `UPDATE` statements to revert changes.
        *   Loading a backup of the table.
    *   **Crucially, define this data rollback strategy during the `k_ausd_v_ta_p_discount_rr` implementation phase.**
4.  **Re-enable Legacy Job:**
    *   Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh` script in the legacy environment to resume normal operations.
5.  **Investigate and Rectify:**
    *   Analyze the `job_log` table in BigQuery and Airflow logs to identify the root cause of the failure. Rectify the issues in the migrated code or configuration before attempting re-deployment.