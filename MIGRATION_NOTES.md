# MIGRATION_NOTES.md: DW.BERT_ABLAUFSTEUERUNG

## 1. Summary

The UC4 Job Scheduler (JSCH) job `DW.BERT_ABLAUFSTEUERUNG`, responsible for orchestrating various "Bert" related ETL processes, has been migrated from a legacy UC4/Automic environment to Google Cloud Platform (GCP).

*   **Source Platform**: UC4/Automic Job Scheduler (XML configuration) and Oracle SQL scripts.
*   **Target Platform**: Google Cloud Composer (Apache Airflow) for orchestration, Google BigQuery for data storage and transformation, and Google Cloud Storage for file exports. Shell scripts have been re-implemented in Python.

This migration re-implements the scheduling logic, task dependencies, and data transformation steps within the GCP ecosystem, leveraging cloud-native services for scalability, reliability, and maintainability.

## 2. Generated Artifacts

The following files were generated as part of this migration:

*   **`ddl/source_tables.sql`**:
    *   **Role**: Contains BigQuery Data Definition Language (DDL) statements for creating placeholder tables corresponding to the original Oracle source tables (e.g., `RPT$TA_S_D1_VERTRAG`, `SOF$TA_BPR_OPTIONEN`). These DDLs serve as a template and require population with actual schema details from the source system.
*   **`sql/bert_bestandsdaten.sql`**:
    *   **Role**: BigQuery SQL script, a direct conversion of the legacy `d_exis_apt_bestandsdaten.sql`. It processes contract and option data to generate "Bestandsdaten" (inventory data).
*   **`sql/bert_nna_daten.sql`**:
    *   **Role**: BigQuery SQL script, a direct conversion of the legacy `d_exis_apt_nna_daten.sql`. It processes NNA (Non-Network Access) data related to GPRS usage.
*   **`sql/bert_nna_voice.sql`**:
    *   **Role**: BigQuery SQL script, a direct conversion of the legacy `d_exis_apt_nna_voice.sql`. It processes NNA data related to voice usage.
*   **`sql/bert_rabattdaten.sql`**:
    *   **Role**: BigQuery SQL script, a direct conversion of the legacy `d_exis_apt_rabattdaten.sql`. It processes discount and rebate data.
*   **`scripts/r_exis_v2.py`**:
    *   **Role**: Python script re-implementing the functionality of the legacy `r_exis_v2` shell script. Its primary function is to export data from BigQuery tables to Google Cloud Storage (GCS) in a specified format (e.g., CSV).
*   **`scripts/bert_log.py`**:
    *   **Role**: Python script providing basic logging functionality, serving as a placeholder re-implementation for the `BERT_LOG.KSH` shell script. Its exact functionality needs to be aligned with the original script's behavior.
*   **`scripts/sql_runner.py`**:
    *   **Role**: Python script designed to execute SQL queries from a file in BigQuery, serving as a placeholder re-implementation for the `SQL.KSH` shell script. Its exact functionality needs to be aligned with the original script's behavior.
*   **`dags/sub_dags/bert_monatlich_jp.py`**:
    *   **Role**: Airflow TaskGroup definition for `JOBP:DW.BERT_MONATLICH_JP`. It encapsulates tasks related to monthly Bert processes, including a placeholder for monthly billing data and logging.
*   **`dags/sub_dags/bert_adm_housekeeping_jp.py`**:
    *   **Role**: Airflow TaskGroup definition for `JOBP:DW.BERT_ADM_HOUSEKEEPING_JP`. It contains placeholder tasks for administrative checks and housekeeping routines.
*   **`dags/sub_dags/dwh_apt_export_taeglich_jp.py`**:
    *   **Role**: Airflow TaskGroup definition for `JOBP:DW.DWH_APT_EXPORT_TAEGLICH_JP`. It orchestrates the daily APT exports, including the execution of the converted SQL scripts and the BigQuery-to-GCS export using `r_exis_v2.py`.
*   **`dags/sub_dags/bert_stammdaten_jp.py`**:
    *   **Role**: Airflow TaskGroup definition for `JOBP:DW.BERT_STAMMDATEN_JP`. It manages Bert master data processes, including dropping temporary tables and various data processing steps (currently placeholders).
*   **`dags/bert_ablaufsteuerung_dag.py`**:
    *   **Role**: The main Airflow DAG that orchestrates the entire `DW.BERT_ABLAUFSTEUERUNG` workflow. It defines the overall schedule, task dependencies, and integrates the various sub-TaskGroups and event placeholders.

## 3. Key Design Decisions

*   **Cloud Composer (Airflow) for Orchestration**: Chosen for its robust scheduling capabilities, native support for Directed Acyclic Graphs (DAGs), and seamless integration with other GCP services. It directly translates UC4's job scheduling and dependency management into a modern, cloud-native framework.
*   **BigQuery for Data Storage and Transformation**: All relational data and SQL transformations are migrated to BigQuery. This leverages BigQuery's serverless architecture, scalability, and cost-effectiveness for analytical workloads.
*   **Python for Script Re-implementation**: Legacy shell scripts (`.ksh`, `.bin`) are re-implemented in Python. Python is a standard language for data engineering in GCP, offering better maintainability, testability, and direct access to GCP client libraries.
*   **Google Cloud Storage for File Exports**: The `r_exis_v2` script's export functionality is re-implemented to write output files to GCS. GCS provides highly durable, available, and scalable object storage, suitable for data exports and staging.
*   **Airflow TaskGroups for UC4 Job Plans (JOBP)**: UC4 Job Plans are mapped to Airflow TaskGroups. This improves DAG readability, modularity, and allows for logical grouping of related tasks within the main orchestration DAG.
*   **Airflow `PythonOperator` for UC4 Events (EVNT)**: UC4 Events are initially represented by `PythonOperator` tasks with placeholder logic. This allows for future refinement, potentially using `ExternalTaskSensor` if these events signify dependencies on other Airflow DAGs or external systems.
*   **Airflow `schedule_interval` and `start_date` for UC4 Scheduling**: UC4's `Period` and `StartTime` attributes are directly translated to Airflow's `schedule_interval` and `start_date` parameters, ensuring consistent scheduling.
*   **Airflow `max_active_runs=1` for UC4 Synchronization (SYNCREF)**: The UC4 `SYNCREF` object with `SETZE_LAEUFT` and `SETZE_FREI` behavior is primarily managed by setting `max_active_runs=1` on the main DAG, ensuring only one instance runs at a time. For external dependencies, `ExternalTaskSensor` would be used.
*   **Templating with Airflow Variables and Jinja**: Airflow Variables (`var.value.PROJECT`, `var.value.DATASET`, `var.value.BUCKET`) and Jinja templating (`{{ ds_nodash }}`) are used in SQL and Python scripts. This promotes reusability, environment-agnostic deployment, and dynamic parameterization.
*   **Placeholder DDLs and SQL**: Initial DDLs for source tables and some SQL/Python tasks are placeholders. This acknowledges the iterative nature of migration and the need for detailed source system analysis for complete accuracy.

## 4. Manual Steps Before Go-Live

Before the `DW.BERT_ABLAUFSTEUERUNG` DAG can be fully operational in a production environment, several manual steps are required:

1.  **GCP Project Setup**:
    *   Ensure a dedicated GCP project is set up for the data warehouse environment.

2.  **Cloud Composer Environment**:
    *   Provision and configure a Cloud Composer environment (Airflow version compatible with generated code).

3.  **BigQuery Datasets Creation**:
    *   Create the BigQuery datasets:
        *   `{{ var.value.SOURCE_BQ_DATASET }}` (e.g., `source_data_bert`)
        *   `{{ var.value.TARGET_BQ_DATASET }}` (e.g., `bert_dwh`)
    *   These datasets will house the source and target tables, respectively.

4.  **BigQuery Source Table Creation & Data Ingestion**:
    *   **Schema Discovery**: Obtain the precise schemas (column names, data types, nullability) for all referenced Oracle source tables (e.g., `RPT$TA_S_D1_VERTRAG`, `SOF$TA_BPR_OPTIONEN`, `DWH$VI_C_VERTRAG`, etc.).
    *   **DDL Application**: Update `ddl/source_tables.sql` with the accurate BigQuery DDLs and execute them in the `{{ var.value.SOURCE_BQ_DATASET }}`.
    *   **Data Ingestion**: Establish and run a separate data ingestion pipeline (e.g., Datastream, Fivetran, custom Dataflow jobs) to continuously load data from the Oracle source systems into these newly created BigQuery source tables. This is a critical prerequisite for the migrated ETL to function.

5.  **Google Cloud Storage Bucket Creation**:
    *   Create the GCS bucket for data exports: `{{ var.value.GCS_EXPORT_BUCKET }}` (e.g., `bert-exports-bucket`).

6.  **IAM Permissions Configuration**:
    *   Grant the Composer Service Account (or a dedicated service account for the DAG) the following roles:
        *   `BigQuery Data Editor` (or more granular permissions) for `{{ var.value.TARGET_BQ_DATASET }}`.
        *   `BigQuery Data Viewer` (or more granular permissions) for `{{ var.value.SOURCE_BQ_DATASET }}`.
        *   `BigQuery Job User` for running BigQuery queries.
        *   `Storage Object Admin` (or `Storage Object Creator` and `Storage Object Viewer`) for `{{ var.value.GCS_EXPORT_BUCKET }}`.
        *   `Secret Manager Secret Accessor` if any external credentials (e.g., for `EXT:DATABASE`) are stored in Secret Manager.

7.  **Airflow Variables Configuration**:
    *   In the Airflow UI (Admin -> Variables), create the following variables:
        *   `SOURCE_BQ_PROJECT`: Your GCP project ID where source BigQuery tables reside.
        *   `SOURCE_BQ_DATASET`: The BigQuery dataset ID for source tables.
        *   `TARGET_BQ_PROJECT`: Your GCP project ID where target BigQuery tables reside.
        *   `TARGET_BQ_DATASET`: The BigQuery dataset ID for target tables.
        *   `GCS_EXPORT_BUCKET`: The GCS bucket name for exports.

8.  **External System Connections/Secrets**:
    *   If `EXT:DATABASE` or `HOST:DWHDWH2P` require specific connection details or credentials, these must be securely stored (e.g., in Google Secret Manager) and configured for access by the Airflow environment. Airflow connections might also be needed.

9.  **Deployment**:
    *   Upload all generated `.py` (DAGs and scripts) and `.sql` files to the Composer environment's DAGs folder (or a designated subfolder for scripts/SQL).

## 5. Known Gaps & Unresolved References

The following items require further investigation, clarification, or redesign (B4 items) before full production readiness:

*   **`UNRESOLVED:BERT_LOG.KSH` and `UNRESOLVED:SQL.KSH`**: The exact functionality of these legacy shell scripts is unknown. The generated Python scripts (`scripts/bert_log.py`, `scripts/sql_runner.py`) are placeholders. A detailed analysis of the original scripts is required to accurately re-implement their logic.
*   **UC4 Calendar Translation**: The complex calendar logic from UC4 (`DAY_OF_MONTH_25`, `DAY_OF_MONTH_05`, `BERT_NICHT`) is currently not fully translated beyond the main DAG's `schedule_interval`. Sophisticated calendar rules may require custom `PythonOperator` tasks or Airflow sensors to implement conditional execution based on specific dates or business calendars.
*   **`EXT:DATABASE` Details**: The specific type, connection method, and purpose of `EXT:DATABASE` (referenced by `r_exis_v2`) are unknown. This requires investigation to determine the appropriate GCP service (e.g., Cloud SQL, BigQuery federated query, Cloud Functions for API interaction) and secure credential management.
*   **`HOST:DWHDWH2P` Functionality**: The exact interaction with `HOST:DWHDWH2P` (e.g., file transfer, API calls, database connection) needs to be clarified to choose the appropriate Google Cloud service replacement.
*   **`DW.BERT_RECHNUNGSDATEN` SQL**: The SQL for `DW.BERT_RECHNUNGSDATEN` within `bert_monatlich_jp.py` is a placeholder. The actual monthly billing data processing SQL needs to be migrated and implemented.
*   **Master Data Processing Tasks**: Tasks like `DW.BERT_P_ADRESSEN`, `DW.BERT_P_AUSTAUSCH`, `DW.BERT_AUSD_BP_TA_APN_CARMEN`, `DW.BERT_AUSD_BP_TA_APN_VERTRAG`, and `DW.BERT_AUSD_BP_TA_BCP_ICCID` within `bert_stammdaten_jp.py` are currently `PythonOperator` placeholders. Their underlying logic (SQL, Python, external calls) needs to be migrated.
*   **UC4 Events (`EVNT`)**: `DW.BERT_RUN_ADM_CHECK_JP_EVT` and `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT` are currently `PythonOperator` placeholders. If these events represent dependencies on other Airflow DAGs or external systems, they should be replaced with `ExternalTaskSensor` or other specific operators to correctly model the synchronization.
*   **Source Table Schemas**: The DDLs in `ddl/source_tables.sql` are generic placeholders. The actual schemas for all source tables must be obtained from the legacy Oracle system and accurately applied in BigQuery.
*   **`bert_bestandsdaten.sql` `WHERE` Clause**: The condition `RPT.VERTRAGSSTATUS = 'ACTIVE'` was inferred from the `GROUP BY` clause in the original SQL but was missing from the `WHERE` clause. This assumption needs to be validated against the source system's business logic.
*   **`MONATS_ID` Date Format**: The conversion `CAST('{{ ds_nodash[:6] }}' AS INT64)` assumes `ds_nodash` is `YYYYMMDD` and `MONATS_ID` expects `YYYYMM`. This date format handling needs to be confirmed against the source data.
*   **BigQuery Export Location**: The `location="US"` parameter in `scripts/r_exis_v2.py` for BigQuery export should be adjusted to match the actual BigQuery dataset location.

## 6. Validation

To ensure the migrated `DW.BERT_ABLAUFSTEUERUNG` job functions correctly, follow these validation steps:

1.  **DAG Syntax Check**:
    *   Before deployment, run `airflow dags parse dags/bert_ablaufsteuerung_dag.py` in a Composer environment shell to check for syntax errors.

2.  **Local Airflow Testing (Development Environment)**:
    *   Use `airflow tasks test bert_ablaufsteuerung_dag <task_id> <execution_date>` for individual task testing.
    *   Simulate a full DAG run in a local Airflow environment to verify task dependencies and overall flow.

3.  **Deployment to Staging/Development Composer Environment**:
    *   Deploy the DAG and associated scripts/SQL to a non-production Composer environment.
    *   Trigger a manual run of the `bert_ablaufsteuerung_dag`.

4.  **Data Validation**:
    *   **Output Comparison**: Compare the data in the target BigQuery tables (`bert_bestandsdaten`, `bert_nna_daten`, `bert_nna_voice`, `bert_rabattdaten`) with the corresponding outputs from the legacy UC4 job for the same execution date.
        *   Verify row counts.
        *   Check key aggregate metrics (sums, averages, distinct counts).
        *   Perform spot checks on individual records for accuracy.
    *   **Exported Files**: Verify that the files exported to GCS by `r_exis_v2.py` are present, correctly named, and their content matches the expected format and data.

5.  **Scheduling and Dependency Validation**:
    *   Confirm that the `bert_ablaufsteuerung_dag` runs at the scheduled time (daily at 00:00 UTC).
    *   Verify that tasks execute in the correct sequence as defined by the dependencies.
    *   Test failure scenarios (e.g., by manually failing a task) to ensure retry mechanisms and downstream task behavior are as expected.

6.  **Logging and Monitoring**:
    *   Check Cloud Logging for task logs, ensuring no unexpected errors or warnings.
    *   Monitor the DAG in the Airflow UI for successful completion and task durations.
    *   Verify that relevant metrics are being captured in Cloud Monitoring.

**"Passing" Criteria**:
A successful migration means:
*   The `bert_ablaufsteuerung_dag` completes successfully without errors in the Airflow UI.
*   All target BigQuery tables contain data that is functionally equivalent and numerically consistent with the output of the legacy UC4 job.
*   All expected files are exported to GCS with correct content and naming conventions.
*   The DAG adheres to its defined schedule and dependencies.
*   All logs are captured and accessible in Cloud Logging.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure should be followed:

1.  **Immediate DAG Deactivation**:
    *   In the Airflow UI for the production Composer environment, immediately **pause/deactivate** the `bert_ablaufsteuerung_dag`. This prevents further execution of the migrated job.

2.  **Re-enable Legacy UC4 Job**:
    *   Re-enable the original `DW.BERT_ABLAUFSTEUERUNG` job in the legacy UC4 system. Ensure it is configured to run according to its original schedule and dependencies.
    *   Verify that the legacy job successfully completes and produces expected outputs.

3.  **Data Reversion/Cleanup (if necessary)**:
    *   If the migrated job wrote any data to production BigQuery tables that is incorrect or corrupted, a decision must be made on whether to revert these tables to a previous state (if backups or snapshots exist) or to perform a targeted cleanup. This step is highly dependent on the nature and impact of the data issue.

4.  **Code Rollback in Composer**:
    *   Revert the `bert_ablaufsteuerung_dag.py` and any associated scripts/SQL files in the Composer environment's DAGs bucket to a previous stable version, or remove them entirely if the rollback is permanent. This can typically be done via version control (e.g., Git) integrated with the Composer environment.

5.  **Root Cause Analysis**:
    *   Thoroughly investigate the root cause of the failure in the GCP environment using Cloud Logging, Cloud Monitoring, and Airflow logs. Address the identified issues in a non-production environment before attempting re-deployment.