# Migration Design — DW.BERT_AUSD_V_TA_C_BFC

## 1. Purpose & Scope
The original UC4 job `DW.BERT_AUSD_V_TA_C_BFC` is a UNIX job defined in `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_C_BFC.xml`. Its primary purpose is to update contract extension period caching. This job orchestrates the execution of a KornShell script named `r_ausd_v_ta_c_bfc.ksh` on a specified UNIX host (`DWHDWH1P`) using the login `DW.UNIX.ISBERT`. The job also includes other UC4 objects (`DW.HOLE_PFAD`, `DW.BERT_LESE_LOG`) which are likely used for environment setup or logging. The scope of this migration is to re-platform this UC4 job to a Google Cloud Platform (GCP) environment, specifically utilizing Cloud Composer (Apache Airflow) for orchestration and Dataproc for executing the transformed business logic.

## 2. Source Inventory
*   **File:** `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_C_BFC.xml`
    *   **Technology:** UC4/Automic (UC4 UNIX Job Definition)
    *   **Summary:** UC4 UNIX job definition for updating contract extension period caching. It orchestrates the execution of a KornShell script.
    *   **Complexity Tier:** Medium (Inferred due to the lack of explicit `file_complexity` data, based on the orchestration nature and external script calls identified in the `file_analysis`).
    *   **Automation Bucket:** Semi-Auto (B2) (Inferred due to the lack of explicit `automation_rate` data, as replatforming an orchestrator and converting shell script logic typically requires semi-automated or manual effort).

## 3. Target Architecture
The target architecture will leverage key Google Cloud Platform services to replace the legacy UC4 environment:
*   **Orchestration:** Cloud Composer (managed Apache Airflow) will be deployed to schedule and manage the end-to-end workflow, replacing the UC4 scheduler.
*   **Compute:** Google Cloud Dataproc will be utilized as the execution engine for the core business logic. The original KornShell script will be refactored and converted into a PySpark script to run on Dataproc clusters.
*   **Storage:** Google Cloud Storage (GCS) will serve as the primary storage for the PySpark script, any necessary configuration files, and potentially for intermediate or final output data.
*   **Logging and Monitoring:** Cloud Logging and Cloud Monitoring will provide centralized logging and monitoring capabilities for the Airflow DAGs and Dataproc job executions.

The UC4 `JOBS_UNIX` object `DW.BERT_AUSD_V_TA_C_BFC` will be transformed into an Airflow DAG named `dw_bert_ausd_v_ta_c_bfc`. This Airflow DAG will contain a single task responsible for launching a Dataproc job.

## 4. Data Flow & Lineage
The original data flow within the UC4 environment is as follows:
1.  The UC4 job `DW.BERT_AUSD_V_TA_C_BFC` is triggered (e.g., by a schedule or an event).
2.  It internally processes UC4 include objects `DW.HOLE_PFAD` and `DW.BERT_LESE_LOG`, which likely set up the execution environment, define common functions, or handle logging.
3.  A job identifier variable `DWH_JOB_KENNUNG` is set to `'AUSD_V_TA_C_BFC'`.
4.  The KornShell script `r_ausd_v_ta_c_bfc.ksh` is executed on the UNIX host `DWHDWH1P` using the `DW.UNIX.ISBERT` login.
5.  The `r_ausd_v_ta_c_bfc.ksh` script performs the specific business logic for updating contract extension period caching.

In the target GCP environment, the data flow will be:
1.  An Airflow DAG `dw_bert_ausd_v_ta_c_bfc` is triggered within Cloud Composer (either manually or based on a defined schedule).
2.  The DAG executes a `DataprocSubmitJobOperator` task.
3.  This task initiates a Dataproc job, submitting a PySpark script named `r_ausd_v_ta_c_bfc.py` (which is the transformed equivalent of the original KornShell script).
4.  The PySpark script executes on a Dataproc cluster, performing the data processing and updating the contract extension period caching.

The UC4 sync object `DW.BERT_AUSD_V_TA_C_BFC_SYNC` with `Else="Wait"` behavior indicates a concurrency control mechanism. This will be mapped in Airflow by setting `max_active_runs=1` for the `dw_bert_ausd_v_ta_c_bfc` DAG, ensuring only one instance of the DAG runs at a time.

## 5. Transformation Logic
The transformation process involves two main components: the UC4 orchestration layer and the underlying KornShell script.

*   **UC4 Job (`DW.BERT_AUSD_V_TA_C_BFC.xml`) to Airflow DAG (`dw_bert_ausd_v_ta_c_bfc.py`):**
    *   **Orchestration:** The UC4 `JOBS_UNIX` object will be converted into a Python-based Airflow DAG.
    *   **Task Definition:** A single `DataprocSubmitJobOperator` will be used within the DAG to encapsulate and trigger the execution of the PySpark script on Dataproc.
    *   **Parameters & Variables:** UC4 variables like `DWH_JOB_KENNUNG` will be adapted to Airflow parameters or environment variables passed to the PySpark script. UC4 host (`DWHDWH1P`) and login (`DW.UNIX.ISBERT`) details will be replaced by the configuration of the Dataproc cluster and the service account used by Airflow.
    *   **Concurrency:** The UC4 `SYNCREF` object, specifically `DW.BERT_AUSD_V_TA_C_BFC_SYNC` with `Else="Wait"`, will be translated into the Airflow DAG property `max_active_runs=1` to manage concurrent executions.
    *   **Scheduling:** The original UC4 object did not provide an `EVNT_TIME` (event time), meaning no direct schedule can be inferred. The Airflow DAG will initially be configured with `schedule=None`, requiring manual triggers or an externally defined schedule to be added post-migration.
    *   **Error Handling:** The UC4 job's `RUNTIME` settings (`MrtFix=7200`, `MrtCancel=1`) imply a runtime limit and cancellation. This will not directly translate to standard Airflow retries (`retries=0` by default). Any desired task timeouts or Dataproc job timeouts will need to be explicitly configured in the Airflow task or Dataproc job definition.

*   **KornShell Script (`r_ausd_v_ta_c_bfc.ksh`) to PySpark (`r_ausd_v_ta_c_bfc.py`):**
    *   The content of the `r_ausd_v_ta_c_bfc.ksh` KornShell script is the core of the business logic. This script will need to be thoroughly analyzed, and its functionality (e.g., data extraction, transformations, aggregations, database updates) must be re-implemented in a PySpark script (`r_ausd_v_ta_c_bfc.py`). This transformation will require careful consideration of data types, file formats, and any external system interactions. The PySpark script will be optimized for distributed processing on Dataproc.

## 6. External Dependencies
*   **Original External Systems:**
    *   **UNIX Host (`DWHDWH1P`):** The physical machine where the KornShell script executed.
    *   **Login (`DW.UNIX.ISBERT`):** The user account used to run the script on the UNIX host.
    *   **UC4 Include Objects (`DW.HOLE_PFAD`, `DW.BERT_LESE_LOG`):** These are other UC4 objects likely containing common functions, environment variables, or logging mechanisms.
*   **Target GCP Replacements:**
    *   **UNIX Host:** Replaced by a managed Dataproc cluster (ephemeral or persistent, depending on design choices) within GCP.
    *   **Login:** Replaced by a GCP Service Account associated with the Cloud Composer environment and Dataproc cluster, providing granular access control to GCP resources.
    *   **UC4 Include Objects:** The functionality provided by `DW.HOLE_PFAD` and `DW.BERT_LESE_LOG` will need to be analyzed and re-implemented. This could involve:
        *   Porting common shell functions to Python modules callable by the PySpark script.
        *   Setting Airflow variables or environment variables for configuration.
        *   Integrating with Cloud Logging for logging.

## 7. Unresolved / Risks
*   **KornShell Script Detailed Analysis:** The exact logic, complexity, and dependencies (e.g., database connections, external tools invoked) within `r_ausd_v_ta_c_bfc.ksh` are currently unknown. This is the primary unresolved component and poses a significant risk. Its conversion to PySpark will require a detailed code review and potentially manual redesign (B3/B4 bucket item).
*   **UC4 Include Object Functionality:** A thorough understanding of what `DW.HOLE_PFAD` and `DW.BERT_LESE_LOG` do is essential for their correct migration. Without this, there's a risk of missing critical setup or logging.
*   **GCP Configuration Placeholders:** The Airflow DAG will contain placeholders for `YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_REGION`, `YOUR_DATAPROC_CLUSTER_NAME`, and `YOUR_BUCKET_NAME`. These must be correctly configured during the deployment phase.
*   **Scheduling Details:** If the original UC4 job had a specific schedule (not an `EVNT_TIME` object), it needs to be identified and explicitly added to the Airflow DAG.
*   **Retry and SLA Policies:** While basic concurrency is handled, a detailed retry strategy and Service Level Agreement (SLA) for the Airflow task should be defined based on business requirements.

## 8. Build Plan
The migration will result in two primary output files:

1.  **Airflow DAG (Python):**
    *   **File Name:** `dags/dw_bert_ausd_v_ta_c_bfc.py`
    *   **Language:** Python
    *   **Description:** This file will define the Airflow DAG responsible for orchestrating the PySpark job on Dataproc.

    ```python
    from datetime import timedelta
    from airflow import DAG
    from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
    # Import other operators/modules as needed for custom logic or error handling

    # --- GCP Configuration (Placeholders to be replaced) ---
    GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
    DATAPROC_REGION = "YOUR_DATAPROC_REGION"
    DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME" # Consider ephemeral clusters for cost efficiency
    GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"
    PYSPARK_SCRIPT_URI = f"gs://{GCS_BUCKET_NAME}/pyspark_scripts/r_ausd_v_ta_c_bfc.py"

    # --- Default Arguments for the DAG ---
    default_args = {
        'owner': 'uc4_migration',
        'depends_on_past': False,
        'retries': 0, # Default, can be overridden per task or globally
        'retry_delay': timedelta(minutes=0),
        'start_date': # Define the DAG's start date, e.g., pendulum.datetime(2023, 1, 1, tz="UTC")
    }

    # --- DAG Definition ---
    with DAG(
        dag_id="dw_bert_ausd_v_ta_c_bfc",
        schedule=None,  # No schedule found in UC4, set to None for manual triggering initially
        catchup=False,  # Set to False to prevent backfills for past missed schedules
        max_active_runs=1, # Maps to UC4 sync object "Else=Wait" behavior
        is_paused_upon_creation=False, # UC4 job was active
        default_args=default_args,
        tags=['uc4', 'dataproc', 'pyspark', 'data_cache'],
        description='Airflow DAG for updating contract extension period caching, migrated from UC4.',
    ) as dag:
        # --- Task: Submit PySpark Job to Dataproc ---
        run_dataproc_job = DataprocSubmitJobOperator(
            task_id="run_dw_bert_ausd_v_ta_c_bfc",
            project_id=GCP_PROJECT_ID,
            region=DATAPROC_REGION,
            cluster_name=DATAPROC_CLUSTER_NAME, # Or use a cluster selector for ephemeral clusters
            job={
                "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
                "pyspark_job": {
                    "main_python_file_uri": PYSPARK_SCRIPT_URI,
                    "args": [], # Pass any required arguments to the PySpark script here, e.g., ["--job-identifier", "AUSD_V_TA_C_BFC"]
                    # Add other PySpark job properties like 'jar_file_uris', 'python_file_uris', 'file_uris' if needed
                },
            },
            # Optional: Add dataproc_job_id for custom job naming
            # Optional: Add execution_timeout for this task
        )

        # --- Task Dependencies ---
        # As there is only one core task, the dependency is implicit from start to finish.
        # Additional tasks (e.g., data quality checks, notification tasks) could be added here.
    ```

2.  **PySpark Script (Python):**
    *   **File Name:** `pyspark_scripts/r_ausd_v_ta_c_bfc.py`
    *   **Language:** Python (PySpark)
    *   **Description:** This script will contain the re-implemented business logic from the original `r_ausd_v_ta_c_bfc.ksh` KornShell script. It will interact with data sources (e.g., BigQuery, Cloud Storage) and perform the necessary transformations to update the contract extension period caching. This script must be uploaded to the specified GCS bucket (`gs://YOUR_BUCKET_NAME/pyspark_scripts/`) before the Airflow DAG can execute it.
    *   **Note:** The detailed content of this script is pending the manual analysis and conversion of the legacy KornShell script. It will involve creating a SparkSession, reading data, applying transformations, and writing results.