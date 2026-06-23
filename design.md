# Migration Design — DW.BERT_AUSD_BP_TA_P_BASISPROD

## 1. Purpose & Scope

This migration design document addresses the UC4 job `DW.BERT_AUSD_BP_TA_P_BASISPROD`. The original purpose of this job, as described in its UC4 title, is "BERT_P_BASISPRODUKT: Aufbereitung der instantiierten Basisprodukte," which translates to "Preparation of instantiated base products." It functions as a UNIX job within UC4, primarily responsible for executing a shell script (`r_ausd_bp_ta_p_basisprod.ksh`) to achieve its objective. The job is marked as restartable.

The scope of this migration is to re-platform this UC4 job to Google Cloud Platform (GCP) using Airflow for orchestration and Dataproc for executing the underlying business logic, presumably refactored into a PySpark application.

## 2. Source Inventory

The migration is based on a single source file:

*   **File:** `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_P_BASISPROD.xml`
    *   **Technology:** UC4/Automic XML (Job definition for a UNIX job)
    *   **Category:** UC4
    *   **Tool:** UC4/Automic
    *   **Summary:** UC4 job definition for a UNIX job named DW.BERT_AUSD_BP_TA_P_BASISPROD, responsible for preparing instantiated base products by executing a shell script.
    *   **Complexity Tier:** Undefined (no rows found in `file_complexity`, implies default or not yet tiered)
    *   **Migration Flags:** Undefined (no rows found)
    *   **Automation Bucket:** `semi_auto`

## 3. Target Architecture

The target architecture for `DW.BERT_AUSD_BP_TA_P_BASISPROD` will involve an Airflow DAG orchestrated on Composer (managed Airflow on GCP). The core business logic, currently implemented in a shell script, will be re-engineered into a PySpark application and executed on a Dataproc cluster.

**Airflow DAG Properties (Placeholders):**

*   **dag_id:** `dw_bert_ausd_bp_ta_p_basisprod`
*   **schedule:** `None` (manual trigger only, as no `EVNT_TIME` was provided in source)
*   **start_date:** `PLACEHOLDER_START_DATE`
*   **catchup:** `False`
*   **max_active_runs:** `1`
*   **is_paused_upon_creation:** `False`
*   **default_args.owner:** `airflow`
*   **default_args.retries:** `0` (unless overridden by parent workflow's UC4 retry logic)
*   **default_args.retry_delay:** `timedelta(minutes=5)` (placeholder)

## 4. Data Flow & Lineage

The original UC4 job's primary function is to trigger a shell script. The transformed data flow will reflect this orchestration pattern:

1.  **Airflow DAG Start:** The `dw_bert_ausd_bp_ta_p_basisprod` Airflow DAG is triggered (currently manually, future scheduling dependent on complete workflow analysis).
2.  **Execute PySpark Job:** A single Airflow task, `run_dw_bert_ausd_bp_ta_p_basisprod`, using `DataprocSubmitJobOperator`, will submit a PySpark job to a Dataproc cluster.
3.  **PySpark Application Execution:** The PySpark application (`r_ausd_bp_ta_p_basisprod.py`) will perform the "preparation of instantiated base products" logic, interacting with BigQuery or other GCP data services as necessary.
4.  **Airflow DAG End:** The DAG completes upon successful (or failed) execution of the Dataproc job.

**Dependency Map:**
`start >> run_dw_bert_ausd_bp_ta_p_basisprod >> end`

## 5. Transformation Logic

The UC4 XML specifies a `SCRIPT` block that executes:
```
:inc DW.HOLE_PFAD
:set &DWH_JOB_KENNUNG='AUSD_BP_TA_P_BASISPROD'
. $HOME/.dw_init
&HOME/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_p_basisprod.ksh
:inc DW.BERT_LESE_LOG
```
The key transformation is the migration of the shell script `r_ausd_bp_ta_p_basisprod.ksh` to a PySpark application, `r_ausd_bp_ta_p_basisprod.py`. This PySpark script will encapsulate the business logic currently performed by the shell script. The initial and final `inc` statements and `set` variable will need to be evaluated for their impact on the shell script's environment and translated into appropriate Airflow variables or PySpark configurations if they contain essential logic or parameters.

## 6. External Dependencies

The UC4 job shows the following external dependencies and their proposed replacements:

*   **UC4 Host:** `|DWHDWH2P|HOST`
    *   **Replacement:** This will be replaced by a Dataproc cluster for job execution, managed within GCP.
*   **UC4 Login:** `DW.UNIX.ISBERT`
    *   **Replacement:** GCP service accounts or workload identity will manage authentication and authorization for the Dataproc cluster and BigQuery access.
*   **Legacy Shell Script:** `r_ausd_bp_ta_p_basisprod.ksh`
    *   **Replacement:** A PySpark application `r_ausd_bp_ta_p_basisprod.py` stored in Google Cloud Storage (GCS) and executed on Dataproc.

## 7. Unresolved / Risks

*   **Incomplete Workflow Information:** The provided UC4 XML is a single job object, not a complete workflow export.
    *   **Risk:** No cron schedule can be derived.
    *   **Risk:** No DAG-level dependencies or trigger DAG mappings can be derived.
    *   **Risk:** No calendar or synchronization behavior is present.
    *   **Mitigation:** A full UC4 workflow export (including `EVNT_TIME` and `JOBP` files) is required to complete the DAG scheduling and inter-job dependency mapping.
*   **Shell Script to PySpark Mapping:** The UC4 script is a shell wrapper. The mapping to a PySpark application (`r_ausd_bp_ta_p_basisprod.py`) is based on filename inference rather than explicit Ab Initio graph invocation parameters.
    *   **Risk:** The complexity and exact logic within `r_ausd_bp_ta_p_basisprod.ksh` are unknown without its content. A direct conversion may not be straightforward.
    *   **Mitigation:** The content of `r_ausd_bp_ta_p_basisprod.ksh` needs to be analyzed to accurately determine the PySpark equivalent.
*   **GCP Placeholders:** Several GCP configuration values (`YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_REGION`, `YOUR_DATAPROC_CLUSTER_NAME`, `YOUR_BUCKET_NAME`) are placeholders and must be populated during implementation.
*   **Retry Policy:** The UC4 XML does not explicitly define a retry policy.
    *   **Risk:** Default Airflow retry behavior might not align with original intent.
    *   **Mitigation:** Confirm original retry requirements during the detailed design phase.

## 8. Build Plan

The build plan will focus on generating the Airflow DAG and the PySpark script.

**1. Airflow DAG Generation (Python):**

*   **File:** `dags/dw_bert_ausd_bp_ta_p_basisprod.py`
*   **Language:** Python
*   **Content:**
    ```python
    from datetime import datetime, timedelta
    from airflow import DAG
    from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

    # GCP Configuration (PLACEHOLDERS)
    GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
    DATAPROC_REGION = "YOUR_DATAPROC_REGION"
    DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
    GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"
    PYSPARK_SCRIPT_URI = f"gs://{GCS_BUCKET_NAME}/pyspark_scripts/r_ausd_bp_ta_p_basisprod.py"

    default_args = {
        "owner": "airflow",
        "retries": 0, # Based on analysis, no explicit retries in UC4 XML
        "retry_delay": timedelta(minutes=5), # Placeholder
        "start_date": datetime(2023, 1, 1), # PLACEHOLDER_START_DATE
    }

    with DAG(
        dag_id="dw_bert_ausd_bp_ta_p_basisprod",
        schedule=None,  # No schedule derived from partial UC4 export
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=False,
        default_args=default_args,
        tags=["bert", "dataproc", "pyspark"],
    ) as dag:
        run_dataproc_job = DataprocSubmitJobOperator(
            task_id="run_dw_bert_ausd_bp_ta_p_basisprod",
            project_id=GCP_PROJECT_ID,
            region=DATAPROC_REGION,
            cluster_name=DATAPROC_CLUSTER_NAME,
            job={
                "placement": {
                    "cluster_name": DATAPROC_CLUSTER_NAME
                },
                "pyspark_job": {
                    "main_python_file_uri": PYSPARK_SCRIPT_URI,
                    # Add arguments here if r_ausd_bp_ta_p_basisprod.py requires them
                    # "args": ["--job-kennung", "AUSD_BP_TA_P_BASISPROD"]
                },
            },
            retries=0, # Matches default_args
            retry_delay=timedelta(minutes=5), # Matches default_args
        )

        # Dependencies
        # start >> run_dataproc_job >> end (implicitly handled by Airflow)
    ```

**2. PySpark Application Development:**

*   **File:** `gs://YOUR_BUCKET_NAME/pyspark_scripts/r_ausd_bp_ta_p_basisprod.py`
*   **Language:** Python (PySpark)
*   **Content:** This file needs to be developed based on the detailed analysis of the original `r_ausd_bp_ta_p_basisprod.ksh` shell script. It should implement the "preparation of instantiated base products" logic, utilizing Spark for distributed processing and BigQuery as the target data warehouse.

This build plan assumes the shell script's logic is entirely migratable to PySpark and does not contain complex system interactions better suited for other GCP services.