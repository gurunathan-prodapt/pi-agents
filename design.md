# Migration Design — DW.BERT_AUSD_BP_TA_RN_VERTRAG

## 1. Purpose & Scope

The legacy job `DW.BERT_AUSD_BP_TA_RN_VERTRAG` is an Automic (UC4) Unix job responsible for the "Aufbereitung der instantiierten Basisprodukte" (preparation of instantiated basic products). Its primary function is orchestration, executing a shell script named `r_ausd_bp_ta_rn_vertrag.ksh` which likely contains the core data processing logic. This document outlines the migration design to convert this UC4 job and its associated shell script to run on Google Cloud Platform, using Airflow for orchestration and PySpark on Dataproc for data processing, targeting BigQuery for data storage and manipulation.

## 2. Source Inventory

The migration involves a single primary source file:

*   **File Name**: `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_RN_VERTRAG.xml`
    *   **Technology**: UC4/Automic (XML configuration for a Unix job)
    *   **Category**: `uc4`
    *   **Purpose**: Unix job definition, orchestrating a shell script.
    *   **Complexity Tier**: (Not specified in analysis, implies basic or simple orchestration.)
    *   **Automation Bucket**: `semi_auto`
    *   **Content Summary**: Defines a Unix job (`JOBS_UNIX`) that includes a `SCRIPT` section to execute a shell script (`&HOME/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_vertrag.ksh`). It also includes variable settings (`&DWH_JOB_KENNUNG`), and includes (`inc`) other UC4 objects (`DW.HOLE_PFAD`, `DW.BERT_LESE_LOG`).

## 3. Target Architecture

The migrated job will leverage Google Cloud Platform services:

*   **Orchestration**: Apache Airflow (managed by Cloud Composer) will replace UC4 for job scheduling and workflow management. An Airflow DAG will be generated to represent the UC4 job.
*   **Data Processing**: The logic currently residing in `r_ausd_bp_ta_rn_vertrag.ksh` will be rewritten as a PySpark script (`r_ausd_bp_ta_rn_vertrag.py`). This script will be executed on a Dataproc cluster.
*   **Data Storage/Target**: BigQuery will be the target data warehouse for any data output or transformation performed by the PySpark script.
*   **Execution Environment**: The Dataproc cluster will serve as the execution environment for the PySpark jobs.

## 4. Data Flow & Lineage

The original UC4 job acts as an orchestrator. The direct data flow for `DW.BERT_AUSD_BP_TA_RN_VERTRAG` is as follows:

1.  **UC4 Job Execution**: The `DW.BERT_AUSD_BP_TA_RN_VERTRAG` UC4 job is triggered (schedule not provided in the input, but implied by the "JOB" type).
2.  **Shell Script Invocation**: The UC4 job executes the shell script `/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_vertrag.ksh`.
3.  **Data Processing (within ksh)**: The shell script `r_ausd_bp_ta_rn_vertrag.ksh` likely performs data extraction, transformation, or loading operations. Its exact logic is unknown without analyzing the script content.
4.  **Logging**: The UC4 job also includes other UC4 objects related to path handling (`DW.HOLE_PFAD`) and logging (`DW.BERT_LESE_LOG`).

In the target architecture:

1.  **Airflow DAG Trigger**: The `dw_bert_ausd_bp_ta_rn_vertrag` Airflow DAG will be triggered (schedule to be defined).
2.  **Dataproc Job Submission**: The DAG will submit a PySpark job (`r_ausd_bp_ta_rn_vertrag.py`) to a Dataproc cluster.
3.  **PySpark Processing**: The PySpark script will execute the transformed business logic, reading from source systems (if any, not explicitly identified in this job's lineage) and writing to BigQuery.
4.  **Logging/Monitoring**: Airflow's native logging and monitoring capabilities will replace UC4's internal logging.

## 5. Transformation Logic

*   **Orchestration Logic (UC4 to Airflow DAG)**:
    *   The `DW.BERT_AUSD_BP_TA_RN_VERTRAG` UC4 Unix job will be translated into a Python-based Airflow DAG named `dw_bert_ausd_bp_ta_rn_vertrag`.
    *   The DAG will contain a single task: `run_dw_bert_ausd_bp_ta_rn_vertrag`, implemented using a `DataprocSubmitJobOperator`.
    *   This operator will be configured to submit the `r_ausd_bp_ta_rn_vertrag.py` PySpark script to a Dataproc cluster.
    *   The current design suggests `retries=0` and `retry_delay=timedelta(seconds=0)`, reflecting the lack of explicit retry logic in the UC4 XML. This can be adjusted based on business requirements.
    *   UC4's variable handling (`:set &DWH_JOB_KENNUNG=...`) will need to be managed by Airflow variables or passed as arguments to the PySpark job.

*   **Data Transformation Logic (Shell Script to PySpark)**:
    *   The content of the `r_ausd_bp_ta_rn_vertrag.ksh` shell script is critical and needs detailed analysis. This script is expected to contain the actual data processing logic, potentially involving SQL queries or other shell commands interacting with a database.
    *   This logic will be re-implemented in PySpark, using Spark SQL or DataFrames to interact with BigQuery.
    *   Any `inc` (include) statements in the original UC4 script (e.g., `:inc DW.HOLE_PFAD`, `:inc DW.BERT_LESE_LOG`) will need to be translated to appropriate Python functions or modules within the PySpark script or as separate Airflow tasks, depending on their functionality.

## 6. External Dependencies

*   **Legacy Host/Login**: The UC4 job specifies `HostDst: |DWHDWH2P|HOST` and `Login: DW.UNIX.ISBERT`.
    *   **Replacement**: In GCP, these will be replaced by a Dataproc cluster (for `DWHDWH2P|HOST`) and a Google Cloud Service Account (for `DW.UNIX.ISBERT`) with appropriate IAM roles for Dataproc and BigQuery access.
*   **Legacy Shell Script**: The UC4 job invokes `r_ausd_bp_ta_rn_vertrag.ksh`.
    *   **Replacement**: This will be replaced by a PySpark script, `r_ausd_bp_ta_rn_vertrag.py`, deployed to Google Cloud Storage (e.g., `gs://YOUR_BUCKET_NAME/pyspark_scripts/r_ausd_bp_ta_rn_vertrag.py`).
*   **Database Interactions (Implied)**: If the `r_ausd_bp_ta_rn_vertrag.ksh` script interacts with a source database (e.g., Oracle, Teradata), this dependency will need to be identified from the script's content.
    *   **Replacement**: These interactions will be re-engineered to read data from source systems (e.g., using BigQuery External Tables, direct connectors, or a separate ingestion pipeline) and write to BigQuery.

## 7. Unresolved / Risks

*   **Shell Script Content Analysis**: The most significant unknown is the detailed functionality and complexity of the `r_ausd_bp_ta_rn_vertrag.ksh` shell script. A thorough manual review and analysis of this script are essential to accurately capture its business logic and translate it to PySpark. This is why the job is categorized as `semi_auto`.
*   **Missing Schedule**: The UC4 export did not contain scheduling information (`EVNT_TIME`). The Airflow DAG will initially be created without a schedule, requiring manual definition of the schedule based on business requirements.
*   **Incomplete Workflow Export**: The tool noted that this is an incomplete workflow export as only one `JOBS_UNIX` file was provided without a `JOBP` or `JSCH` container. This means there might be other upstream or downstream UC4 jobs that interact with this one that are not part of the current scope.
*   **GCP Placeholders**: Several GCP-specific parameters (e.g., `YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_REGION`, `YOUR_DATAPROC_CLUSTER_NAME`, `YOUR_BUCKET_NAME`) need to be concretely defined and configured for the target environment.
*   **UC4 Includes**: The UC4 includes like `DW.HOLE_PFAD` and `DW.BERT_LESE_LOG` need to be analyzed to understand their functionality and determine the appropriate translation in the GCP environment (e.g., utility functions, shared modules, or separate Airflow tasks).
*   **Error Handling and Retries**: The UC4 XML did not explicitly define sophisticated retry mechanisms. The current Airflow design defaults to no retries. This needs to be reviewed and aligned with desired operational robustness.

## 8. Build Plan

The migration will involve the following steps:

1.  **Airflow DAG Generation (Python)**:
    *   Generate the `dw_bert_ausd_bp_ta_rn_vertrag.py` Airflow DAG file based on the provided design (Section 2 - Pseudocode from CM MCP output).
    *   Fill in the GCP placeholder values (`GCP_PROJECT_ID`, `DATAPROC_REGION`, `DATAPROC_CLUSTER_NAME`, `GCS_BUCKET_NAME`).
    *   Define the appropriate schedule for the DAG.
    *   **(Language**: Python)
2.  **Shell Script to PySpark Conversion (Python/Spark SQL)**:
    *   Manually analyze the `r_ausd_bp_ta_rn_vertrag.ksh` shell script.
    *   Re-implement the data processing and transformation logic in a PySpark script, `r_ausd_bp_ta_rn_vertrag.py`.
    *   Ensure the PySpark script reads from relevant source systems (potentially via BigQuery external tables or direct BigQuery queries) and writes its output to BigQuery tables.
    *   Address any variables or shared logic from UC4 includes.
    *   **(Language**: Python with Spark SQL/DataFrames)
3.  **Deployment**:
    *   Deploy the `dw_bert_ausd_bp_ta_rn_vertrag.py` Airflow DAG to Cloud Composer.
    *   Upload the `r_ausd_bp_ta_rn_vertrag.py` PySpark script to a GCS bucket.
4.  **Testing**: Thoroughly test the migrated Airflow DAG and PySpark job to ensure functional equivalence and performance.
5.  **Monitoring and Alerting**: Configure Cloud Monitoring and Alerting for the new Airflow DAG and Dataproc jobs.