# Migration Design — DW.BERT_AUSD_V_TA_P_VERTRAG

## 1. Purpose & Scope
This migration design document outlines the re-platforming of the UC4 job `DW.BERT_AUSD_V_TA_P_VERTRAG` to Google Cloud Platform, specifically using Airflow for orchestration and Dataproc for executing the core business logic. The original UC4 job is responsible for updating contract information related to twin-bill processing by orchestrating a Korn Shell (KSH) script. The scope of this migration is to convert the UC4 job definition into an Airflow DAG and to replace the underlying KSH script with a functionally equivalent PySpark application running on Dataproc, with BigQuery as the target data platform.

## 2. Source Inventory
The primary source component for this job is a single UC4 UNIX job definition.
- **File Path**: `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_P_VERTRAG.xml`
- **Category**: `uc4`
- **Tool**: `UC4/Automic`
- **Summary**: UC4 UNIX job definition that orchestrates the execution of a shell script to update contract information.
- **Complexity Tier**: Not available in analysis data.
- **Migration Bucket**: Not available in analysis data.

The UC4 job (XML content) contains the following script:
```ksh
:inc DW.HOLE_PFAD
:set &DWH_JOB_KENNUNG='AUSD_V_TA_P_VERTRAG'
. $HOME/.dw_init
&HOME/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh
:inc DW.BERT_LESE_LOG
```
This script indicates that the UC4 job primarily:
1. Includes UC4 objects `DW.HOLE_PFAD` and `DW.BERT_LESE_LOG`.
2. Sets a UC4 variable `DWH_JOB_KENNUNG`.
3. Sources an initialization script `$HOME/.dw_init`.
4. Executes the core business logic within the `r_ausd_v_ta_p_vertrag.ksh` Korn Shell script.

## 3. Target Architecture
The migrated job will be implemented as an Airflow DAG deployed on Google Cloud Composer. The core business logic, originally in `r_ausd_v_ta_p_vertrag.ksh`, will be re-platformed to a PySpark application (`r_ausd_v_ta_p_vertrag.py`) executed on a Google Cloud Dataproc cluster. BigQuery is the intended target for all data processing and storage.

- **Orchestration**: Apache Airflow on Cloud Composer.
- **Compute**: Google Cloud Dataproc (for PySpark execution).
- **Data Storage**: Google BigQuery.
- **Output**: The PySpark job will update contract information in BigQuery tables.

## 4. Data Flow & Lineage
The original UC4 job represents a single, self-contained orchestration unit that invokes a shell script. In the target architecture, this translates to a straightforward, linear Airflow DAG.

**Legacy Flow:**
`UC4 Job DW.BERT_AUSD_V_TA_P_VERTRAG.xml`
  -> Includes `DW.HOLE_PFAD`, `DW.BERT_LESE_LOG` (UC4 objects for common functions/variables)
  -> Sets `DWH_JOB_KENNUNG` (UC4 variable)
  -> Sources `$HOME/.dw_init` (shell initialization)
  -> Invokes `r_ausd_v_ta_p_vertrag.ksh` (core business logic)

**Target Airflow DAG Flow:**
The Airflow DAG will consist of a single main task:
`start (Airflow) >> run_bert_ausd_v_ta_p_vertrag (Dataproc PySpark Job) >> end (Airflow)`

- The `run_bert_ausd_v_ta_p_vertrag` task will be a `DataprocSubmitJobOperator` responsible for submitting the `r_ausd_v_ta_p_vertrag.py` PySpark script to a Dataproc cluster.
- The PySpark script is expected to read data (source tables are unknown from current analysis), perform transformations, and write updated contract information to BigQuery.

## 5. Transformation Logic
The original transformation logic resides within the `r_ausd_v_ta_p_vertrag.ksh` shell script. This script is assumed to perform the actual update of contract information. For migration, this KSH script's functionality will be reimplemented as a PySpark application, `r_ausd_v_ta_p_vertrag.py`.

The conversion will involve:
- **Parameter Mapping**: UC4 variables like `DWH_JOB_KENNUNG` or any parameters passed to the KSH script will need to be mapped to Airflow task arguments or Dataproc job parameters.
- **Script Logic Reimplementation**: The specific business logic within `r_ausd_v_ta_p_vertrag.ksh` (data extraction, transformation, and loading) will be rewritten in PySpark to leverage Dataproc's distributed processing capabilities and BigQuery's analytical power.
- **Standard Library/Includes**: The UC4 includes `DW.HOLE_PFAD` and `DW.BERT_LESE_LOG`, and the shell sourcing of `$HOME/.dw_init` will need to be analyzed and their functionalities integrated or replaced with appropriate Python/PySpark modules or Airflow configurations.

## 6. External Dependencies
The original UC4 job has the following external dependencies and considerations:

- **UNIX Host (`|DWHDWH1P|HOST`)**: The UC4 job executes on a specific UNIX host. In the target architecture, this will be abstracted by the Dataproc cluster where the PySpark job runs.
- **UC4 Login (`DW.UNIX.ISBERT`)**: The job runs under a specific UC4 login. This will need to be managed through GCP IAM service accounts for Dataproc and BigQuery access.
- **Shell Scripts (`. $HOME/.dw_init`, `r_ausd_v_ta_p_vertrag.ksh`)**: These local shell scripts will be replaced by a PySpark script and potentially Airflow hooks/operators for initialization.
- **UC4 Includes (`DW.HOLE_PFAD`, `DW.BERT_LESE_LOG`)**: These are UC4-specific objects that likely provide common functions or path definitions. Their content needs to be analyzed and reimplemented as Python modules, Airflow variables, or configurations within the PySpark application.
- **Database Interaction**: The job's purpose ("update contract information") strongly suggests interaction with a database. While not explicitly identified for this job in the provided `lineage_edges`, other `lineage_edges` records show `READS_TABLE` operations on `RPT$TA_S_D1_VERTRAG`, `SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG`, etc., from other SQL scripts. It is highly probable that `r_ausd_v_ta_p_vertrag.ksh` interacts with an Oracle database. In the target, these interactions will be re-engineered to use BigQuery.

## 7. Unresolved / Risks
- **Incomplete Workflow Context**: Only a single UC4 `JOBS_UNIX` object was provided. No `EVNT_TIME` (scheduling), `JOBP` (job plan/workflow), or `JSCH` (job schedule) files were available. This means the overall workflow dependencies, scheduling, and error handling of the broader UC4 process are unknown.
- **Scheduling**: The original schedule cannot be derived from the provided UC4 XML. The Airflow DAG will be configured with a `None` schedule, requiring manual definition of the schedule in Cloud Composer.
- **KSH Script Logic**: The exact transformation logic within `r_ausd_v_ta_p_vertrag.ksh` is not detailed in the analysis. A manual review and reverse engineering of this script will be required to accurately reimplement its functionality in PySpark.
- **UC4 Includes Implementation**: The functionality of `DW.HOLE_PFAD` and `DW.BERT_LESE_LOG` needs to be understood and translated to a GCP-native equivalent.
- **GCP Placeholders**: `YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_REGION`, `YOUR_DATAPROC_CLUSTER_NAME`, and `YOUR_BUCKET_NAME` must be replaced with actual GCP environment values.
- **Retry/Error Handling**: The UC4 job XML did not specify any explicit retry, restart, block, or notification post-conditions. The initial Airflow DAG will default to `retries=0` and no specific `on_failure_callback`, which may need to be adjusted based on business requirements.

## 8. Build Plan
The migration involves generating an Airflow DAG Python file and a PySpark script.

**1. Generate Airflow DAG Python File (`dw_bert_ausd_v_ta_p_vertrag.py`)**
   - **Language**: Python
   - **Tool**: Airflow DAG generation tool (e.g., `airflow_dag_build` from CM MCP, or direct coding based on the pseudocode).
   - **Content**:
     ```python
     from datetime import timedelta
     from airflow import DAG
     from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
     from airflow.utils.dates import days_ago

     # GCP Configuration
     GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
     DATAPROC_REGION = "YOUR_DATAPROC_REGION"
     DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
     GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"
     PYSPARK_SCRIPT_URI = f"gs://{GCS_BUCKET_NAME}/pyspark_scripts/r_ausd_v_ta_p_vertrag.py"

     default_args = {
         'owner': 'data_platform',
         'depends_on_past': False,
         'email_on_failure': False,
         'email_on_retry': False,
         'retries': 0,
         'retry_delay': timedelta(seconds=0),
         'start_date': days_ago(1), # Placeholder, adjust as needed
     }

     with DAG(
         dag_id='dw_bert_ausd_v_ta_p_vertrag',
         default_args=default_args,
         description='Airflow DAG for DW.BERT_AUSD_V_TA_P_VERTRAG',
         schedule_interval=None, # No schedule derived from source UC4 XML
         catchup=False,
         max_active_runs=1,
         is_paused_upon_creation=False,
         tags=['uc4', 'dataproc', 'pyspark', 'bigquery'],
     ) as dag:
         run_pyspark_job = DataprocSubmitJobOperator(
             task_id='run_bert_ausd_v_ta_p_vertrag',
             project_id=GCP_PROJECT_ID,
             region=DATAPROC_REGION,
             job={
                 'placement': {'cluster_name': DATAPROC_CLUSTER_NAME},
                 'pyspark_job': {
                     'main_python_file_uri': PYSPARK_SCRIPT_URI,
                     'args': [
                         # Any arguments passed from UC4 to the KSH script will go here
                         # e.g., '--job_kennung', 'AUSD_V_TA_P_VERTRAG'
                     ],
                 },
             },
         )
     ```

**2. Develop PySpark Script (`r_ausd_v_ta_p_vertrag.py`)**
   - **Language**: Python (PySpark)
   - **Tool**: Manual development/code conversion based on the logic of `r_ausd_v_ta_p_vertrag.ksh`.
   - **Content**: This script will encapsulate the data extraction, transformation, and loading logic to update contract information, interacting with BigQuery. It will be uploaded to a GCS bucket (`gs://YOUR_BUCKET_NAME/pyspark_scripts/`).

**3. Implement UC4 Include Equivalents**
   - The logic of `DW.HOLE_PFAD` and `DW.BERT_LESE_LOG` needs to be analyzed.
   - If they are simple variable definitions, they can be translated to Airflow variables or hardcoded constants in the PySpark script.
   - If they are complex functions, they will be reimplemented as Python modules or helper functions within the PySpark application.

**4. IAM and Networking Configuration**
   - Ensure a service account with appropriate permissions for Dataproc, BigQuery, and GCS is created and assigned.
   - Configure network connectivity for Dataproc to access BigQuery and any other required external systems.