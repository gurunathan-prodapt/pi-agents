# Migration Design — DW.BERT_AUSD_V_TA_VERTRAG_TMP

## 1. Purpose & Scope
This document outlines the migration design for the legacy UC4 job `DW.BERT_AUSD_V_TA_VERTRAG_TMP`. The original UC4 job, titled "Combine contract related information from various sources", is a Unix job that orchestrates the execution of a KornShell (KSH) script (`r_ausd_v_ta_vertrag_tmp.ksh`) to perform contract-related data preparation. The migration aims to re-platform this orchestration to Google Cloud's Cloud Composer (Apache Airflow) and the data processing logic to Dataproc using PySpark.

## 2. Source Inventory
The primary source artifact for this job is a single UC4 XML definition. Detailed metadata for this file in `file_complexity` and `automation_rate` tables was not available.

*   **File:** `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_VERTRAG_TMP.xml`
    *   **Technology:** UC4/Automic JOBS_UNIX
    *   **Category:** Orchestration
    *   **Tool:** UC4/Automic
    *   **Purpose:** Defines a Unix job that combines contract-related information by executing a KornShell script.
    *   **Complexity Tier:** Not available in database.
    *   **Automation Bucket:** Not available in database.

The UC4 job executes a KornShell script named `r_ausd_v_ta_vertrag_tmp.ksh`. Although this script is critical to the job's functionality, no corresponding entry was found in the `file_analysis`, `file_complexity`, or `automation_rate` tables within the provided database context, nor was it identified through lineage tracing. Therefore, its specific content, complexity, and automation bucket remain unknown through automated analysis and will require manual inspection.

## 3. Target Architecture
The migrated solution will leverage Google Cloud Platform (GCP) services:

*   **Orchestration:** Cloud Composer (Apache Airflow) will manage the workflow execution. A single Airflow DAG will replace the UC4 `JOBS_UNIX` object.
*   **Data Processing:** The logic encapsulated within the original KSH script (`r_ausd_v_ta_vertrag_tmp.ksh`) will be transformed into a PySpark script (`r_ausd_v_ta_vertrag_tmp.py`) and executed on a Dataproc cluster.
*   **Storage:** Google Cloud Storage (GCS) will be used to store the PySpark scripts and potentially input/output data.

The Airflow DAG will be named `dw_bert_ausd_v_ta_vertrag_tmp`.

## 4. Data Flow & Lineage
The legacy UC4 job `DW.BERT_AUSD_V_TA_VERTRAG_TMP` invokes an internal KornShell script. In the target architecture, this will translate to a direct execution of a Dataproc job from an Airflow task.

*   **Legacy Flow:**
    *   UC4 Job (`DW.BERT_AUSD_V_TA_VERTRAG_TMP`) -> Executes `r_ausd_v_ta_vertrag_tmp.ksh`
*   **Target Flow:**
    *   Airflow DAG (`dw_bert_ausd_v_ta_vertrag_tmp`)
        *   `start` task
        *   `dw_bert_ausd_v_ta_vertrag_tmp` task (DataprocSubmitJobOperator) -> Executes PySpark script (`r_ausd_v_ta_vertrag_tmp.py`) on Dataproc.
        *   `end` task
Due to the absence of `EVNT_TIME` or `JOBP` files in the source, the Airflow DAG will have a simple linear dependency (`start >> dw_bert_ausd_v_ta_vertrag_tmp >> end`). Any complex scheduling or inter-job dependencies will need to be manually defined after a thorough review of the broader UC4 environment.

## 5. Transformation Logic
The core transformation involves converting the UC4 job definition into an Airflow DAG and the underlying KSH script into a PySpark application.

*   **UC4 to Airflow DAG:** The UC4 `JOBS_UNIX` object `DW.BERT_AUSD_V_TA_VERTRAG_TMP` will be converted into a Python-based Airflow DAG. This DAG will primarily consist of a `DataprocSubmitJobOperator` task responsible for initiating the PySpark job.
    *   **UC4 Variables:** Variables like `&DWH_JOB_KENNUNG='AUSD_V_TA_VERTRAG_TMP'` will need to be mapped to Airflow Variables or passed as arguments to the Dataproc job.
*   **KSH Script to PySpark:** The shell script `r_ausd_v_ta_vertrag_tmp.ksh` performs the actual data processing. This script needs to be re-implemented in PySpark (`r_ausd_v_ta_vertrag_tmp.py`). The exact transformation logic depends on the content of the KSH script, which was not available for automated analysis. It is inferred that the script combines contract-related information from various sources. This implies data extraction, transformation, and loading (ETL) logic that will be replicated in PySpark.

## 6. External Dependencies
The UC4 job references several external components:

*   **Login:** `DW.UNIX.ISBERT` - This Unix login credential will need to be securely managed in GCP, potentially using Service Accounts for Dataproc and appropriate IAM roles.
*   **Host:** `DWHDWH1P` - This indicates the Unix host where the script runs. In GCP, Dataproc clusters will serve as the execution environment. The specific cluster details (project ID, region, name) are currently placeholders.
*   **Include Files:**
    *   `:inc DW.HOLE_PFAD`
    *   `:inc DW.BERT_LESE_LOG`
    These are shared UC4 include files. Their content needs to be reviewed. If they contain significant logic or common functions, they might need to be translated into Python modules or helper functions used by the PySpark script, or they might represent other shared resources (like configuration files or logging utilities) that need to be adapted for GCP.

## 7. Unresolved / Risks
Several aspects could not be fully resolved due to missing information:

*   **KSH Script Content:** The actual logic of `r_ausd_v_ta_vertrag_tmp.ksh` is unknown, as the file content or its analysis was not available in the database. This is a significant risk and requires manual reverse-engineering and re-implementation.
*   **Missing UC4 Workflow Definitions:** The absence of `EVNT_TIME` (scheduling) and `JOBP` (job plan/workflow) objects means the complete scheduling and inter-job dependency chain of this UC4 job within its broader environment could not be automatically derived. The Airflow DAG's schedule and upstream/downstream dependencies will need manual investigation and definition.
*   **Incomplete Metadata:** `file_complexity` and `automation_rate` for the UC4 XML file were not available. This prevents an accurate assessment of migration effort and automation bucket.
*   **Shared Includes:** The logic within `DW.HOLE_PFAD` and `DW.BERT_LESE_LOG` needs to be analyzed to ensure all dependencies are captured and migrated correctly.

## 8. Build Plan
The migration will involve generating the following artifacts:

1.  **Airflow DAG Python file:** `dw_bert_ausd_v_ta_vertrag_tmp.py`
    *   **Language:** Python
    *   **Content:** This file will define the Airflow DAG, including the `DataprocSubmitJobOperator` task to run the PySpark script. It will include placeholders for GCP project ID, region, cluster name, and GCS bucket, which must be manually configured.
2.  **PySpark Script:** `r_ausd_v_ta_vertrag_tmp.py`
    *   **Language:** Python (PySpark)
    *   **Content:** This script will encapsulate the data processing logic derived from the original `r_ausd_v_ta_vertrag_tmp.ksh` KornShell script. This will require manual development based on the reverse-engineered KSH logic. The script will be uploaded to a GCS bucket (e.g., `gs://YOUR_BUCKET_NAME/pyspark_scripts/`).
