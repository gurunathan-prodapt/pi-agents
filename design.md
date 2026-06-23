# Migration Design — DW.BERT_AUSD_BP_TA_BCP_ICCID

## 1. Purpose & Scope
The purpose of this migration is to convert the legacy UC4 job `DW.BERT_AUSD_BP_TA_BCP_ICCID` to an Airflow Directed Acyclic Graph (DAG) for execution on Google Cloud Platform (GCP), specifically leveraging Dataproc for the underlying data processing. The original UC4 job is responsible for "Aufbereitung der instantiierten Basisprodukte" (preparation of instantiated base products) related to ICCID. The scope of this document covers the migration of the UC4 orchestration to Airflow and the high-level design for migrating the invoked shell script to PySpark.

## 2. Source Inventory
The job `DW.BERT_AUSD_BP_TA_BCP_ICCID` is composed of a single UC4 XML file:

*   **File Name**: `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_BCP_ICCID.xml`
*   **Technology**: UC4/Automic Job (Unix Type)
*   **Complexity Tier**: (Not available from analysis)
*   **Automation Bucket**: `semi_auto`
*   **Description**: This XML defines a UC4 Unix job named `DW.BERT_AUSD_BP_TA_BCP_ICCID` that executes a ksh script (`r_ausd_bp_ta_bcp_iccid.ksh`). It uses UC4 include files `DW.HOLE_PFAD` and `DW.BERT_LESE_LOG` and runs on host `DWHDWH2P` with login `DW.UNIX.ISBERT`.

## 3. Target Architecture
The target architecture involves migrating the UC4 orchestration to an Airflow DAG. The shell script (`r_ausd_bp_ta_bcp_iccid.ksh`) invoked by the UC4 job will be refactored into a PySpark script (`r_ausd_bp_ta_bcp_iccid.py`) for execution on a Google Cloud Dataproc cluster. This PySpark script will handle the core data processing logic.

*   **Orchestration**: Airflow DAG (`dw_bert_ausd_bp_ta_bcp_iccid`)
*   **Execution Environment**: Google Cloud Platform (GCP)
*   **Compute**: Google Cloud Dataproc for PySpark job execution
*   **Storage**: Google Cloud Storage (GCS) for PySpark scripts and any input/output data.

## 4. Data Flow & Lineage
The original UC4 job's execution flow:
1.  The UC4 job `DW.BERT_AUSD_BP_TA_BCP_ICCID` is triggered (manual or external schedule, as no UC4 schedule was provided).
2.  The job prepares the environment by including `DW.HOLE_PFAD` and setting `DWH_JOB_KENNUNG`.
3.  It then executes the shell script `$HOME/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_iccid.ksh`.
4.  After the script execution, it includes `DW.BERT_LESE_LOG`.
5.  The UC4 job interacts with the host `DWHDWH2P` using login `DW.UNIX.ISBERT`.

**Migrated Data Flow:**
1.  An Airflow DAG `dw_bert_ausd_bp_ta_bcp_iccid` will be defined.
2.  A `DataprocSubmitJobOperator` task within the DAG will be responsible for submitting the PySpark script `r_ausd_bp_ta_bcp_iccid.py` to a Dataproc cluster.
3.  The PySpark script `r_ausd_bp_ta_bcp_iccid.py` will contain the transformed business logic from the original ksh script.
4.  Any environment setup or logging (`DW.HOLE_PFAD`, `DW.BERT_LESE_LOG`) will either be integrated into the Airflow environment, the PySpark script, or replaced with GCP-native logging mechanisms (e.g., Cloud Logging).
5.  Interactions with the legacy host `DWHDWH2P` and login `DW.UNIX.ISBERT` will be replaced by Dataproc's access to GCP resources, potentially utilizing service accounts for authentication and GCS for data transfer.

**Task Dependency Map (Airflow):**
`start >> run_dw_bert_ausd_bp_ta_bcp_iccid >> end`

## 5. Transformation Logic
*   **UC4 XML to Airflow DAG**: The UC4 job definition will be translated into a Python-based Airflow DAG. This includes defining the DAG's metadata (ID, owner, start date, retry policy) and a single task.
*   **Shell Script to PySpark**: The core logic within `r_ausd_bp_ta_bcp_iccid.ksh` needs to be analyzed and converted to a PySpark script. This conversion will involve:
    *   Identifying data sources and targets used by the ksh script.
    *   Translating shell commands and any embedded SQL/scripting logic to PySpark DataFrame operations or BigQuery SQL.
    *   Ensuring proper error handling and logging within the PySpark script.
*   **UC4 Includes**: The functionality of `DW.HOLE_PFAD` (likely environment variable setup or path configuration) and `DW.BERT_LESE_LOG` (likely custom logging) must be replicated. `DW.HOLE_PFAD` might be handled by Airflow variables or environment settings, while `DW.BERT_LESE_LOG` can be replaced with standard Python logging within the PySpark script, integrated with Cloud Logging.

## 6. External Dependencies
*   **Legacy Host `DWHDWH2P`**: This host will be replaced by the Dataproc cluster where the PySpark job will execute. Network connectivity and data access will be managed within the GCP environment.
*   **Legacy Login `DW.UNIX.ISBERT`**: This Unix login will be superseded by GCP service accounts assigned to the Dataproc cluster, providing fine-grained access control to GCP resources.
*   **Unidentified Script (`r_ausd_bp_ta_bcp_iccid.ksh`)**: The internal dependencies (databases, files, other scripts) of this ksh script are currently unknown and require further analysis during its conversion to PySpark.

## 7. Unresolved / Risks
*   **Detailed `r_ausd_bp_ta_bcp_iccid.ksh` Analysis**: The exact business logic, data sources, transformations, and output targets of the `r_ausd_bp_ta_bcp_iccid.ksh` script are not yet fully understood. This is the primary unresolved item, contributing to the `semi_auto` migration bucket. A deeper dive into the ksh script content is required to complete the transformation logic.
*   **Schedule Definition**: The original UC4 job did not have an explicit schedule provided in the analysis. The Airflow DAG will be unscheduled (`schedule=None`) by default, requiring manual triggering or an external scheduler. If a schedule exists in a broader UC4 workflow, it must be identified and applied to the Airflow DAG.
*   **GCP Placeholders**: GCP project ID, Dataproc region, Dataproc cluster name, and GCS bucket name are placeholders and must be configured manually for deployment.
*   **UC4 Include File Functionality**: The precise actions of `DW.HOLE_PFAD` and `DW.BERT_LESE_LOG` need to be determined and their functionality mapped to suitable Airflow or PySpark implementations.

## 8. Build Plan
1.  **Airflow DAG Generation (Python)**: Generate the Airflow DAG file (`dw_bert_ausd_bp_ta_bcp_iccid.py`) based on the provided design.
2.  **PySpark Script Development (Python)**: Develop the `r_ausd_bp_ta_bcp_iccid.py` PySpark script. This will involve:
    *   Reading the content of the `r_ausd_bp_ta_bcp_iccid.ksh` file.
    *   Analyzing the ksh script to identify data manipulation logic, external calls, and file operations.
    *   Translating the identified logic into PySpark DataFrame transformations or other appropriate PySpark code.
    *   Integrating logging and error handling.
3.  **GCS Deployment**: Upload the `r_ausd_bp_ta_bcp_iccid.py` PySpark script to a designated GCS bucket (`gs://YOUR_BUCKET_NAME/pyspark_scripts/`).
4.  **Airflow Deployment**: Deploy the generated Airflow DAG `dw_bert_ausd_bp_ta_bcp_iccid.py` to the Airflow environment.
5.  **GCP Resource Configuration**: Configure the GCP project, Dataproc cluster, and GCS bucket as per the placeholders in the Airflow DAG.