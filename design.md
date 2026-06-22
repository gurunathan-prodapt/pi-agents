# Migration Design — EXIS_SD_APT_RABATT

## 1. Purpose & Scope

The `EXIS_SD_APT_RABATT` job is responsible for extracting discount-related data from an Oracle database, processing it, compressing it, and distributing it to a target system via SFTP, while also archiving a copy. This job is orchestrated by a UC4 (Automic Workload Automation) UNIX job definition. The output is a CSV file named `DWHM_APT_RABATTREPORT_<SYSDATE YYYYMMDDHH24MISS>.csv.gz`.

**Scope:** Migrate the entire `EXIS_SD_APT_RABATT` job, including its data extraction, transformation, and distribution components, to Google Cloud Platform (GCP) with BigQuery as the primary data warehouse and Cloud Storage for staging/archiving, orchestrated by Cloud Composer (Airflow).

## 2. Source Inventory

| File Path                                                                                                                                                             | Technology               | Category | Complexity | Migration Bucket | Description                                                                                                                                                                                                                                                               |
| :-------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :----------------------- | :------- | :--------- | :--------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_TAEGLICH_JP/DW.DWH_EXIS_SD_APT_RABATT.xml` | UC4/Automic Workload Automation | uc4      | medium     | semi_auto        | UC4 UNIX Job definition for orchestrating the discount data export. It calls an external shell script `r_exis_v2` with a configuration file.                                                                                                                            |
| `vobs/dw_source/isdwh/exporter/apt/cfg/h_exis_apt_rabattdaten.var`                                                                                                    | Custom ETL Framework     | config   | medium     | retire           | Configuration file for the `r_exis_v2` exporter. Defines job ID, output separator, destination path, includes the SQL query, specifies post-processing steps (`nawk`, `gzip`), and details SFTP distribution and local archiving.                                              |
| `vobs/dw_source/isdwh/exporter/apt/sql/d_exis_apt_rabattdaten.sql`                                                                                                    | Oracle PL/SQL            | sql      | medium     | retire           | Oracle SQL query that selects and aggregates discount data from multiple `RPT$TA_S_D1_VERTRAG`, `RPT$TA_S_D1_DISCOUNT_RR`, `SOF$TA_BPR_OPTIONEN`, and `SOF$VI_L_OPTIONZUORDNUNG` tables. Uses `LISTAGG` for aggregation.                                                       |

## 3. Target Architecture

The target platform is Google Cloud Platform (GCP), utilizing the following components:

*   **Orchestration:** Cloud Composer (Apache Airflow) for scheduling and managing the end-to-end workflow.
*   **Data Extraction & Transformation:**
    *   **Data Source:** Legacy Oracle database will be migrated to BigQuery. For the interim, a federated query or Cloud SQL proxy might be used for direct Oracle access, or a data ingestion service (e.g., DataStream, Fivetran) will replicate necessary Oracle tables to BigQuery.
    *   **SQL Logic:** Transformed into BigQuery Standard SQL, potentially within a BigQuery script or a data transformation tool (e.g., dbt) if more complex.
    *   **Post-processing (nawk):** Re-implemented using Python scripting (Pandas, custom logic) within a Cloud Function or a PythonOperator in Airflow.
    *   **Compression (gzip):** Handled natively by Cloud Storage during file uploads or by Python libraries.
*   **Data Landing & Archiving:** Cloud Storage buckets.
    *   `gs://<project_id>-apt-rabatt-export/work/`: For the immediate CSV output.
    *   `gs://<project_id>-apt-rabatt-export/archive/`: For archiving the processed files.
*   **Data Distribution:** Cloud Storage with `gsutil` for inter-bucket transfers or managed file transfer services for external SFTP targets (e.g., Managed Service for Microsoft Active Directory (MS AD) if the SFTP server is Windows-based, or a custom SFTP solution on GCE if external system cannot consume from Cloud Storage directly).

**BigQuery Dataset & Tables:**
*   **Staging/External Tables:** For initial ingestion of Oracle data, if direct replication is used.
*   **`dwh_apt_rabatt` dataset:** To house the transformed `rabatt` data.
*   **`dwh_apt_rabatt.rabatt_report` table:** The target table corresponding to the output of the SQL query, before CSV export.

## 4. Data Flow & Lineage

The migrated job will follow this data flow:

1.  **Orchestration (Cloud Composer/Airflow):**
    *   An Airflow DAG (`exis_sd_apt_rabatt_dag.py`) will be scheduled to run daily (or as per original UC4 schedule).
    *   The DAG will contain tasks for each stage of the process.

2.  **Data Extraction (BigQuery):**
    *   A BigQuery SQL query (derived from `d_exis_apt_rabattdaten.sql`) will select data from source tables (either replicated Oracle tables in BigQuery or directly from Oracle via federated query if necessary) and store the results in a temporary BigQuery table or directly export to CSV.
    *   **Source Tables:**
        *   `ORACLE_DATA.RPT_TA_S_D1_VERTRAG` (or equivalent in BQ)
        *   `ORACLE_DATA.RPT_TA_S_D1_DISCOUNT_RR` (or equivalent in BQ)
        *   `ORACLE_DATA.SOF_TA_BPR_OPTIONEN` (or equivalent in BQ)
        *   `ORACLE_DATA.SOF_VI_L_OPTIONZUORDNUNG` (or equivalent in BQ)
    *   **Target:** `PROJECT_ID.dwh_apt_rabatt.rabatt_report_staging` (temporary table) or directly to Cloud Storage as CSV.

3.  **Post-processing & Formatting (Python/Cloud Function):**
    *   The data (CSV format) from BigQuery will be fetched (if not directly exported to GCS).
    *   A Python script (either via `PythonOperator` in Airflow or a Cloud Function invoked by Airflow) will perform the `nawk`-like post-processing: adding a custom header/footer line as defined in `h_exis_apt_rabattdaten.var`.
    *   The processed file will be compressed using `gzip`.

4.  **Landing Zone (Cloud Storage):**
    *   The final compressed CSV file (`DWHM_APT_RABATTREPORT_<timestamp>.csv.gz`) will be placed in the `gs://<project_id>-apt-rabatt-export/work/` bucket.

5.  **Distribution (Cloud Storage/SFTP):**
    *   **SFTP:** If an external SFTP system is still required, the file from `gs://<project_id>-apt-rabatt-export/work/` will be transferred to the external SFTP server using an Airflow `SftpOperator` or a custom transfer mechanism (e.g., Cloud Function or GCE instance running `gsutil cp` and then `sftp`).
    *   **Internal Distribution:** If the target system can consume from Cloud Storage, the file can be moved/copied directly.

6.  **Archiving (Cloud Storage):**
    *   After successful distribution, the file from `gs://<project_id>-apt-rabatt-export/work/` will be moved to `gs://<project_id>-apt-rabatt-export/archive/` for long-term storage.

**Execution Order:**
UC4 Job (`DW.DWH_EXIS_SD_APT_RABATT.xml`) -> Exporter (`r_exis_v2`) using Config (`h_exis_apt_rabattdaten.var`) -> Executes SQL (`d_exis_apt_rabattdaten.sql`) -> Processes output (nawk, gzip) -> Distributes (SFTP) & Archives.

## 5. Transformation Logic

**UC4 Orchestration (`DW.DWH_EXIS_SD_APT_RABATT.xml`):**
*   **Legacy:** UNIX job calling `r_exis_v2` with `h_exis_apt_rabattdaten.var` as a parameter. Includes variables and `DW.HOLE_PFAD`, `DW.LESE_LOG` includes.
*   **Target (Airflow DAG):** An Airflow DAG `exis_sd_apt_rabatt_dag.py` will encapsulate the entire workflow.
    *   `DW.HOLE_PFAD` and `DW.LESE_LOG` will need to be analyzed for their functionality. If they are generic logging/path setup, they can be replaced by Airflow's native logging and environment variables.
    *   The call to `r_exis_v2` will be broken down into specific Airflow tasks for SQL execution, post-processing, and distribution.

**Configuration (`h_exis_apt_rabattdaten.var`):**
*   **Legacy:** A `.var` file defining metadata (`JOBID`, `SEPARATOR`, `DESTINATION`), `OUTPUT_SQL` inclusion, `POSTPROCESSING` (nawk, gzip), and `DISTRIBUTION` (SFTP, MOVE).
*   **Target (Airflow/Python):**
    *   Metadata (`JOBID`, `SEPARATOR`): Can be defined as Airflow variables or passed as parameters to Python scripts.
    *   `DESTINATION`: Will map to a Cloud Storage path. Timestamping will be handled by Airflow macros (e.g., `{{ ds_nodash }}_{{ ts_nodash }}`).
    *   `OUTPUT_SQL include`: This directs the exporter to use the SQL file. In Airflow, this will translate to a `BigQueryOperator` or a Python script calling BigQuery.
    *   `POSTPROCESSING` (nawk, gzip): This will be implemented as a Python script (`PythonOperator`) within the Airflow DAG. The `nawk` logic (adding `X|<DESTINATION_FILE>|<FROM YYYYMMDD>|` NR `|V_S_Rabattreport|<SYSDATE YYYYMMDD>`) will be translated to Python string manipulation or Pandas operations. `gzip` compression is standard in Python.
    *   `DISTRIBUTION` (SFTP, MOVE):
        *   SFTP details (`PORT`, `USER`, `HOST`, `DIR`): Will be stored as Airflow Connections and used by an `SftpOperator`.
        *   `COMPRESS`: Handled by the Python post-processing task.
        *   `MOVE =$DW_DIR_EXP_APT/store`: Will translate to a `GoogleCloudStorageMoveOperator` or a `BashOperator` with `gsutil mv` command.

**SQL Query (`d_exis_apt_rabattdaten.sql`):**
*   **Legacy:** Oracle PL/SQL query with `PARALLEL` hints, joins, `DISTINCT`, and `LISTAGG`.
    *   Reads from `RPT$TA_S_D1_VERTRAG`, `RPT$TA_S_D1_DISCOUNT_RR`, `SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG`.
*   **Target (BigQuery Standard SQL):**
    *   The Oracle-specific hints (`/*+ PARALLEL(4)*/`, `USE_HASH`) will be removed as BigQuery automatically optimizes queries.
    *   `LISTAGG` in Oracle is equivalent to `STRING_AGG` in BigQuery. The `WITHIN GROUP (ORDER BY BPR_ID)` clause will be preserved.
    *   Table names will be updated to their BigQuery equivalents (e.g., `PROJECT_ID.SOURCE_DATASET.RPT_TA_S_D1_VERTRAG`).
    *   `CAST (LISTAGG(BPR_ID,',') WITHIN GROUP( ORDER BY BPR_ID) AS VARCHAR2(500)) BASISPRODUKTE` will become `STRING_AGG(CAST(BPR_ID AS STRING), ',') WITHIN GROUP (ORDER BY BPR_ID) AS BASISPRODUKTE` in BigQuery.

## 6. External Dependencies

| External System           | Original Reference                 | Replacement/Migration Strategy                                                                       |
| :------------------------ | :--------------------------------- | :--------------------------------------------------------------------------------------------------- |
| **Oracle Database**       | `RPT$TA_S_D1_VERTRAG`, `RPT$TA_S_D1_DISCOUNT_RR`, `SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG` | **Option 1 (Preferred):** Replicate Oracle tables to BigQuery using DataStream, Fivetran, or similar. **Option 2 (Interim):** Use BigQuery Federated Queries or Cloud SQL proxy to connect to on-prem Oracle. |
| **SFTP Server**           | `DW_APT_SFTP_SERVER`, `DW_APT_SFTP_PORT`, `DW_APT_SFTP_USER`, `DW_APT_SFTP_DIR` | Replace with an Airflow `SftpOperator` using credentials stored in Airflow Connections. Alternatively, investigate if the downstream system can consume directly from Cloud Storage.                                                                            |
| **File System Paths**     | `$DW_DIR_EXP_APT`, `$DW_DIR_ROOT`, `$HOME/aktuell/exporter/is/bin/r_exis_v2`, `$HOME/.dw_init` | Replaced by Cloud Storage buckets for data and Airflow DAGs/Python scripts for logic. Environment variables or Airflow Variables will manage configuration paths. `r_exis_v2` will be replaced by direct Airflow tasks.                                                      |
| **UC4 (Automic)**         | `DW.DWH_EXIS_SD_APT_RABATT.xml`, `:inc DW.HOLE_PFAD`, `:inc DW.LESE_LOG` | Replaced by Cloud Composer (Apache Airflow) DAG. UC4 includes (`DW.HOLE_PFAD`, `DW.LESE_LOG`) will be analyzed and their functionality incorporated into the Airflow DAG or Python helper scripts.           |

## 7. Unresolved / Risks

*   **`DW.HOLE_PFAD` and `DW.LESE_LOG` UC4 Includes:** The exact functionality of these included UC4 objects is unknown. They likely handle common setup or logging, but their content needs to be reviewed to ensure no critical business logic or unique environment setups are missed during migration to Airflow. (Complexity: Medium)
*   **Oracle Data Replication:** The method for migrating or replicating the Oracle source tables to BigQuery is crucial. Any latency or data consistency issues here will impact the entire job. (Risk: High, requiring careful design)
*   **SFTP Target System Compatibility:** Confirm if the receiving SFTP system can adapt to consuming files directly from a Cloud Storage bucket, or if an SFTP server hosted on GCP (e.g., GCE instance running SFTP) or a managed SFTP service (e.g., SFTP Gateway) is required. (Risk: Medium, requiring external system coordination)
*   **`r_exis_v2` Exporter Logic:** The `r_exis_v2` script is a custom binary. While its configuration (`.var` file) gives insight, its full behavior needs to be confirmed to ensure the Python re-implementation of the post-processing accurately replicates its functionality. (Risk: Medium, requiring detailed testing)

## 8. Build Plan

The migration will be executed in the following steps:

1.  **Define BigQuery Tables for Oracle Sources:**
    *   Create external tables in BigQuery pointing to Oracle via federated queries (interim) OR
    *   Set up DataStream/Fivetran to continuously replicate `RPT$TA_S_D1_VERTRAG`, `RPT$TA_S_D1_DISCOUNT_RR`, `SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG` to BigQuery.
    *   **Language:** DDL/SQL

2.  **Develop BigQuery SQL Transformation:**
    *   Translate `d_exis_apt_rabattdaten.sql` into BigQuery Standard SQL (`rabatt_data_extraction.sql`).
    *   **Language:** BigQuery Standard SQL

3.  **Develop Python Post-processing Script:**
    *   Create a Python script (`post_process_rabatt_data.py`) to read the BigQuery CSV output, apply the `nawk` logic (adding header/footer line), and `gzip` compress the file.
    *   **Language:** Python

4.  **Create Cloud Composer (Airflow) DAG:**
    *   Design and implement `exis_sd_apt_rabatt_dag.py` to orchestrate the tasks:
        *   **Task 1: `extract_transform_bq`:** Execute `rabatt_data_extraction.sql` in BigQuery, storing output as a temporary CSV in Cloud Storage.
        *   **Task 2: `post_process_and_compress`:** Execute `post_process_rabatt_data.py` on the CSV from Task 1, storing the final `gz` file in `gs://<project_id>-apt-rabatt-export/work/`.
        *   **Task 3: `distribute_to_sftp`:** Use `SftpOperator` to transfer the file from `gs://<project_id>-apt-rabatt-export/work/` to the external SFTP server.
        *   **Task 4: `archive_processed_file`:** Use `GoogleCloudStorageMoveOperator` to move the file from `gs://<project_id>-apt-rabatt-export/work/` to `gs://<project_id>-apt-rabatt-export/archive/`.
    *   **Language:** Python

5.  **Configure Airflow Connections and Variables:**
    *   Set up Airflow Connections for SFTP access.
    *   Define Airflow Variables for any configurable parameters from `h_exis_apt_rabattdaten.var` that aren't dynamic (e.g., `SEPARATOR`).
    *   **Language:** Airflow UI/CLI (YAML/JSON for automation)