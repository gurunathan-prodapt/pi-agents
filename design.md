# Migration Design — EXIS_SD_APT_RABATT

## 1. Purpose & Scope

This job, `EXIS_SD_APT_RABATT`, is an ETL workflow designed to extract discount data, perform post-processing, and distribute the resulting file to target systems. The primary function is to generate a compressed CSV file containing aggregated discount information from an Oracle database.

The scope of this migration encompasses the complete re-platforming of this ETL process to Google Cloud Platform (GCP), specifically targeting:
*   **Orchestration**: Transition from UC4/Automic Workload Automation to Google Cloud Composer (Apache Airflow).
*   **Data Extraction & Transformation**: Convert Oracle PL/SQL queries to Google Standard SQL for execution in BigQuery.
*   **Data Processing**: Migrate custom shell script logic (including configuration parsing, `nawk`, and `gzip`) to Python scripts or appropriate Airflow operators.
*   **Data Distribution**: Adapt existing SFTP and local archiving mechanisms to leverage Google Cloud Storage (GCS) and potentially other GCP services for external data transfer.

## 2. Source Inventory

The `EXIS_SD_APT_RABATT` job consists of the following source files:

| File Path                                                                                                                                              | Technology                  | Category | Tool                            | Summary                                                                                                                                                                                                                                                                                   | Tier  | Automation Bucket |
| :----------------------------------------------------------------------------------------------------------------------------------------------------- | :-------------------------- | :------- | :------------------------------ | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :---- | :---------------- |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_TAEGLICH_JP/DW.DWH_EXIS_SD_APT_RABATT.xml` | UC4 XML                     | uc4      | UC4/Automic Workload Automation | UC4 job definition for exporting discount data to a compressed CSV file.                                                                                                                                                                                                                  | (N/A) | (N/A)             |
| `vobs/dw_source/isdwh/exporter/apt/cfg/h_exis_apt_rabattdaten.var`                                                                                     | Custom Config (Shell Script) | config   | Custom ETL Framework            | Configuration file for an ETL exporter job named EXIS_SD_APT_RABATT, defining its SQL source, post-processing steps (nawk, gzip), and distribution via SFTP and local move to an archive directory.                                                                                            | (N/A) | (N/A)             |
| `vobs/dw_source/isdwh/exporter/apt/sql/d_exis_apt_rabattdaten.sql`                                                                                     | Oracle PL/SQL               | sql      | Oracle PL/SQL                   | This SQL script selects data from multiple tables, performs joins, aggregates data using LISTAGG, and renames columns, intended for export to a CSV file. It reads from `RPT$TA_S_D1_VERTRAG`, `RPT$TA_S_D1_DISCOUNT_RR`, `SOF$TA_BPR_OPTIONEN`, and `SOF$VI_L_OPTIONZUORDNUNG`. | (N/A) | (N/A)             |

*Note: Complexity tier and automation bucket information were not available from the database for these files.*

## 3. Target Architecture

The migrated `EXIS_SD_APT_RABATT` job will operate within Google Cloud Platform, leveraging the following services:

*   **Orchestration**: **Google Cloud Composer** (managed Apache Airflow) will be used to define, schedule, and monitor the ETL workflow. Each legacy UC4 job will correspond to an Airflow DAG.
*   **Data Source**: The Oracle tables (`RPT$TA_S_D1_VERTRAG`, `RPT$TA_S_D1_DISCOUNT_RR`, `SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG`) are assumed to be migrated or replicated to **Google BigQuery**.
*   **Data Transformation**: **Google BigQuery** will be the execution engine for the SQL transformation logic.
*   **Staging & Output Storage**: **Google Cloud Storage (GCS)** will serve as the primary storage for intermediate data, the final compressed CSV output, and the archive.
*   **Data Distribution**:
    *   **External SFTP**: For sending data to external SFTP targets, an Airflow operator utilizing a secure method (e.g., a custom Python operator with `paramiko` or a Cloud Function if complex logic is involved) will transfer files from GCS.
    *   **Internal Archive**: GCS will be used directly for archiving, performing object move operations within buckets.

## 4. Data Flow & Lineage

The data flow for the migrated job will be sequential, managed by the Airflow DAG:

1.  **Cloud Composer DAG Execution**:
    *   The `exis_sd_apt_rabatt_dag.py` (Airflow DAG, replacing `DW.DWH_EXIS_SD_APT_RABATT.xml`) is triggered by its defined schedule.
    *   This DAG orchestrates the entire workflow.

2.  **Data Extraction & Transformation (BigQuery)**:
    *   An Airflow task executes the converted BigQuery SQL query (`d_exis_apt_rabattdaten.bqsql`).
    *   This query **READS** from the following BigQuery tables (assumed migrated names):
        *   `project_id.dataset_id.RPT_TA_S_D1_VERTRAG`
        *   `project_id.dataset_id.RPT_TA_S_D1_DISCOUNT_RR`
        *   `project_id.dataset_id.SOF_TA_BPR_OPTIONEN`
        *   `project_id.dataset_id.SOF_VI_L_OPTIONZUORDNUNG`
    *   The results of this query are **WRITTEN** to a temporary CSV file in a GCS bucket (e.g., `gs://<bucket>/temp/rabattdaten_raw_<timestamp>.csv`).

3.  **Post-processing (Python in Airflow)**:
    *   An Airflow task (executing `post_process_rabattdaten.py`) **READS** the temporary CSV from GCS.
    *   It applies the specific formatting and trailer line logic previously handled by `nawk`.
    *   It compresses the file using `gzip`.
    *   The final processed and compressed CSV file is **WRITTEN** to the specified destination path in GCS (e.g., `gs://<bucket>/<DW_DIR_EXP_APT>/work/DWHM_APT_RABATTREPORT_<SYSDATE YYYYMMDDHH24MISS>.csv.gz`).

4.  **Data Distribution**:
    *   **SFTP Transfer**: An Airflow task **READS** the final compressed CSV from GCS. It then **WRITES** this file to the external SFTP target system using the configured credentials and path (`$DW_APT_SFTP_SERVER`, `$DW_APT_SFTP_DIR`).
    *   **Archiving**: An Airflow task performs an internal GCS object move operation, moving a copy of (or the original) the processed file from the "work" directory to the designated GCS archive location (`gs://<bucket>/<DW_DIR_EXP_APT>/store/`).

## 5. Transformation Logic

### 5.1. UC4 Job to Airflow DAG (`DW.DWH_EXIS_SD_APT_RABATT.xml` -> `exis_sd_apt_rabatt_dag.py`)

The UC4 job definition will be translated into an Airflow DAG. Key elements include:
*   **Scheduling**: The Airflow DAG will be configured with a schedule (e.g., `schedule_interval='@daily'`) that matches the original UC4 job's execution frequency.
*   **Task Definition**: Individual steps (SQL execution, post-processing, SFTP, archive) will be defined as Airflow tasks using appropriate operators (e.g., `BigQueryOperator`, `PythonOperator`, custom `SftpOperator`).
*   **Environment Variables & Parameters**: UC4 variables and parameters will be managed using Airflow Variables, XComs for inter-task communication, or GCP Secret Manager for sensitive information.
*   **Logging**: Airflow's native logging capabilities will replace UC4's `DW.LESE_LOG` mechanism, integrating with Google Cloud Logging.
*   **Login & Host**: The `Login` (`DW.UNIX.ISTNS`) will be replaced by a GCP Service Account with necessary IAM roles. The `HostDst` (`DWHDWH1P`) will be replaced by the distributed nature of BigQuery and GCS.

### 5.2. Custom Config & Script Logic to Python (`h_exis_apt_rabattdaten.var`, `r_exis_v2` -> `post_process_rabattdaten.py`)

The logic in the `.var` configuration file and the invoked `r_exis_v2` script will be implemented in a dedicated Python script:
*   **Configuration**: The parameters like `JOBID`, `SEPARATOR`, `DESTINATION`, `SFTP` details, and `MOVE` path will be passed as arguments to the Python script or accessed via Airflow Variables.
*   **SQL Inclusion**: The reference to `d_exis_apt_rabattdaten.sql` will be handled by the Airflow DAG, ensuring the BigQuery operator executes the content of the migrated SQL file.
*   **Post-processing (`nawk`)**: The `nawk` command's logic for prepending/appending lines will be translated into Python code. This will involve reading the CSV from GCS (e.g., using Pandas), constructing the trailer line based on dynamic values (`<DESTINATION_FILE>`, `<FROM YYYYMMDD>`, `NR` - row count, `V_S_Rabattreport`, `<SYSDATE YYYYMMDD>`), and writing the modified content.
*   **Compression (`gzip`)**: Python's `gzip` library will be used to compress the output file to the `.gz` format.

**Example Python Snippet for Post-processing (simplified):**
```python
import pandas as pd
import gzip
from datetime import datetime

def post_process_and_compress(input_gcs_path, output_gcs_path, job_id, separator, **context):
    # Fetch job execution date from Airflow context for '<FROM YYYYMMDD>'
    execution_date_str = context['ds_nodash'] # e.g., '20231027'
    current_datetime_str = datetime.now().strftime('%Y%m%d%H%M%S')
    current_date_str = datetime.now().strftime('%Y%m%d')

    # Read data from GCS
    df = pd.read_csv(input_gcs_path, sep=separator)
    
    # Generate trailer line (assuming V_S_Rabattreport is static)
    trailer_line = f"X|{output_gcs_path.split('/')[-1]}|{execution_date_str}|{len(df)}|V_S_Rabattreport|{current_date_str}"
    
    # Write DataFrame to a buffer, append trailer, then compress
    import io
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, sep=separator, index=False, header=True)
    csv_buffer.write("\n" + trailer_line + "\n")
    
    # Compress and upload to GCS
    with gzip.open(f"/tmp/output_{job_id}.csv.gz", 'wt', encoding='utf-8') as gz_file:
        gz_file.write(csv_buffer.getvalue())
    
    # Upload from local /tmp to GCS
    # GCS hook or client upload logic here
    # Example: GoogleCloudStorageHook().upload(bucket_name='your-bucket', object_name=output_gcs_path, filename=f"/tmp/output_{job_id}.csv.gz")
```

### 5.3. Oracle PL/SQL to Google Standard SQL (`d_exis_apt_rabattdaten.sql` -> `d_exis_apt_rabattdaten.bqsql`)

The Oracle `SELECT` statement will be converted to Google Standard SQL:
*   **SQL Dialect**: Transition from Oracle PL/SQL syntax to Google Standard SQL.
*   **Table References**: Oracle table names (`RPT$TA_S_D1_VERTRAG`, etc.) will be updated to their BigQuery fully qualified names (e.g., ``project_id.dataset_id.RPT_TA_S_D1_VERTRAG``).
*   **Hints Removal**: Oracle-specific performance hints like `/*+ PARALLEL(4)*/` and `USE_HASH(...)` will be removed, as BigQuery's query optimizer handles these automatically.
*   **`LISTAGG` to `STRING_AGG`**: The `LISTAGG(BPR_ID,',') WITHIN GROUP( ORDER BY BPR_ID) AS VARCHAR2(500)` function will be replaced by BigQuery's `STRING_AGG(CAST(BPR_ID AS STRING), ',' ORDER BY BPR_ID)`. The `VARCHAR2(500)` cast is generally not needed in BigQuery, which uses `STRING` types without explicit length unless specified in schema definition.
*   **Join Syntax**: The implicit join syntax (`FROM table1, table2 WHERE table1.col = table2.col`) will be converted to explicit `INNER JOIN` syntax for clarity and adherence to modern SQL standards.

**Converted Google Standard SQL (Example):**
```sql
SELECT
    RAHMENVERTRAG_ID,
    CNTRCT_TEMPLATE_ID AS TARIF_ID,
    DWH_TARIFGR_TEXT,
    RABATTIERTE_RECH_POS,
    DISC_INVOICE_ITEM_ID AS RABATTIERTE_RECH_POS_ID,
    RABATTHOEHE,
    STRING_AGG(CAST(BPR_ID AS STRING), ',' ORDER BY BPR_ID) AS BASISPRODUKTE
FROM (
    SELECT DISTINCT
        RPT.RAHMENVERTRAG_ID,
        RPT.DWH_TARIFGR_TEXT,
        DISC.CNTRCT_TEMPLATE_ID,
        DISC.RABATTIERTE_RECH_POS,
        DISC.DISC_INVOICE_ITEM_ID,
        DISC.RABATTHOEHE,
        BPR.BPR_ID
    FROM
        `project_id.dataset_id.RPT_TA_S_D1_VERTRAG` AS RPT
    INNER JOIN
        `project_id.dataset_id.RPT_TA_S_D1_DISCOUNT_RR` AS DISC
        ON RPT.RAHMENVERTRAG_ID = DISC.CONTRACT_NUMBER
        AND RPT.SV_ID = DISC.CNTRCT_TEMPLATE_ID
    INNER JOIN
        `project_id.dataset_id.SOF_TA_BPR_OPTIONEN` AS BPR
        ON RPT.VERTRAG_ID_CARMEN = BPR.CNTRCT_ID
    INNER JOIN
        `project_id.dataset_id.SOF_VI_L_OPTIONZUORDNUNG` AS OPT
        ON BPR.BPR_ID = OPT.OPTION_ID
)
GROUP BY
    RAHMENVERTRAG_ID,
    CNTRCT_TEMPLATE_ID,
    DWH_TARIFGR_TEXT,
    RABATTIERTE_RECH_POS,
    DISC_INVOICE_ITEM_ID,
    RABATTHOEHE;
```

## 6. External Dependencies

| Legacy External System | How it's replaced in GCP                                                                                                                                                                                                                                                                                                                            | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| :--------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Oracle Database**    | **Google BigQuery**                                                                                                                                                                                                                                                                                                                                 | The source tables (`RPT$TA_S_D1_VERTRAG`, `RPT$TA_S_D1_DISCOUNT_RR`, `SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG`) are assumed to be migrated or continuously replicated to BigQuery. The BigQuery SQL query will directly query these target tables.                                                                                                                                                                                                                                                                                                                                             |
| **SFTP Target System** | **Google Cloud Storage (GCS)** for intermediate storage, then an **Airflow SFTP operator** or a dedicated **Cloud Function/Dataflow job** for pushing data.                                                                                                                                                                                         | The SFTP destination parameters (`DW_APT_SFTP_SERVER`, `DW_APT_SFTP_PORT`, `DW_APT_SFTP_USER`, `DW_APT_SFTP_DIR`) will be securely stored as Airflow connections or in GCP Secret Manager. Direct SFTP support from GCS is not natively available; a separate component is required to handle the secure transfer. This may require firewall rules or VPN setup for connectivity.                                                                                                                                                                                                                     |
| **Local Archive Path** | **Google Cloud Storage (GCS) archive bucket/folder**                                                                                                                                                                                                                                                                                                | The local archive directory (`$DW_DIR_EXP_APT/store`) will be mapped to a dedicated archive location within GCS (e.g., `gs://<your-bucket>/archive/`). GCS object move operations will replace the local filesystem move.                                                                                                                                                                                                                                                                                                                                                                    |
| **`DWHDWH1P` (Host)**  | **Google Cloud Composer Worker Nodes**                                                                                                                                                                                                                                                                                                              | The UC4 job's execution environment will be replaced by the managed worker nodes within a Cloud Composer environment.                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| **`DW.UNIX.ISTNS` (Login)** | **GCP Service Account**                                                                                                                                                                                                                                                                                                                             | The legacy Unix login will be replaced by a GCP Service Account. This service account will be granted the necessary IAM roles for interacting with BigQuery (data viewer), GCS (object creator/viewer), and potentially other services involved in SFTP or data movement. It will be associated with the Cloud Composer environment. |

## 7. Unresolved / Risks

*   **Undocumented `r_exis_v2` Logic**: The full functionality of the `r_exis_v2` script is not entirely evident from the provided configuration. Any complex logic, error handling, or specific environment setup within `r_exis_v2` not explicitly defined in `h_exis_apt_rabattdaten.var` might pose a risk during reimplementation. A detailed analysis or reverse-engineering of `r_exis_v2` source code is recommended if available.
*   **Source of `<FROM YYYYMMDD>`**: The `nawk` command's trailer line includes `<FROM YYYYMMDD>`. The exact origin and meaning of this date in the legacy system need to be confirmed to ensure accurate replication in the Python post-processing script. It could be the job execution date, a data extraction start date, or a hardcoded value.
*   **SFTP Connectivity and Security**: Ensuring secure and reliable connectivity to external SFTP targets from GCP can be complex. Depending on network topology and security requirements, this might necessitate setting up VPNs, VPC Service Controls, or using third-party SFTP gateways. This needs careful planning and testing.
*   **Performance of Python Post-processing**: For very large datasets, executing the CSV reading, `nawk` logic, and `gzip` compression directly within an Airflow `PythonOperator` might hit resource limits of the Airflow worker. If performance becomes an issue, this step might need to be scaled up using Google Cloud Dataflow.
*   **Metadata Management for Dynamic Filenames**: The `DESTINATION` path (`DWHM_APT_RABATTREPORT_<SYSDATE YYYYMMDDHH24MISS>.csv.gz`) uses a dynamic timestamp. Ensuring consistent generation and referencing of this timestamp across Airflow tasks (e.g., for SFTP and archiving) is crucial.
*   **UC4 Job Dependencies**: While this design focuses on `EXIS_SD_APT_RABATT`, if this UC4 job has external dependencies (predecessor/successor jobs within UC4 or external systems), those dependencies need to be identified and managed within Airflow (e.g., using `ExternalTaskSensor` or direct scheduling based on external events).

## 8. Build Plan

The migration of `EXIS_SD_APT_RABATT` will proceed in an iterative fashion:

1.  **BigQuery Source Table Readiness (Pre-requisite)**:
    *   Confirm that all source Oracle tables (`RPT$TA_S_D1_VERTRAG`, `RPT$TA_S_D1_DISCOUNT_RR`, `SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG`) are successfully migrated and available in BigQuery with historical and incremental data.

2.  **SQL Transformation Development & Testing**:
    *   **Convert `d_exis_apt_rabattdaten.sql`**: Translate the Oracle PL/SQL query to Google Standard SQL, creating `d_exis_apt_rabattdaten.bqsql`.
    *   **Unit Test SQL**: Execute `d_exis_apt_rabattdaten.bqsql` in BigQuery against sample data to verify data correctness and performance.

3.  **Python Post-processing Development & Testing**:
    *   **Develop `post_process_rabattdaten.py`**: Create a Python script that encapsulates the `nawk` and `gzip` logic, designed to run as an Airflow `PythonOperator`. It should accept input and output GCS paths, and relevant configuration (like separator, job ID, and dynamic date values).
    *   **Unit Test Python Script**: Test `post_process_rabattdaten.py` locally and in a simulated GCS environment with sample CSV data to ensure correct formatting and compression.

4.  **Airflow DAG Development**:
    *   **Create `exis_sd_apt_rabatt_dag.py`**: Develop the Airflow DAG definition in Python.
    *   **Define Tasks**:
        *   `extract_transform_bq_task`: Uses `BigQueryOperator` (or `BigQueryInsertJobOperator`) to execute `d_exis_apt_rabattdaten.bqsql` and export results to GCS.
        *   `post_process_data_task`: Uses `PythonOperator` to execute `post_process_rabattdaten.py`.
        *   `sftp_transfer_task`: Uses an `SftpOperator` (if available and suitable) or a custom `PythonOperator` to send the file to the external SFTP server.
        *   `archive_file_task`: Uses `GCSObjectsWithPrefixMoveOperator` or `PythonOperator` for GCS archiving.
    *   **Configure Connections**: Set up Airflow connections for BigQuery, GCS, and the SFTP server (using secure credentials).
    *   **Error Handling**: Implement Airflow task retries, `on_failure_callback`, and alerting mechanisms.
    *   **Scheduling**: Define the DAG's `schedule_interval` to match the original UC4 job.

5.  **Configuration and Deployment**:
    *   **Externalize Configuration**: Use Airflow Variables or GCP Secret Manager for dynamic parameters (GCS bucket names, SFTP details, `DW_DIR_EXP_APT`, etc.).
    *   **Deploy DAG**: Upload `exis_sd_apt_rabatt_dag.py` and `post_process_rabattdaten.py` to the Cloud Composer DAGs folder.

6.  **Integrated Testing & UAT**:
    *   **End-to-End Testing**: Run the complete Airflow DAG in a staging environment to verify the entire data flow, from BigQuery extraction to SFTP delivery and archiving.
    *   **Data Validation**: Perform thorough data validation with business users (UAT) to ensure the output matches the legacy system's results.

**Generated Files:**

*   `exis_sd_apt_rabatt_dag.py` (Python, Airflow DAG definition)
*   `d_exis_apt_rabattdaten.bqsql` (Google Standard SQL script)
*   `post_process_rabattdaten.py` (Python script for post-processing and compression)
