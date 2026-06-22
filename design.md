# Migration Design — DW.DWH_APT_EXPORT_MONATLICH_JP

## 1. Purpose & Scope
The job `DW.DWH_APT_EXPORT_MONATLICH_JP` is a UC4 Job Plan designed to export master data from a telephone system into compressed CSV files. It orchestrates two UNIX jobs, `DW.DWH_EXIS_SD_APT_NNA_DATA` and `DW.DWH_EXIS_SD_APT_NNA_VOIC`, which perform the actual data extraction and distribution. The overall process is triggered monthly by a UC4 Event, `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT`, subject to the successful completion of two prerequisite job plans: `DW.BERT_STAMMDATEN_JP` and `DW.ACCESSP_SIGMA_GPRS_MONATLICH_JP`. The migration aims to re-implement this functionality in Google Cloud Platform, specifically leveraging Airflow for orchestration and Dataproc/PySpark for data processing, with BigQuery as the target platform for any resulting data if needed. The scope includes migrating the scheduling logic, the data extraction processes, and the associated error handling.

## 2. Source Inventory
The job is composed of four UC4 XML configuration files, all categorized as `uc4` and processed by the `UC4/Automic` tool. All components are of `medium` complexity and fall into the `semi_auto` migration bucket, indicating that some manual intervention will be required beyond automated conversion.

| File Path                                                                                                                              | Technology    | Complexity Tier | Automation Bucket | Purpose                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
|:---------------------------------------------------------------------------------------------------------------------------------------|:--------------|:----------------|:------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `vobs/dw_source/isdwh/uc4_prod_exports/.../DW.DWH_APT_EXPORT_MONATLICH_JP.xml`                                                         | UC4/Automic   | medium          | semi_auto         | This UC4 Job Plan orchestrates the execution of other jobs to export data into CSV files. It includes synchronization objects and conditional post-processing.                                                                                                                                                                                                                                                                   |
| `vobs/dw_source/isdwh/uc4_prod_exports/.../DW.DWH_EXIS_SD_APT_NNA_DATA.xml`                                                            | UC4/Automic   | medium          | semi_auto         | This UC4 job defines a UNIX job that exports telephone system master data into a compressed CSV file and distributes it to a target system. The script content shows it invokes `$HOME/aktuell/exporter/is/bin/r_exis_v2` with configuration `h_exis_apt_nna_daten.var`.                                                                                                                                         |
| `vobs/dw_source/isdwh/uc4_prod_exports/.../DW.DWH_EXIS_SD_APT_NNA_VOIC.xml`                                                            | UC4/Automic   | medium          | semi_auto         | This UC4 JOBS_UNIX object defines a job for exporting telephone system master data. It executes a shell script to export data into a compressed CSV file and distributes it to a target system. The script content shows it invokes `$HOME/aktuell/exporter/is/bin/r_exis_v2` with configuration `h_exis_apt_nna_voice.var`.                                                                                                                                                                      |
| `vobs/dw_source/isdwh/uc4_prod_exports/.../DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT.xml`                                                  | UC4/Automic   | medium          | semi_auto         | This UC4 Event (Time) job defines a schedule and conditional logic to activate a monthly export job plan, `DW.DWH_APT_EXPORT_MONATLICH_JP`, based on the successful completion of two prerequisite job plans: `DW.BERT_STAMMDATEN_JP` and `DW.ACCESSP_SIGMA_GPRS_MONATLICH_JP`. This handles the scheduling and prerequisite checks for the main job plan. |

## 3. Target Architecture
The target architecture will consist of an Airflow DAG running on Google Cloud Composer for orchestration. The data export logic currently implemented in shell scripts and executed via `JOBS_UNIX` objects will be re-implemented as PySpark jobs running on Google Cloud Dataproc. The output compressed CSV files will be stored in Google Cloud Storage (GCS). Any data that needs to be loaded into a data warehouse for further processing will be ingested into Google BigQuery.

-   **Orchestration**: Airflow (on Google Cloud Composer)
-   **Data Processing**: PySpark (on Google Cloud Dataproc)
-   **Storage**: Google Cloud Storage (for exported CSVs)
-   **Data Warehousing (optional/downstream)**: Google BigQuery

## 4. Data Flow & Lineage

The current data flow in UC4 is:
1.  **Event Trigger**: `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT` is a time-based event that checks for the successful completion of `DW.BERT_STAMMDATEN_JP` and `DW.ACCESSP_SIGMA_GPRS_MONATLICH_JP`.
2.  **Job Plan Activation**: If prerequisites are met, the event activates the job plan `DW.DWH_APT_EXPORT_MONATLICH_JP`.
3.  **Data Export Jobs**: The job plan `DW.DWH_APT_EXPORT_MONATLICH_JP` then sequentially invokes:
    *   `DW.DWH_EXIS_SD_APT_NNA_DATA` (UNIX Job)
    *   `DW.DWH_EXIS_SD_APT_NNA_VOIC` (UNIX Job)
4.  **External Script Execution**: Both UNIX jobs execute an external shell script `r_exis_v2` with different configuration files (`h_exis_apt_nna_daten.var` and `h_exis_apt_nna_voice.var`).
5.  **Output**: These scripts are responsible for exporting data into compressed CSV files and distributing them.

**Target Data Flow (Airflow DAG):**

The migrated Airflow DAG (`dw_dwh_apt_export_monatlich_jp`) will mirror this flow:

-   **Airflow DAG Schedule**: The monthly schedule of the event `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT` will be translated into an Airflow `schedule` parameter. This will likely involve a cron-based schedule expression.
-   **Prerequisite Checks**: Airflow sensors or external task sensors will be implemented to check for the successful completion of the equivalent migrated jobs for `DW.BERT_STAMMDATEN_JP` and `DW.ACCESSP_SIGMA_GPRS_MONATLICH_JP` (assuming these are also migrated to Airflow DAGs).
-   **Dataproc Job Tasks**: Two `DataprocSubmitJobOperator` tasks will be created:
    *   `dw_dwh_exis_sd_apt_nna_data`: Executes a PySpark script (e.g., `dw_dwh_exis_sd_apt_nna_data.py`) on Dataproc.
    *   `dw_dwh_exis_sd_apt_nna_voic`: Executes another PySpark script (e.g., `dw_dwh_exis_sd_apt_nna_voic.py`) on Dataproc.
-   **Dependencies**: The tasks will be chained sequentially: `start >> sensor_prereq_1 >> sensor_prereq_2 >> dw_dwh_exis_sd_apt_nna_data >> dw_dwh_exis_sd_apt_nna_voic >> end`.
-   **Output**: The PySpark jobs will write compressed CSV files to a designated GCS bucket (e.g., `gs://your-bucket-name/exports/`).

## 5. Transformation Logic

The core transformation logic resides within the `r_exis_v2` shell script and its associated configuration files (`h_exis_apt_nna_daten.var`, `h_exis_apt_nna_voice.var`). Since these are external executables and configuration, they will require manual analysis and re-implementation.

**Proposed Transformation:**
The `r_exis_v2` script, with its `.var` configuration, likely performs data extraction (presumably from an Oracle database, given common legacy data warehouse patterns), some filtering/aggregation, and then formats the output into CSV. This logic will be re-implemented as PySpark applications:

1.  **`dw_dwh_exis_sd_apt_nna_data.py` (PySpark)**:
    *   Reads data from the source system (e.g., Oracle via a Dataproc connector or a BigQuery external table).
    *   Applies the transformation rules defined in `h_exis_apt_nna_daten.var` (manual extraction and conversion needed). This might involve SQL queries or PySpark DataFrame operations.
    *   Writes the transformed data to GCS as a compressed CSV file. The file naming convention `DWHM_APT_NNA_Daten_<yyyymmddhhmmss>.csv.gz` from the original documentation will be maintained.

2.  **`dw_dwh_exis_sd_apt_nna_voic.py` (PySpark)**:
    *   Similar to the above, reads data from the source system.
    *   Applies transformation rules defined in `h_exis_apt_nna_voice.var` (manual extraction and conversion needed).
    *   Writes the transformed data to GCS as a compressed CSV file, following a similar naming convention.

The variable `&MONAT_ID = SUBSTR(SYS_DATE('YYYYMMDD'),1,6)` which derives a YYYYMM (monthly) ID will be replaced by Airflow's templating (e.g., `{{ ds_nodash[:6] }}`) or Python's datetime operations to pass the correct month identifier to the PySpark jobs.

## 6. External Dependencies

The following external systems and components have been identified:

1.  **Oracle Database (presumed source)**: The nature of "master data export" jobs often implies extraction from a source database. While not explicitly mentioned in the UC4 XML, legacy `DWH` contexts often rely on Oracle.
    *   **Replacement Strategy**: If Oracle is the source, it will be accessed from Dataproc via a JDBC connector, or data will be ingested into BigQuery using Data Transfer Service or custom ingestion pipelines, and then queried from BigQuery.
2.  **`r_exis_v2` executable**: A UNIX binary/script responsible for the actual data extraction and file generation.
    *   **Replacement Strategy**: This script and its configuration files (`h_exis_apt_nna_daten.var`, `h_exis_apt_nna_voice.var`) must be reverse-engineered to understand the extraction and transformation logic. This logic will then be re-implemented in PySpark.
3.  **UNIX Host (`DWHDWH1P`)**: The UC4 `JOBS_UNIX` objects specify `HostDst>|DWHDWH1P|HOST`.
    *   **Replacement Strategy**: The PySpark jobs will execute on ephemeral Dataproc clusters. Any file system operations or local commands previously run on `DWHDWH1P` will be adapted to GCS for storage and PySpark for processing.
4.  **Pre-requisite UC4 Job Plans**: `DW.BERT_STAMMDATEN_JP` and `DW.ACCESSP_SIGMA_GPRS_MONATLICH_JP`.
    *   **Replacement Strategy**: It is assumed these will also be migrated to Airflow DAGs. The `DW.DWH_APT_EXPORT_MONATLICH_JP` DAG will include `ExternalTaskSensor` or `DagRunSensor` tasks to ensure these prerequisite DAGs have completed successfully before proceeding.

## 7. Unresolved / Risks

1.  **`r_exis_v2` and `.var` files analysis**: The exact data extraction queries, transformation rules, and output formatting implemented by `r_exis_v2` and its configuration files are currently unknown. This is a significant risk and requires detailed manual analysis and reverse-engineering of these legacy components. Without this, the PySpark re-implementation cannot be accurately developed.
2.  **Source Database Details**: The specific source database (e.g., Oracle schema, table names, connection details) from which `r_exis_v2` extracts data is not explicit in the provided XML. This information is crucial for developing the PySpark extraction logic.
3.  **File Distribution Mechanism**: The UC4 documentation mentions "distributes it to a target system." The mechanism and target for this distribution are unclear. If it's SFTP, S3, or another system, this needs to be identified and a corresponding GCP-native solution (e.g., Cloud Storage notifications, Cloud Functions, or an Airflow transfer operator) implemented.
4.  **Prerequisite Jobs Migration**: The successful migration and scheduling of `DW.BERT_STAMMDATEN_JP` and `DW.ACCESSP_SIGMA_GPRS_MONATLICH_JP` are critical for the correct functioning of this job. Delays or issues in those migrations will directly impact this job.
5.  **UC4 `ENDED_SKIPPED` Postcondition**: The `ENDED_SKIPPED` postcondition logic needs careful manual review to ensure its intent is correctly translated into Airflow's branching or sensor logic, especially considering the warning about `TriggerRule.ALL_DONE`.
6.  **`DW.CALL_STANDARD` and `BLOCK` actions**: The failure actions involving `DW.CALL_STANDARD` and `BLOCK` indicate a specific error handling or alerting mechanism in UC4. This needs to be understood and mapped to appropriate Airflow `on_failure_callback` mechanisms, potentially involving Cloud Logging/Monitoring and alerting services.

## 8. Build Plan

The build plan outlines the ordered steps and generated components for migrating `DW.DWH_APT_EXPORT_MONATLICH_JP` to Google Cloud Platform.

1.  **Analyze `r_exis_v2` and `.var` Configuration Files**:
    *   **Description**: Manually analyze the `r_exis_v2` binary/script and its `h_exis_apt_nna_daten.var` and `h_exis_apt_nna_voice.var` configuration files to fully understand the data sources, extraction queries, transformation logic, and output formatting.
    *   **Language**: Bash/Shell script analysis, Configuration file analysis.
    *   **Output**: Detailed specification of data sources, SQL queries/transformation rules, and output CSV schema.

2.  **Develop PySpark Data Export Scripts**:
    *   **Description**: Re-implement the extraction and transformation logic identified in step 1 into two separate PySpark applications.
    *   **Language**: Python (PySpark).
    *   **Output**:
        *   `dw_dwh_exis_sd_apt_nna_data.py` (PySpark script)
        *   `dw_dwh_exis_sd_apt_nna_voic.py` (PySpark script)
        *   Associated dependency files (e.g., `requirements.txt` for Dataproc).

3.  **Develop Airflow DAG**:
    *   **Description**: Create an Airflow DAG that orchestrates the PySpark jobs, incorporates scheduling, and manages prerequisites.
    *   **Language**: Python (Airflow).
    *   **Output**:
        *   `dags/dw_dwh_apt_export_monatlich_jp.py` (Airflow DAG file)
        *   Includes `DataprocSubmitJobOperator` for the PySpark scripts.
        *   Includes `ExternalTaskSensor` or `DagRunSensor` for `DW.BERT_STAMMDATEN_JP` and `DW.ACCESSP_SIGMA_GPRS_MONATLICH_JP`.
        *   Configured with appropriate `schedule_interval` (monthly) and `on_failure_callback`.

4.  **Configure GCP Resources**:
    *   **Description**: Set up necessary GCP infrastructure.
    *   **Language**: Terraform (recommended) or gcloud CLI.
    *   **Output**:
        *   Cloud Composer Environment.
        *   Dataproc cluster configurations (or cluster templates for ephemeral clusters).
        *   GCS buckets for PySpark scripts and exported CSV data.
        *   IAM roles and permissions for Airflow service accounts and Dataproc.

5.  **Deployment and Testing**:
    *   **Description**: Deploy the Airflow DAG and PySpark scripts to the Cloud Composer environment and GCS, respectively. Thoroughly test the end-to-end workflow, including scheduling, prerequisite checks, data extraction, transformation, and error handling.
    *   **Language**: Airflow UI, `gcloud` CLI, PySpark testing frameworks.
    *   **Output**: Successfully executing and validated Airflow DAG.