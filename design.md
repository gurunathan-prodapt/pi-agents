# Migration Design — EXIS_SD_APT_BESTANDS

## 1. Purpose & Scope
This document outlines the migration design for the `EXIS_SD_APT_BESTANDS` job, currently defined as a UC4 JOBS_UNIX object. The primary business purpose of this job is to export stock data daily from several source tables into a gzipped CSV file, `DWHM_APT_BESTANDSREPORT_<yyyymmddhhmmss>.csv.gz`, and distribute it to a downstream target system. The migration aims to re-platform this workflow from its legacy UC4 scheduler and Unix-based execution environment to Google Cloud Platform, specifically utilizing BigQuery for data storage and Airflow for workflow orchestration.

The scope of this migration covers the conversion of the UC4 job definition into an Airflow DAG and the re-implementation or wrapping of the core data extraction and export logic for execution within a Google Cloud environment (e.g., Dataproc for PySpark).

## 2. Source Inventory
The job consists of a single source file, a UC4 JOBS_UNIX XML definition, which orchestrates a shell script.

*   **File**: `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_TAEGLICH_JP/DW.DWH_EXIS_SD_APT_BESTANDS.xml`
    *   **Technology**: UC4 JOBS_UNIX (XML definition) executing a shell script.
    *   **Complexity Tier**: `medium`
    *   **Automation Bucket**: `semi_auto`
    *   **Summary**: This UC4 job orchestrates the daily export of stock data. The embedded shell script executes a custom binary (`r_exis_v2`) with a configuration file to perform the data extraction and file generation.

## 3. Target Architecture
The target architecture will leverage Google Cloud Platform services to replace the existing UC4 and Unix-based system.

*   **Orchestration**: Apache Airflow on Google Cloud Composer will manage the scheduling and execution of the job.
*   **Data Processing**: Google Cloud Dataproc will be used to execute the data extraction and transformation logic, likely via PySpark jobs.
*   **Data Landing/Staging**: Google Cloud Storage (GCS) will serve as the landing zone for the exported CSV files.
*   **Data Warehousing**: BigQuery will be the ultimate target for storing the migrated data. For this specific job, BigQuery will not be the direct target but rather the implicit destination if the exported CSV is later loaded into BigQuery. The immediate output is a CSV file.

## 4. Data Flow & Lineage
The current data flow starts with the UC4 scheduler triggering the `DW.DWH_EXIS_SD_APT_BESTANDS` job. This job, in turn, executes a Unix shell script. The shell script's core action is to run `r_exis_v2`, which reads data from specific Oracle tables and writes a gzipped CSV file. The file is then distributed to a target system.

**Legacy Flow:**
1.  UC4 Scheduler triggers `DW.DWH_EXIS_SD_APT_BESTANDS`.
2.  `DW.DWH_EXIS_SD_APT_BESTANDS` (UC4 JOBS_UNIX) executes a shell script on a Unix host.
3.  The shell script sources initialization files and executes `$HOME/aktuell/exporter/is/bin/r_exis_v2` with configuration `$HOME/aktuell/exporter/apt/cfg/h_exis_apt_bestandsdaten.var`.
4.  `r_exis_v2` reads data from Oracle tables: `SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG`, `RPT$TA_S_D1_VERTRAG`.
5.  `r_exis_v2` writes the extracted stock data to `DWHM_APT_BESTANDSREPORT_<yyyymmddhhmmss>.csv.gz`.
6.  The gzipped CSV file is distributed to a downstream target system.

**Target Airflow DAG Structure:**
The migrated Airflow DAG will be named `dw_dwh_exis_sd_apt_bestands`.
*   **`start` node**: The DAG initiation.
*   **`run_dwh_exis_sd_apt_bestands` task**: A `DataprocSubmitJobOperator` that will execute a PySpark script, likely `r_exis_v2.py`, replicating the logic of the original `r_exis_v2` binary.
*   **`end` node**: The DAG completion.

The task dependency will be `start >> run_dwh_exis_sd_apt_bestands >> end`.

## 5. Transformation Logic
The core transformation logic resides within the `r_exis_v2` executable. This executable, invoked by the UC4 job's shell script, is responsible for:
1.  Connecting to the source database.
2.  Querying data from `SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG`, and `RPT$TA_S_D1_VERTRAG`. The specific joins, filters, and aggregations performed are currently encapsulated within `r_exis_v2` and its configuration.
3.  Formatting the extracted data into a CSV format.
4.  Compressing the CSV into a `.gz` file.
5.  Writing the file to a specified output location.

For migration, this logic will need to be re-implemented. The suggested approach is to:
*   **Re-implement `r_exis_v2`**: This custom binary's functionality will need to be re-engineered, preferably in PySpark, to run on Dataproc. This PySpark script (`r_exis_v2.py`) will connect to the source data (potentially via federated queries if directly from Oracle, or after an initial data lake ingestion), perform the necessary data transformations, and write the output CSV to GCS.
*   **Configuration File Translation**: The configuration file `h_exis_apt_bestandsdaten.var` will need to be analyzed and its parameters translated into PySpark job arguments or configurations.
*   **UC4 Includes**: UC4 include files like `DW.HOLE_PFAD` and `DW.LESE_LOG` contain environment setups and logging routines. These will be replaced by Airflow's environment configuration and native logging mechanisms.

## 6. External Dependencies
The current job has several external dependencies:

*   **Source Database**: An Oracle database containing tables `SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG`, and `RPT$TA_S_D1_VERTRAG`.
    *   **Replacement Strategy**: Data from these Oracle tables should be ingested into BigQuery or a GCS data lake as part of an upstream migration process (e.g., using DataStream, Fivetran, or custom ingestion pipelines). The PySpark job (`r_exis_v2.py`) will then read from these BigQuery tables or GCS files.
*   **Target System for Distributed File**: The gzipped CSV file is distributed to an unspecified "target system".
    *   **Replacement Strategy**: If this target system is also migrated to GCP, GCS can serve as the distribution point, with downstream systems consuming from GCS. If the target system remains external, secure data transfer mechanisms like Cloud Storage Transfer Service or custom API integrations will be needed.
*   **`$HOME/.dw_init`**: A Unix initialization script.
    *   **Replacement Strategy**: Environment variables and configurations typically set by such scripts will be managed by Airflow (e.g., `Variable`, `Connection` objects) or within the Dataproc job definition.
*   **`r_exis_v2` binary**: The custom executable.
    *   **Replacement Strategy**: Re-implementation in PySpark, executed on Dataproc.
*   **`h_exis_apt_bestandsdaten.var` config file**: Configuration for `r_exis_v2`.
    *   **Replacement Strategy**: Parameters will be passed to the PySpark job directly or stored in GCS and read by the PySpark job.

## 7. Unresolved / Risks
*   **Missing Workflow Details**: The absence of `EVNT_TIME` (scheduler object) and other workflow objects (like `JOBP` or `JSCH`) in the provided UC4 XML means the exact schedule and upstream/downstream dependencies of `EXIS_SD_APT_BESTANDS` within the broader UC4 ecosystem are not fully captured. This may lead to gaps in the Airflow DAG's scheduling and dependency definition.
*   **`r_exis_v2` Logic Obscurity**: The detailed logic of the `r_exis_v2` binary is opaque. Reverse-engineering or obtaining documentation for this executable is critical for accurate PySpark re-implementation. This is a significant risk, as any misinterpretation could lead to data discrepancies.
*   **Target System for File Distribution**: The "target system" for the exported CSV is not specified. Understanding this target's requirements and migration status is crucial for defining the final data distribution mechanism on GCP.
*   **UC4 Variable Resolution**: The UC4 script uses `:inc` and `:set &DWH_JOB_KENNUNG`. The exact values and scope of these variables need to be understood for proper translation.
*   **Error Handling and Retry Policy**: While the UC4 documentation indicates restartability, explicit retry counts and delays are not present. A default retry policy will be applied in Airflow, but this might need fine-tuning based on operational requirements.

## 8. Build Plan
The build plan focuses on generating the Airflow DAG and the associated data processing logic.

1.  **Airflow DAG Generation (Python)**
    *   **File**: `dags/dw_dwh_exis_sd_apt_bestands.py`
    *   **Content**: An Airflow DAG definition including:
        *   `dag_id`: `dw_dwh_exis_sd_apt_bestands`
        *   `schedule`: To be determined based on the actual UC4 schedule. (Currently `None` / placeholder).
        *   `start_date`: Placeholder `{{ placeholder_start_date }}`.
        *   A `DataprocSubmitJobOperator` task named `run_dwh_exis_sd_apt_bestands`.
        *   Placeholders for GCP project ID, Dataproc region, cluster name, and GCS bucket.
2.  **PySpark Script Development (Python)**
    *   **File**: `pyspark_scripts/r_exis_v2.py`
    *   **Content**: PySpark code that replicates the functionality of the original `r_exis_v2` binary. This script will:
        *   Connect to BigQuery (or a GCS data lake) to read data from `SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG`, and `RPT$TA_S_D1_VERTRAG`.
        *   Perform any necessary joins, filters, and transformations.
        *   Write the resulting dataset as a gzipped CSV to a specified GCS location.
        *   Incorporate logic to handle the translated parameters from `h_exis_apt_bestandsdaten.var`.
3.  **GCP Resources Provisioning**
    *   Provision a Dataproc cluster (or use an existing one).
    *   Create a GCS bucket for output CSV files and PySpark scripts.
    *   Ensure appropriate IAM roles and permissions are configured for Airflow and Dataproc.
4.  **Configuration Management**
    *   Translate `h_exis_apt_bestandsdaten.var` into Airflow variables or Dataproc job parameters.
    *   Map `DW.HOLE_PFAD` and other UC4 environment settings to Airflow connections/variables or Dataproc environment.
5.  **Deployment**
    *   Deploy the Airflow DAG to Cloud Composer.
    *   Upload `r_exis_v2.py` and any necessary configuration files to GCS.