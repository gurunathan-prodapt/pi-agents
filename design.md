# Migration Design — EXIS_SD_APT_NNA_DATA

## 1. Purpose & Scope

This migration design document outlines the conversion of the legacy UC4 job `DW.DWH_EXIS_SD_APT_NNA_DATA` to run on Google Cloud Platform, leveraging Airflow for orchestration and Dataproc for execution of PySpark code.

The original UC4 job is responsible for exporting telephone system master data into a compressed CSV file (`DWHM_APT_NNA_Daten_<yyyymmddhhmmss>.csv.gz`) and distributing it to a target system. The job identifies itself with `EXIS_SD_APT_NNA_DATA`, initializes its environment, determines a `YYYYMM` month identifier, and then executes a custom export script named `r_exis_v2` with specific configuration and parameters.

The scope of this document covers the conversion of the `DW.DWH_EXIS_SD_APT_NNA_DATA` UC4 job definition into an Airflow DAG and its associated PySpark component.

## 2. Source Inventory

| File Path | Technology | Category | Complexity Tier | Automation Bucket | Summary |
| :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :--------- | :------- | :---------------- | :---------------- | :---------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_MONATLICH_JP/DW.DWH_EXIS_SD_APT_NNA_DATA.xml` | UC4/Automic | uc4 | Unspecified (likely simple/medium) | semi_auto | This UC4 job defines a UNIX job that exports telephone system master data into a compressed CSV file and distributes it to a target system. |

## 3. Target Architecture

The migrated solution will consist of:
*   An **Airflow DAG** (`dw_dwh_exis_sd_apt_nna_data`) responsible for scheduling and orchestrating the data export process.
*   A **Dataproc cluster** to execute a PySpark script.
*   A **PySpark script** (`r_exis_v2.py`) deployed to **Google Cloud Storage (GCS)**, which will perform the data extraction and transformation logic, replacing the original `r_exis_v2` shell script.
*   The output data (`DWHM_APT_NNA_Daten_<yyyymmddhhmmss>.csv.gz`) will be written to **Google Cloud Storage**. Further distribution mechanisms will need to be defined based on the "target system" mentioned in the UC4 job's documentation.

## 4. Data Flow & Lineage

**Legacy Flow:**
1.  The UC4 job `DW.DWH_EXIS_SD_APT_NNA_DATA` is triggered (schedule unknown due to missing `EVNT_TIME` file).
2.  It executes a UNIX shell script on host `DWHDWH1P`.
3.  The script initializes its environment (`. $HOME/.dw_init`).
4.  It sets a job identifier (`DWH_JOB_KENNUNG='EXIS_SD_APT_NNA_DATA'`).
5.  It calculates a `MONAT_ID` (e.g., `YYYYMM`) from the current system date.
6.  It invokes the `r_exis_v2` executable with a configuration file (`$HOME/aktuell/exporter/apt/cfg/h_exis_apt_nna_daten.var`) and the derived `MONAT_ID` as parameters.
7.  The `r_exis_v2` script exports telephone system master data into a compressed CSV file (`DWHM_APT_NNA_Daten_*.csv.gz`) and then distributes it.
8.  A logging script (`DW.LESE_LOG`) is executed.

**Target Flow (Airflow on GCP):**
1.  An Airflow DAG (`dw_dwh_exis_sd_apt_nna_data`) is scheduled (schedule to be determined).
2.  The DAG starts.
3.  A `DataprocSubmitJobOperator` task named `dwh_exis_sd_apt_nna_data` is executed.
4.  This task submits a PySpark job (`r_exis_v2.py`) to a Dataproc cluster.
5.  The PySpark script will:
    *   Replicate the environment initialization and variable settings.
    *   Derive the `MONAT_ID` from the Airflow execution date.
    *   Read the configuration equivalent to `h_exis_apt_nna_daten.var` from GCS.
    *   Perform the data extraction and transformation logic of `r_exis_v2` (to be re-implemented in PySpark).
    *   Write the output data (compressed CSV) to a specified GCS bucket.
    *   Trigger any necessary downstream processes for data distribution (to be defined).
6.  The DAG ends upon successful completion of the Dataproc job.

**Lineage:**
Source data (external telephone system master data) → `r_exis_v2` (PySpark on Dataproc) → GCS (CSV/GZ file) → Target System (further distribution TBD).

## 5. Transformation Logic

The core transformation logic resides within the `r_exis_v2` script, which is invoked by the UC4 job. This script will need to be reverse-engineered and re-implemented in PySpark.

**Key elements to re-implement in PySpark:**
*   **Environment Initialization:** Replace shell script environment setup (`. $HOME/.dw_init`) with Python/PySpark equivalents for configuration and variable management.
*   **Job Identifier:** The `DWH_JOB_KENNUNG='EXIS_SD_APT_NNA_DATA'` can be passed as a parameter or configuration to the PySpark script.
*   **Month ID Derivation:** The `MONAT_ID` (`YYYYMM`) derived from `SYS_DATE('YYYYMMDD')` will be calculated from the Airflow execution date in the PySpark script.
*   **Configuration File Handling:** The `$HOME/aktuell/exporter/apt/cfg/h_exis_apt_nna_daten.var` file will need to be accessible from Dataproc, likely by storing it in GCS and providing its path as a parameter.
*   **Data Export Logic:** The main logic of `r_exis_v2` for extracting telephone system master data, compressing it, and formatting it as CSV will be the primary focus of the PySpark implementation. This will likely involve reading from a source system (e.g., a database, an API) and writing to GCS.
*   **Logging:** The `DW.LESE_LOG` component suggests logging capabilities, which should be integrated into the PySpark script using standard logging practices.

## 6. External Dependencies

**Legacy External Systems:**
*   **UNIX Host (`DWHDWH1P`):** The job executes on this host.
*   **Source Data System:** An implicit external system providing "telephone system master data."
*   **Target System for Distribution:** An implicit external system to which the exported data is distributed.

**Migration Strategy for External Dependencies:**
*   **UNIX Host:** Replaced by the Dataproc cluster environment.
*   **Source Data System:** Identify the original source of "telephone system master data" and establish a connection from Dataproc. This could involve direct database connections (e.g., via JDBC), API calls, or reading from a staging area in GCS.
*   **Target System for Distribution:** The `csv.gz` output will initially be staged in GCS. The mechanism for "distributing it to the target system" needs to be explicitly identified. This might involve:
    *   Further Airflow tasks to push data to another system (e.g., SFTP, another cloud storage, a different database).
    *   GCS notifications triggering cloud functions or other services.
    *   Direct ingestion by the target system from GCS.

## 7. Unresolved / Risks

*   **Missing Workflow Context (High Risk):** The most significant unresolved item is the lack of UC4 `EVNT_TIME` and `JOBP` files. This means the original job's scheduling frequency and its position within a larger UC4 workflow are unknown. A manual investigation is required to determine:
    *   The exact schedule (e.g., daily, monthly, specific dates).
    *   Any upstream dependencies that trigger this job.
    *   Any downstream jobs that depend on the output of this job.
    *   The purpose and logic of `DW.HOLE_PFAD` and `DW.LESE_LOG` beyond their names.
*   **GCP Placeholders:** The Airflow DAG design contains placeholders for `YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_REGION`, `YOUR_DATAPROC_CLUSTER_NAME`, and `YOUR_BUCKET_NAME`. These need to be configured for the target GCP environment.
*   **`r_exis_v2` Internal Logic (Medium Risk):** The internal logic of the `r_exis_v2` executable is unknown and needs to be reverse-engineered or re-written based on documentation or collaboration with SMEs.
*   **External System Integration (Medium Risk):** The exact source of "telephone system master data" and the "target system" for distribution need to be clearly identified and their integration points designed for GCP.
*   **Retry Policy (Low Risk):** The UC4 job's restartability hints at a retry mechanism, but no explicit retry count or delay was found. The Airflow DAG is initially set to `retries=0`. This might need adjustment based on business requirements.

## 8. Build Plan

The build plan will involve creating the necessary Airflow DAG and PySpark script, along with setting up the required GCP infrastructure.

1.  **Define Airflow DAG Skeleton (`dw_dwh_exis_sd_apt_nna_data.py`):**
    *   Create a Python file for the Airflow DAG based on the pseudocode generated by the MCP tool.
    *   Populate `dag_id`, `default_args` (including a placeholder `start_date`), `catchup=False`, `max_active_runs=1`, `is_paused_upon_creation=False`.
    *   Implement a `DataprocSubmitJobOperator` task named `dwh_exis_sd_apt_nna_data`.
    *   Configure the `DataprocSubmitJobOperator` with `project_id`, `region`, `cluster_name` (using environment variables or Airflow connections/variables), and the `main_python_file_uri` pointing to the PySpark script in GCS.
    *   Define task dependencies: `start >> dwh_exis_sd_apt_nna_data`.

2.  **Develop PySpark Script (`r_exis_v2.py`):**
    *   Translate the logic of the legacy `r_exis_v2` shell script into a PySpark application.
    *   Implement logic to read the configuration equivalent to `h_exis_apt_nna_daten.var`.
    *   Implement logic to derive the `YYYYMM` month ID from the PySpark job's execution context.
    *   Implement the data extraction, transformation, and CSV/GZ compression logic.
    *   Implement logic to write the output to a specified GCS path.
    *   Integrate appropriate logging.

3.  **GCP Infrastructure Setup:**
    *   Create a dedicated GCS bucket for PySpark scripts and output data.
    *   Provision a Dataproc cluster (or configure a serverless Dataproc environment).
    *   Set up necessary IAM roles and permissions for Airflow, Dataproc, and GCS.

4.  **Configuration Migration:**
    *   Migrate the contents of `h_exis_apt_nna_daten.var` to a suitable configuration format (e.g., JSON, YAML) and store it in GCS.

5.  **Determine Schedule and Dependencies:**
    *   **Manual investigation required:** Work with business users or SMEs to ascertain the original scheduling frequency and any upstream/downstream dependencies of the UC4 job.
    *   Update the Airflow DAG's `schedule` and add any necessary `ExternalTaskSensor` or other operators to manage dependencies.

6.  **Testing:**
    *   Develop unit and integration tests for the PySpark script.
    *   Test the Airflow DAG end-to-end in a development environment.

7.  **Deployment:**
    *   Deploy the PySpark script to GCS.
    *   Deploy the Airflow DAG to the Airflow environment.