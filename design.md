# Migration Design — DW.DWH_APT_EXPORT_MONATLICH_JP

## 1. Purpose & Scope
The UC4 job `DW.DWH_APT_EXPORT_MONATLICH_JP` orchestrates a monthly data export process. Its primary function is to generate compressed CSV files containing telephone system master data, which are then distributed to a target system. The execution is event-driven and contingent upon the successful completion of specific prerequisite jobs. The scope of this migration is to re-implement this monthly export process on Google Cloud Platform, targeting BigQuery for data processing and a cloud-native orchestrator (e.g., Cloud Composer) for workflow management, while preserving the original functionality, scheduling, and data delivery mechanism.

## 2. Source Inventory
The legacy job consists of the following UC4 components:

| File Name                                                                                                                              | Technology   | Tier       | Automation Bucket | Summary                                                                                                                                                                                                                               |
| :------------------------------------------------------------------------------------------------------------------------------------- | :----------- | :--------- | :---------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_MONATLICH_JP/DW.BERT_LOG.xml`                      | UC4 JOBS_UNIX | `No rows.` | semi_auto         | This UC4 JOBS_UNIX object defines a job named DW.BERT_LOG which executes a shell script. It includes other UC4 scripts and sets a job-specific variable before running a command.                                                  |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_MONATLICH_JP/DW.BERT_MONATLICH_JP.xml`              | UC4 JOBP     | `No rows.` | semi_auto         | This UC4 Job Plan (JOBP) orchestrates the monthly execution of BERT-related jobs, specifically DW.BERT_RECHNUNGSDATEN and DW.BERT_LOG, with conditional branching and synchronization.                                               |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_MONATLICH_JP/DW.BERT_RECHNUNGSDATEN.xml`          | UC4 JOBS_UNIX | `No rows.` | semi_auto         | UC4 job definition for a UNIX job named DW.BERT_RECHNUNGSDATEN, which orchestrates the execution of the r_aurd_rechstan.ksh shell script.                                                                                              |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_MONATLICH_JP/DW.DWH_APT_EXPORT_MONATLICH_JP.xml` | UC4 JOBP     | `No rows.` | semi_auto         | This UC4 Job Plan orchestrates the execution of other jobs to export data into CSV files. It includes synchronization objects and conditional post-processing.                                                                    |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_MONATLICH_JP/DW.DWH_EXIS_SD_APT_NNA_DATA.xml` | UC4 JOBS_UNIX | `No rows.` | semi_auto         | This UC4 job defines a UNIX job that exports telephone system master data into a compressed CSV file and distributes it to a target system.                                                                                       |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_MONATLICH_JP/DW.DWH_EXIS_SD_APT_NNA_VOIC.xml` | UC4 JOBS_UNIX | `No rows.` | semi_auto         | This UC4 JOBS_UNIX object defines a job for exporting telephone system master data. It executes a shell script to export data into a compressed CSV file and distributes it to a target system.                                |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_MONATLICH_JP/DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT.xml` | UC4 EVNT_TIME | `No rows.` | semi_auto         | This UC4 Event (Time) job defines a schedule and conditional logic to activate a monthly export job plan, DW.DWH_APT_EXPORT_MONATLICH_JP, based on the successful completion of two prerequisite job plans. |

## 3. Target Architecture
The target architecture on Google Cloud Platform will replace the legacy UC4 components with cloud-native services:

-   **Orchestration Layer:** Cloud Composer (managed Airflow) will manage the overall workflow, replacing the UC4 event and job plan. This includes scheduling, dependency management, and error handling.
-   **Data Preparation Layer:** BigQuery SQL will be used to process and prepare the data for export. This involves reading from existing BigQuery datasets, applying filtering (e.g., by month), and shaping the data into the required CSV format.
-   **Export Layer:** BigQuery Extract jobs will export the prepared data directly to Cloud Storage in CSV format, compressed with GZIP.
-   **Distribution Layer:** Cloud Storage will serve as the landing zone for the exported files. Depending on the target system's requirements, further distribution can be handled via Cloud Storage mechanisms (e.g., signed URLs, object lifecycle management) or potentially a managed SFTP service.
-   **Observability Layer:** Cloud Logging and Cloud Monitoring will be used for logging, monitoring, and alerting. Custom BigQuery audit tables will capture execution metadata.

## 4. Data Flow & Lineage
The data flow in the migrated solution will be as follows:

1.  A Cloud Composer DAG, scheduled monthly, initiates the export process.
2.  The DAG first checks the status of prerequisite jobs (`DW.BERT_STAMMDATEN_JP` and `DW.ACCESSP_SIGMA_GPRS_MONATLICH_JP`). This will require either migrating these prerequisite jobs to GCP or establishing a mechanism to query their status from the legacy UC4 system if they remain there.
3.  Upon successful prerequisite validation, the DAG triggers two parallel BigQuery processes:
    a.  One to prepare data for `DW.DWH_EXIS_SD_APT_NNA_DATA`.
    b.  Another to prepare data for `DW.DWH_EXIS_SD_APT_NNA_VOIC`.
4.  Each BigQuery process executes SQL queries to extract and format the data.
5.  BigQuery extract jobs then export the results of these queries to a Cloud Storage bucket, generating `.csv.gz` files with names like `DWHM_APT_NNA_Daten_<yyyymmddhhmmss>.csv.gz`.
6.  Files in Cloud Storage are then made available for the target system, potentially via Cloud Storage bucket access or further integration.
7.  Throughout the process, execution logs and metadata are captured in Cloud Logging and BigQuery audit tables.

## 5. Transformation Logic
The core transformation logic originates from the shell scripts executed by the `DW.DWH_EXIS_SD_APT_NNA_DATA` and `DW.DWH_EXIS_SD_APT_NNA_VOIC` UNIX jobs. These scripts utilize an external binary `r_exis_v2` and configuration files (`.var` files) to perform the data export.

In the BigQuery environment:
-   **Month Derivation:** The `YYYYMM` month identifier, previously derived from `SYS_DATE` in UC4, will be derived using BigQuery SQL functions (e.g., `FORMAT_DATE('%Y%m', CURRENT_DATE())`).
-   **Data Extraction & Formatting:** The logic encapsulated within the `r_exis_v2` binary and `.var` configuration files will need to be reverse-engineered and re-implemented using BigQuery SQL. This will involve selecting, filtering, joining, and formatting columns from source tables to match the exact structure and content of the original CSV outputs.
-   **Compression:** BigQuery extract operations inherently support GZIP compression, so this will be handled automatically during the export to Cloud Storage.

## 6. External Dependencies
**Legacy External Dependencies:**
-   **UC4 Scheduler:** For job orchestration and scheduling.
-   **UNIX Host (`DWHDWH1P`):** Where shell scripts and the `r_exis_v2` binary execute.
-   **UC4 Prerequisite Job Plans (`DW.BERT_STAMMDATEN_JP`, `DW.ACCESSP_SIGMA_GPRS_MONATLICH_JP`):** Their successful completion is a trigger condition.
-   **`r_exis_v2` binary:** The proprietary executable for data export.
-   **`.var` configuration files:** External files defining export parameters and possibly data mappings.
-   **Common UC4 includes (`DW.HOLE_PFAD`, `DW.LESE_LOG`):** For environment setup and logging.
-   **`DW.CALL_STANDARD`:** A UC4 object for standard failure handling.

**Replacement in Target Architecture:**
-   **UC4 Scheduler:** Replaced by Cloud Composer (managed Airflow) for scheduling and orchestration.
-   **UNIX Host / `r_exis_v2` / `.var` files:** Replaced by BigQuery SQL for data transformation and BigQuery Extract for data export. The logic within `r_exis_v2` and `.var` files must be fully translated to BigQuery SQL.
-   **UC4 Prerequisite Job Plans:** These will need to be either migrated to Cloud Composer and integrated into the new DAG structure, or if they remain in UC4, their status will need to be queried via API or a bridge mechanism.
-   **Common UC4 includes (`DW.HOLE_PFAD`, `DW.LESE_LOG`):** Replaced by standard Airflow practices for environment setup and Cloud Logging for unified logging.
-   **`DW.CALL_STANDARD`:** Replaced by Airflow's native error handling, alerting (e.g., PagerDuty, email), and Cloud Monitoring alerts.

## 7. Unresolved / Risks
-   **`r_exis_v2` Logic:** The exact data extraction and transformation logic within the `r_exis_v2` binary and its `.var` configuration files is not directly exposed in the UC4 XML. This logic needs to be fully understood, documented, and translated into BigQuery SQL. This is a critical dependency and a potential risk if the logic is complex or poorly documented.
-   **Prerequisite Job Migration:** The `DW.BERT_STAMMDATEN_JP` and `DW.ACCESSP_SIGMA_GPRS_MONATLICH_JP` job plans are external prerequisites. The strategy for their migration or integration with the new Cloud Composer DAG needs to be defined. If they are not migrated, a reliable mechanism to check their status from GCP is required.
-   **Target System Integration:** The method for the target system to retrieve the exported `.csv.gz` files from Cloud Storage needs to be confirmed and implemented (e.g., push notification, pull from SFTP gateway, direct Cloud Storage access).
-   **Sensitive Information in `.var` files:** If the `.var` files contain sensitive information (e.g., database credentials, API keys), these must be securely migrated to Google Secret Manager.

## 8. Build Plan
### Phase 1: Discovery & Reverse Engineering (Weeks 1-2)
-   **Objective:** Understand the exact functionality of `r_exis_v2` and the `.var` configuration files.
-   **Tasks:**
    -   Interview subject matter experts for insights into `r_exis_v2` behavior and data sources.
    -   Analyze sample inputs and outputs of `r_exis_v2` to infer transformation logic.
    -   Document the full schema of expected CSV outputs for both `DATA` and `VOIC` exports.
    -   Identify the source tables in the legacy data warehouse that `r_exis_v2` reads from.
    -   Clarify the status checking mechanism for `DW.BERT_STAMMDATEN_JP` and `DW.ACCESSP_SIGMA_GPRS_MONATLICH_JP`.

### Phase 2: BigQuery Data Modeling & SQL Development (Weeks 3-5)
-   **Objective:** Recreate data preparation logic in BigQuery.
-   **Tasks:**
    -   Design and create necessary BigQuery views or tables for staging and final export data.
    -   Develop BigQuery SQL scripts that replicate the data extraction and transformation logic for both `DW.DWH_EXIS_SD_APT_NNA_DATA` and `DW.DWH_EXIS_SD_APT_NNA_VOIC` exports.
    -   Implement parameterization for the monthly identifier.
    -   Develop unit tests for BigQuery SQL scripts to ensure data accuracy.

### Phase 3: Cloud Composer DAG Development (Weeks 6-8)
-   **Objective:** Replicate UC4 orchestration in Airflow.
-   **Tasks:**
    -   Develop an Airflow DAG (`dw_dwh_apt_export_monatlich_jp_dag.py`) for the monthly schedule.
    -   Implement prerequisite checks using Airflow sensors or external system operators (if prerequisites remain in UC4).
    -   Integrate BigQuery operators to execute the SQL scripts developed in Phase 2.
    -   Configure BigQuery Extract Operators to export data to Cloud Storage as compressed CSVs.
    -   Implement error handling, retries, and failure notifications in the DAG.
    -   Integrate logging to Cloud Logging.

### Phase 4: Export and Delivery Mechanism (Weeks 9-10)
-   **Objective:** Ensure secure and reliable delivery of exported files.
-   **Tasks:**
    -   Configure the Cloud Storage bucket with appropriate permissions and lifecycle policies.
    -   Implement the chosen distribution mechanism for the target system (e.g., Cloud Storage access for pull, Cloud Function for push, SFTP transfer service).
    -   Test end-to-end file delivery and integrity.

### Phase 5: Monitoring, Alerting & Audit (Weeks 11-12)
-   **Objective:** Establish comprehensive operational visibility.
-   **Tasks:**
    -   Create BigQuery audit tables to record job run details, file paths, row counts, and statuses.
    -   Develop Cloud Monitoring dashboards for DAG health and export metrics.
    -   Set up Cloud Monitoring alerts for critical failures or delays.

### Phase 6: Testing & Cutover (Weeks 13-14)
-   **Objective:** Validate the migrated job and transition to production.
-   **Tasks:**
    -   Execute parallel runs of the legacy and migrated jobs for at least one full monthly cycle.
    -   Perform data validation to compare outputs from both systems.
    -   Obtain business sign-off.
    -   Decommission the legacy UC4 job.