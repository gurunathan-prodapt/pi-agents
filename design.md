# Migration Design — EXIS

## 1. Purpose & Scope

The EXIS job is a data export process originating from an Oracle Data Warehouse environment, orchestrated by UC4 (Automic Workload Automation), and leveraging KornShell scripts and Oracle PL/SQL for data extraction, transformation, and distribution. Its primary purpose is to extract various sets of master data (telephone system data, stock data, discount data) into gzipped CSV files and distribute them to external target systems via SFTP.

The scope of this migration is to re-platform the entire EXIS job to Google Cloud Platform (GCP), specifically targeting:
*   **Orchestration:** Apache Airflow
*   **Data Processing:** Google BigQuery for SQL transformations, and Python for orchestration and file handling.
*   **File Storage:** Google Cloud Storage (GCS)
*   **External Data Transfer:** Cloud Storage Transfer Service or custom Python for SFTP.

## 2. Source Inventory

The EXIS job comprises the following files:

| File Name                                                                     | Category | Tool                 | Tier    | Automation Bucket | Description                                                                                                                                                                                                                                                                                                                                                                                                  |
| :---------------------------------------------------------------------------- | :------- | :------------------- | :------ | :---------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isdwh/exporter/apt/cfg/h_exis_apt_nna_daten.var`               | config   | EXIS                 | simple  | semi_auto         | Configuration for `EXIS_SD_APT_NNA_DATA` job, defining parameters, SQL includes, post-processing (nawk, gzip), and SFTP distribution details.                                                                                                                                                                                                                                                            |
| `vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2`                              | shell    | KornShell            | medium  | redesign          | Unified KornShell framework for data export from Oracle to files, supporting various configurations, partitioning, parallel processing, and distribution methods. It acts as the core orchestrator.                                                                                                                                                                                                      |
| `vobs/dw_source/isdwh/uc4_prod_exports/.../DW.DWH_EXIS_SD_APT_NNA_DATA.xml`   | uc4      | UC4/Automic          | simple  | semi_auto         | UC4 job definition for exporting telephone system master data (`DW.DWH_EXIS_SD_APT_NNA_DATA`), executing `r_exis_v2` with `h_exis_apt_nna_daten.var`.                                                                                                                                                                                                                                        |
| `vobs/dw_source/isdwh/uc4_prod_exports/.../DW.DWH_EXIS_SD_APT_NNA_VOIC.xml`   | uc4      | UC4/Automic          | simple  | semi_auto         | UC4 job definition for exporting telephone system voice data (`DW.DWH_EXIS_SD_APT_NNA_VOIC`), executing `r_exis_v2` with `h_exis_apt_nna_voice.var`.                                                                                                                                                                                                                                           |
| `vobs/dw_source/isdwh/uc4_prod_exports/.../DW.DWH_EXIS_SD_APT_BESTANDS.xml`  | uc4      | UC4/Automic          | simple  | semi_auto         | UC4 job definition for exporting stock data (`DW.DWH_EXIS_SD_APT_BESTANDS`), executing `r_exis_v2` with `h_exis_apt_bestandsdaten.var`.                                                                                                                                                                                                                                                                |
| `vobs/dw_source/isdwh/uc4_prod_exports/.../DW.DWH_EXIS_SD_APT_RABATT.xml`    | uc4      | UC4/Automic          | simple  | semi_auto         | UC4 job definition for exporting discount data (`DW.DWH_EXIS_SD_APT_RABATT`), executing `r_exis_v2` with `h_exis_apt_rabattdaten.var`.                                                                                                                                                                                                                                                                |
| `vobs/dw_source/isdwh/exporter/apt/cfg/h_exis_apt_bestandsdaten.var`          | config   | Custom Exporter F.   | simple  | retire            | Configuration for `EXIS_SD_APT_BESTANDS` job, similar in structure to `h_exis_apt_nna_daten.var`.                                                                                                                                                                                                                                                                                                     |
| `vobs/dw_source/isdwh/exporter/apt/cfg/h_exis_apt_nna_voice.var`              | config   | other_etl_config     | simple  | retire            | Configuration for `EXIS_SD_APT_NNA_VOIC` job, similar in structure to `h_exis_apt_nna_daten.var`.                                                                                                                                                                                                                                                                                                     |
| `vobs/dw_source/isdwh/exporter/apt/cfg/h_exis_apt_rabattdaten.var`            | config   | Custom ETL F.        | simple  | retire            | Configuration for `EXIS_SD_APT_RABATT` job, similar in structure to `h_exis_apt_nna_daten.var`.                                                                                                                                                                                                                                                                                                       |
| `vobs/dw_source/isdwh/exporter/apt/sql/d_exis_apt_bestandsdaten.sql`          | sql      | Oracle PL/SQL        | simple  | retire            | Oracle SQL script to select and aggregate data from `RPT$TA_S_D1_VERTRAG`, `SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG` for stock data export.                                                                                                                                                                                                                                                  |
| `vobs/dw_source/isdwh/exporter/apt/sql/d_exis_apt_nna_daten.sql`              | sql      | Oracle PL/SQL        | simple  | retire            | Oracle SQL script to select data from `DWH$VI_L_MAP_FA_TARIF`, `BL_D_TARIF`, `DWH$VI_C_VERTRAG`, `DWH$TA_F_NNV_GPRS` for telephone system master data export.                                                                                                                                                                                                                                            |
| `vobs/dw_source/isdwh/exporter/apt/sql/d_exis_apt_nna_voice.sql`              | sql      | Oracle PL/SQL        | simple  | retire            | Oracle SQL script to select data from `DWH$VI_L_MAP_FA_TARIF`, `BL_D_TARIF`, `DWH$VI_C_VERTRAG`, `DWH$VI_F_NNV_TVD_12_MONATE`, `DWH$VI_L_TVD_LEISTUNGSKLASSE` for telephone system voice data export.                                                                                                                                                                                                         |
| `vobs/dw_source/isdwh/exporter/apt/sql/d_exis_apt_rabattdaten.sql`            | sql      | Oracle PL/SQL        | simple  | retire            | Oracle SQL script to select and aggregate data from `RPT$TA_S_D1_VERTRAG`, `RPT$TA_S_D1_DISCOUNT_RR`, `SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG` for discount data export.                                                                                                                                                                                                                         |
| `vobs/dw_source/isdwh/exporter/is/bin/k_exis_v2_defaults.cfg`                 | config   | EXIS Exporter / Shell| simple  | retire            | Default configuration parameters for the EXIS data exporter tool, including job settings, partitioning, SQL execution engine, and post-processing commands.                                                                                                                                                                                                                                             |

**Note on Tier and Automation Bucket:** The "simple" tier for SQL files is assumed in the absence of explicit analysis results, as is "retire" for config files if no specific guidance. The `r_exis_v2` shell script is classified as "redesign" due to its complex orchestration logic.

## 3. Target Architecture

The EXIS job will be re-architected on GCP as follows:

*   **Orchestration:** Four independent Airflow DAGs will replace the individual UC4 `JOBS_UNIX` objects. Each DAG will represent one export type (NNA Data, NNA Voice, Bestandsdaten, Rabattdaten).
*   **Core Export Logic:** The complex `r_exis_v2` shell script will be redesigned into a Python application or a set of Python modules/functions. This Python application will encapsulate the logic for:
    *   Reading job-specific configuration.
    *   Connecting to BigQuery.
    *   Executing BigQuery SQL for data extraction.
    *   Performing post-processing (e.g., nawk equivalents, gzip compression, CSV formatting).
    *   Uploading generated files to GCS.
    *   Handling SFTP distribution if required.
*   **Data Storage:**
    *   Source Oracle tables (`RPT$TA_S_D1_VERTRAG`, `SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG`, `DWH$VI_L_MAP_FA_TARIF`, `BL_D_TARIF`, `DWH$VI_C_VERTRAG`, `DWH$TA_F_NNV_GPRS`, `DWH$VI_F_NNV_TVD_12_MONATE`, `DWH$VI_L_TVD_LEISTUNGSKLASSE`, `RPT$TA_S_D1_DISCOUNT_RR`) will be ingested into corresponding BigQuery tables within a designated Data Warehouse project (e.g., `project_id.dwh_raw_layer.<table>`).
    *   Output CSV.GZ files will be stored in a dedicated GCS bucket (e.g., `gs://<your_bucket_name>/exis_exports/`).
*   **Configuration:** The `.var` and `.cfg` files will be converted into Airflow Variables, environment variables, or configuration files (e.g., YAML/JSON) read by the Python application, stored securely (e.g., Secret Manager).
*   **External Transfer:** SFTP distribution will be handled by a dedicated Python function using an SFTP library or by GCP services like Cloud Storage Transfer Service if external endpoints support pulling from GCS.

## 4. Data Flow & Lineage

The overall data flow for each export job (e.g., `DW.DWH_EXIS_SD_APT_NNA_DATA`) will be:

1.  **Airflow DAG Trigger:** An Airflow DAG (e.g., `dw_dwh_exis_sd_apt_nna_data`) is triggered (manual or scheduled).
2.  **Configuration Loading:** The DAG tasks retrieve job-specific configuration (e.g., parameters from `h_exis_apt_nna_daten.var`, defaults from `k_exis_v2_defaults.cfg`).
3.  **BigQuery Data Extraction:** A Python task (representing the `r_exis_v2` logic) executes a BigQuery SQL query (e.g., `d_exis_apt_nna_daten.sql` translated to BQSQL) to extract data from BigQuery tables. This step may involve dynamic date/month parameters (e.g., `MONATS_ID`).
4.  **In-Memory Processing/Transformation (Python):** The extracted data, if needed, undergoes post-processing (e.g., `nawk` equivalents for formatting, adding footer) within the Python application.
5.  **Gzip Compression (Python):** The processed data is compressed into a Gzip format.
6.  **GCS Upload:** The compressed CSV.GZ file is uploaded to a temporary location in GCS.
7.  **SFTP Distribution (Python/GCS Transfer):** The file is transferred from GCS to the external SFTP target.
8.  **GCS Archival/Move:** The file is moved to a final archive/store location within GCS (e.g., `$DW_DIR_EXP_APT/store` equivalent).
9.  **Logging & Status:** Airflow logs task execution, and potentially updates a BigQuery logging table (equivalent to `dwh$ta_k_meldungen`).

**Execution Order (Example for `DW.DWH_EXIS_SD_APT_NNA_DATA`):**

*   `start` (Airflow)
*   `extract_nna_data_from_bq` (Python application executing BQSQL from `d_exis_apt_nna_daten.sql` with params from `h_exis_apt_nna_daten.var`)
*   `post_process_and_compress` (Python application)
*   `upload_to_gcs` (Python application)
*   `distribute_via_sftp` (Python application or Cloud Storage Transfer Service)
*   `move_to_gcs_archive` (Python application or GCS operation)
*   `end` (Airflow)

## 5. Transformation Logic

Each original Oracle PL/SQL script (`d_exis_apt_bestandsdaten.sql`, `d_exis_apt_nna_daten.sql`, `d_exis_apt_nna_voice.sql`, `d_exis_apt_rabattdaten.sql`) will be translated directly into BigQuery SQL. The transformations involve:

*   **Data Type Mapping:**
    *   `TO_CHAR(RPT.VERTRAGSBEGINN,'DD.MM.YYYY')` -> `FORMAT_DATE('%d.%m.%Y', DATE(RPT.VERTRAGSBEGINN))`
    *   `LISTAGG(..., ',') WITHIN GROUP (ORDER BY ...)` -> `STRING_AGG(CAST(... AS STRING), ',' ORDER BY ...)`
    *   `TO_NUMBER(<FROM YYYYMM>)` -> `CAST(<FROM YYYYMM> AS INT64)`
    *   `ROUND(value / divisor, precision)`: Use `ROUND(CAST(value AS NUMERIC) / divisor, precision)` to ensure correct decimal handling in BigQuery.
    *   Oracle `||` for string concatenation -> BigQuery `CONCAT(string1, ',', string2)`.
    *   `TRUNC(number)` -> `CAST(FLOOR(CAST(number AS NUMERIC)) AS INT64)`.
*   **Oracle Optimizer Hints:** All `/*+ ... */` hints will be removed as they are specific to Oracle and not applicable in BigQuery.
*   **Join Syntax:** Implicit comma-separated joins in Oracle will be converted to explicit `JOIN` clauses (e.g., `FROM table1, table2 WHERE table1.id = table2.id` -> `FROM table1 JOIN table2 ON table1.id = table2.id`).
*   **Parameter Handling:** Placeholders like `<FROM YYYYMM>` will be replaced with Airflow-managed parameters or dynamically generated values within the Python application.

The shell script `r_exis_v2`'s `nawk` post-processing for appending footers (`X|<DESTINATION_FILE>|<FROM YYYYMMDD>|...`) will be re-implemented in Python, likely as a data frame manipulation or a direct string/file operation before compression and upload to GCS.

## 6. External Dependencies

| Original External System | Reference in Source Files                                                 | Migration Strategy                                                                                                                                                                                                                                                                                                                                                                                                           |
| :----------------------- | :------------------------------------------------------------------------ | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Oracle Database**      | `sqlplus -S $DBCONNECT`, `DWH$VI_L_MAP_FA_TARIF`, `DWH$TA_F_NNV_GPRS`, etc. | All source Oracle tables (`DWH$VI_L_MAP_FA_TARIF`, `BL_D_TARIF`, `DWH$VI_C_VERTRAG`, `DWH$TA_F_NNV_GPRS`, `DWH$VI_F_NNV_TVD_12_MONATE`, `DWH$VI_L_TVD_LEISTUNGSKLASSE`, `RPT$TA_S_D1_VERTRAG`, `SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG`, `RPT$TA_S_D1_DISCOUNT_RR`) will be ingested into corresponding BigQuery datasets (e.g., `project_id.dwh_raw_layer.<table>`) via a separate data ingestion pipeline (e.g., Database Migration Service, Fivetran, custom Dataflow jobs). The migrated SQL queries will then read from these BigQuery tables. |
| **SFTP Server**          | `<SFTP>` block in `.var` files (`PORT`, `USER`, `HOST`, `DIR`)            | SFTP distribution will be implemented using a Python function within the new application, leveraging an SFTP library (e.g., `paramiko`). This function will retrieve files from GCS and push them to the external SFTP target. Alternatively, if the external system can pull from GCS, Cloud Storage Transfer Service could be configured. SFTP credentials will be stored in Secret Manager.                               |
| **Local Filesystem**     | `$DW_DIR_EXP_APT/work`, `$DW_DIR_EXP_APT/store`                           | All local filesystem operations (temporary file creation, moving to store directory) will be replaced with Google Cloud Storage (GCS) operations. Output files will be written directly to a GCS bucket (`gs://<your_bucket_name>/exis_exports/`).                                                                                                                                                                   |
| **UC4 Scheduler**        | `JOBS_UNIX` XMLs, `EVNT_TIME` (missing), `JOBP` (missing)                 | UC4 scheduling and orchestration will be replaced by Airflow DAGs. Each `JOBS_UNIX` object will correspond to an Airflow DAG. The scheduling frequency (e.g., monthly, daily) for these DAGs will need to be determined by the business since the `EVNT_TIME` file was not provided.                                                                                                                            |
| **KornShell Utilities**  | `nawk`, `gzip`, `sed`, `grep`, `perl`, `sqlplus`, etc.                    | Core shell utilities will be replaced by Python equivalents. `nawk` logic for post-processing will be re-implemented using Python string/data manipulation. `gzip` will be handled by Python's `gzip` library. `sqlplus` commands will become BigQuery SQL execution via the Python BigQuery client library.                                                                                                |
| **Custom Shell Libraries**| `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parser.ksh`, etc.                | These helper libraries will need to be re-implemented in Python. Functions for parameter parsing, date handling, and error reporting will be integrated into the main Python application, or replaced with standard Python libraries.                                                                                                                                                                           |

## 7. Unresolved / Risks

*   **Undeclared Variables in `r_exis_v2`:** The `shellscript_to_horizon_python_design` tool highlighted numerous undeclared/externally defined variables (e.g., `DW_DIR_ROOT`, `DW_ORAUSER`, `LogDatei`, `SEPARATOR`, `SYSDATE`, `FROM`, `TO`, `SQLENGINE`, `SQLSPLIT`, `FILE_PARTITION`, `JOBID`, `CODEPAGE`, SFTP/SCP/MAIL credentials). The exact origin and definition of these variables are crucial for a complete and accurate Python re-implementation.
    *   **Mitigation:** Further analysis with source system owners or by reviewing environment configuration scripts is required to identify how these variables are set and used. They will likely translate to Airflow Variables, environment variables, or explicit Python configuration parameters.
*   **UC4 Workflow (JOBP/EVNT_TIME):** The lack of `JOBP` (workflow definition) and `EVNT_TIME` (scheduling) files means that the higher-level orchestration and scheduling dependencies between the four export jobs (and potentially other UC4 jobs) are currently unknown.
    *   **Mitigation:** Consult UC4 system administrators or business users to understand the full workflow and scheduling requirements. A master Airflow DAG might be needed to orchestrate these four individual export DAGs, or they may run independently.
*   **SFTP Credential Management:** Secure management of SFTP credentials for outbound transfers is critical.
    *   **Mitigation:** Use Google Secret Manager to store and retrieve SFTP credentials securely within the Airflow environment.
*   **Oracle Source Data Availability:** The successful migration hinges on having the Oracle source data fully and accurately ingested into BigQuery.
    *   **Risk:** Data discrepancies or incomplete ingestion could lead to incorrect exports.
    *   **Mitigation:** Establish robust data validation and reconciliation processes between Oracle and BigQuery after initial data migration.
*   **Performance Tuning:** Oracle queries with `/*+ parallel(...)*/` hints will require performance tuning in BigQuery, as these hints are not directly transferable.
    *   **Mitigation:** Leverage BigQuery's automatic query optimization, but be prepared for manual tuning (e.g., partitioning, clustering, optimal join strategies) if performance issues arise.

## 8. Build Plan

The migration will be executed in a phased approach:

1.  **BigQuery Data Ingestion (Pre-requisite):**
    *   Ingest all required Oracle source tables (e.g., `RPT$TA_S_D1_VERTRAG`, `SOF$TA_BPR_OPTIONEN`, `DWH$VI_L_MAP_FA_TARIF`, etc.) into a BigQuery `dwh_raw_layer` dataset. (Language: GCP Data Migration Services / Custom Dataflow).

2.  **SQL Translation & Validation:**
    *   Translate `d_exis_apt_bestandsdaten.sql` to `bq_d_exis_apt_bestandsdaten.sql` (Language: BigQuery SQL).
    *   Translate `d_exis_apt_nna_daten.sql` to `bq_d_exis_apt_nna_daten.sql` (Language: BigQuery SQL).
    *   Translate `d_exis_apt_nna_voice.sql` to `bq_d_exis_apt_nna_voice.sql` (Language: BigQuery SQL).
    *   Translate `d_exis_apt_rabattdaten.sql` to `bq_d_exis_apt_rabattdaten.sql` (Language: BigQuery SQL).
    *   Validate translated SQL queries against sample data in BigQuery for functional equivalence.

3.  **Python Export Application Development:**
    *   Develop a Python application (`exis_exporter.py`) to replace the `r_exis_v2` shell script. This application will:
        *   Accept configuration parameters (e.g., job type, config file path, date parameters).
        *   Implement logic for reading and parsing configuration (`.var`, `.cfg` equivalents).
        *   Utilize BigQuery client library to execute translated BQSQL queries.
        *   Implement `nawk` post-processing logic in Python.
        *   Implement gzip compression in Python.
        *   Handle GCS file uploads and archival.
        *   Implement SFTP client logic for external distribution (using `paramiko` or similar).
        *   Incorporate Python equivalents for custom shell helper libraries (`f_alis_msgerr.ksh`, `h_alis_parser.ksh`, `h_alis_date.ksh`). (Language: Python).

4.  **Airflow DAG Development:**
    *   Create `exis_nna_data_dag.py` (Airflow DAG): Orchestrates the NNA Data export.
        *   Task: Invoke `exis_exporter.py` with parameters for `h_exis_apt_nna_daten.var` and date. (Language: Python/Airflow).
    *   Create `exis_nna_voice_dag.py` (Airflow DAG): Orchestrates the NNA Voice export.
        *   Task: Invoke `exis_exporter.py` with parameters for `h_exis_apt_nna_voice.var` and date. (Language: Python/Airflow).
    *   Create `exis_bestands_dag.py` (Airflow DAG): Orchestrates the stock data export.
        *   Task: Invoke `exis_exporter.py` with parameters for `h_exis_apt_bestandsdaten.var`. (Language: Python/Airflow).
    *   Create `exis_rabatt_dag.py` (Airflow DAG): Orchestrates the discount data export.
        *   Task: Invoke `exis_exporter.py` with parameters for `h_exis_apt_rabattdaten.var`. (Language: Python/Airflow).
    *   Define scheduling and retry policies for each DAG based on business requirements.

5.  **Configuration Migration:**
    *   Extract key-value pairs from `.var` and `.cfg` files.
    *   Load these into Airflow Variables or environment variables, or as structured configuration files accessible by `exis_exporter.py`. (Language: Airflow UI/CLI, YAML/JSON).

6.  **Testing & Deployment:**
    *   Unit testing for Python components and BQSQL queries.
    *   Integration testing for full Airflow DAGs, including data extraction, processing, GCS upload, and SFTP distribution.
    *   Deployment to Airflow on GCP (Cloud Composer).

This build plan outlines the sequence of development and integration for migrating the EXIS job to GCP.