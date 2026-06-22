# Migration Design — EXIS_SD_APT_NNA_DATA

## 1. Purpose & Scope
The purpose of this migration is to re-platform the legacy UC4 job `DW.DWH_EXIS_SD_APT_NNA_DATA` from its current Automic (UC4) and UNIX environment to Google Cloud Platform, utilizing Airflow for orchestration and Dataproc for execution. This UC4 job is responsible for exporting telephone system master data into a compressed CSV file and distributing it to a target system. The output file is named following the pattern `DWHM_APT_NNA_Daten_<yyyymmddhhmmss>.csv.gz`.

The scope of this job is limited to the single UC4 `JOBS_UNIX` object provided. As no parent workflow (JOBP) or schedule (EVNT_TIME) was provided, this design focuses on migrating the core data export logic and assumes an independent or manually triggered execution.

## 2. Source Inventory
The migration targets a single UC4 XML file:

- **File Path**: `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_MONATLICH_JP/DW.DWH_EXIS_SD_APT_NNA_DATA.xml`
- **Technology**: UC4/Automic (JOBS_UNIX)
- **Summary**: This UC4 job defines a UNIX job that exports telephone system master data into a compressed CSV file and distributes it to a target system.
- **Complexity Tier**: Medium
- **Migration Bucket**: Semi-Auto (B2)

## 3. Target Architecture
The target architecture for this job will be:
- **Orchestration**: Apache Airflow on Google Cloud Composer.
- **Processing**: A PySpark script executed on a Google Cloud Dataproc cluster.
- **Data Storage (Intermediate/Output)**: Google Cloud Storage (GCS) for the exported CSV/GZIP files.
- **Target Platform**: BigQuery (implied as a potential downstream consumer, though the job's direct target is a distributed CSV).

The UC4 `JOBS_UNIX` will be converted into a single-task Airflow DAG.

## 4. Data Flow & Lineage
Due to the absence of explicit lineage in the database for this specific UC4 file, the data flow is inferred from the script content and job description:

1.  **Input**: The `r_exis_v2` executable, which forms the core of the UC4 job, is responsible for reading "telephone system master data." The source system for this data is not explicitly defined but is an external dependency.
2.  **Configuration**: The job utilizes a configuration file specified by `-k $HOME/aktuell/exporter/apt/cfg/h_exis_apt_nna_daten.var`. This file likely contains parameters, connection details, or logic for the export.
3.  **Parameterization**: A month identifier (`&MONAT_ID`) is derived from the current system date (`SYS_DATE('YYYYMMDD')` truncated to YYYYMM) and passed to `r_exis_v2` via the `-p` flag.
4.  **Transformation/Export**: The `r_exis_v2` script processes the master data and generates a compressed CSV file (`DWHM_APT_NNA_Daten_<yyyymmddhhmmss>.csv.gz`). This processing logic will be translated into a PySpark application.
5.  **Output**: The generated CSV/GZIP file is intended to be distributed to a target system. In the GCP context, this file will likely be landed in a GCS bucket.
6.  **UC4 Includes**: The UC4 script includes `DW.HOLE_PFAD` and `DW.LESE_LOG`. These are likely UC4-specific helper objects for path management and logging, respectively. Their functionality needs to be evaluated and incorporated into the Airflow DAG or the PySpark script, or replaced by standard GCP logging/path mechanisms.

**Execution Order (Airflow DAG):**
- `start` (Airflow DAG start)
- `dwh_exis_sd_apt_nna_data` (DataprocSubmitJobOperator running the PySpark script)
- `end` (Airflow DAG completion)

## 5. Transformation Logic
The core transformation logic resides within the `r_exis_v2` executable called by the UC4 job. The migration involves re-implementing this logic in PySpark.

**Original UC4 Script Snippet:**
```
:inc DW.HOLE_PFAD
:set &DWH_JOB_KENNUNG='EXIS_SD_APT_NNA_DATA'
. $HOME/.dw_init

: set &MONAT_ID = SYS_DATE('YYYYMMDD')
: set &MONAT_ID = SUBSTR(&MONAT_ID,1,6)
$HOME/aktuell/exporter/is/bin/r_exis_v2 -k $HOME/aktuell/exporter/apt/cfg/h_exis_apt_nna_daten.var -p &MONAT_ID

:inc DW.LESE_LOG
```

**Proposed BigQuery / PySpark Transformation Logic:**
1.  **Orchestration (Airflow)**:
    *   An Airflow DAG (`dw_dwh_exis_sd_apt_nna_data`) will be created.
    *   It will contain a `DataprocSubmitJobOperator` task named `dwh_exis_sd_apt_nna_data`.
    *   This task will execute a PySpark script (e.g., `exis_v2.py`) on a Dataproc cluster.
    *   The `&MONAT_ID` parameter, which is a YYYYMM string of the current date, will be computed within the Airflow task or passed as a parameter to the PySpark script (e.g., `ds_nodash[0:6]` from Airflow context).
    *   The configuration file path will be passed as an argument to the PySpark job.

2.  **Data Export (PySpark)**:
    *   The `exis_v2.py` PySpark script will encapsulate the functionality of the original `r_exis_v2` executable.
    *   It will read the configuration from `h_exis_apt_nna_daten.var` (which needs to be made accessible, e.g., on GCS).
    *   It will connect to the source system containing "telephone system master data" (details to be determined, likely an external database or API).
    *   It will extract, transform (if any), and write the data into a compressed CSV format.
    *   The output file will be named `DWHM_APT_NNA_Daten_YYYYMMDDHHMMSS.csv.gz` and stored in a designated GCS bucket.

3.  **UC4 Includes**: The functionalities of `DW.HOLE_PFAD` (path definition) and `DW.LESE_LOG` (logging) will be handled by:
    *   Standard Airflow/Cloud Composer mechanisms for path management (e.g., GCS buckets for scripts, configs, and outputs).
    *   Stackdriver Logging for application logs from the PySpark job and Airflow logs.

## 6. External Dependencies
The original UC4 job interacts with several external components:

-   **Source of Telephone System Master Data**: This is an implicit input to the `r_exis_v2` script. The specific database, API, or system needs to be identified.
    -   **Replacement Strategy**: The PySpark `exis_v2.py` will need to establish connectivity to this source system. This might involve direct database connections, API calls, or reading from a staging area in GCP (e.g., Cloud Storage, Cloud SQL, or BigQuery if pre-ingested). Appropriate GCP authentication (e.g., Service Accounts) will be used.

-   **Configuration File (`$HOME/aktuell/exporter/apt/cfg/h_exis_apt_nna_daten.var`)**: This file configures the `r_exis_v2` export process.
    -   **Replacement Strategy**: This configuration file should be stored in a GCS bucket and read by the PySpark script at runtime. Sensitive information within it should be managed using Secret Manager.

-   **Target System for CSV Distribution**: The job's summary states it "distributes it to a target system."
    -   **Replacement Strategy**: The final destination and method of distribution need to be clarified. Options include:
        *   Another GCS bucket for consumption by other GCP services or external systems.
        *   SFTP/FTPS via a managed service or Cloud VPN/Interconnect if an on-premise system is the target.
        *   Pub/Sub notifications upon file arrival for event-driven processing.

-   **UC4 Specifics (`DW.UNIX.ISTNS` login, `|DWHDWH1P|HOST`, `:inc` commands)**: These are internal UC4 mechanisms.
    -   **Replacement Strategy**: These will be replaced by Airflow's native capabilities for user/service accounts, host selection (Dataproc cluster), and Python module imports/function calls for modularity.

## 7. Unresolved / Risks
-   **Core Logic of `r_exis_v2`**: The actual content and logic of the `r_exis_v2` executable and the configuration file `h_exis_apt_nna_daten.var` are currently unknown. These need to be reverse-engineered or provided by SMEs for accurate PySpark re-implementation. This is the primary risk for a "semi-auto" migration.
-   **Source System Details**: The specific source of "telephone system master data" is not detailed. Connectivity and schema details will be required.
-   **Target System for Distribution**: The ultimate destination and method of distribution for the exported CSV are unclear and require clarification.
-   **Complete UC4 Workflow**: This design assumes a standalone job. If `DW.DWH_EXIS_SD_APT_NNA_DATA` is part of a larger UC4 workflow (Job Plan `JOBP`) or has specific scheduling requirements (`EVNT_TIME`), those dependencies are not captured and need to be analyzed separately. This could lead to a more complex Airflow DAG structure.
-   **Empty Metadata Fields**: The `file_analysis` fields for `complexity_signals`, `file_purpose`, `input_sources`, and `output_targets` were empty, indicating limited automated insight into the job's functionality. This reinforces the need for manual review and SME input.
-   **Retry Semantics**: The UC4 job's error handling implies restartability without prior actions. While Airflow offers retry mechanisms, the exact retry count and delay are not defined in the source, requiring a business decision.

## 8. Build Plan
The build plan will consist of generating the following components:

1.  **Airflow DAG Python File**:
    *   **File Name**: `dw_dwh_exis_sd_apt_nna_data_dag.py`
    *   **Language**: Python
    *   **Content**: Defines the DAG, its schedule (or lack thereof), default arguments, and the `DataprocSubmitJobOperator` task to launch the PySpark job. It will handle the derivation of the `MONAT_ID` parameter.

2.  **PySpark Script**:
    *   **File Name**: `exis_v2.py`
    *   **Language**: PySpark (Python)
    *   **Content**: This script will contain the re-implemented logic of the original `r_exis_v2` executable. It will be responsible for connecting to the source system, reading the configuration file (from GCS), extracting the data, applying any necessary transformations, and writing the compressed CSV output to a GCS bucket.

3.  **Configuration File (Migrated)**:
    *   **File Name**: `h_exis_apt_nna_daten.var` (or a GCP-native equivalent like a JSON config)
    *   **Location**: Google Cloud Storage
    *   **Content**: Configuration parameters for the `exis_v2.py` PySpark script. Sensitive parameters will be replaced by references to Google Secret Manager.

4.  **Documentation/README**:
    *   **Content**: Instructions for deployment, operational procedures, and details about the source and target systems.