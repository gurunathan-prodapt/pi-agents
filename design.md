# Migration Design — EXIS_SD_APT_NNA_VOIC

## 1. Purpose & Scope
This job, `EXIS_SD_APT_NNA_VOIC`, is an assembled ETL workflow primarily responsible for the data export of telephone system master data. It extracts voice-related data by joining several Data Warehouse (DWH) tables, applies filters and reformatting, and then exports this data into a compressed CSV file for distribution to a target system via SFTP. The job is orchestrated by an Automic (UC4) `JOBS_UNIX` object, which invokes a shell script using a custom configuration file. The scope of this migration is to re-platform this ETL process to Google Cloud Platform, utilizing Cloud Composer (Airflow) for orchestration, BigQuery for SQL transformations, and other GCP services for file handling and external data transfers.

## 2. Source Inventory

The `EXIS_SD_APT_NNA_VOIC` job consists of the following source components:

| File Name (Relative Path) | Technology | Summary | Migration Bucket | Complexity Tier | Migration Flags |
| :------------------------ | :--------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :--------------- | :-------------- | :-------------- |
| `vobs/.../DW.DWH_EXIS_SD_APT_NNA_VOIC.xml` | UC4/Automic | This UC4 JOBS_UNIX object defines a job for exporting telephone system master data. It executes a shell script to export data into a compressed CSV file and distributes it to a target system. | `semi_auto` | (Not available) | (Not available) |
| `vobs/.../h_exis_apt_nna_voice.var` | Config (Custom ETL) | Configuration file for an ETL job that exports data from DWH tables to a gzipped CSV file and then distributes it via SFTP. | `retire` | (Not available) | (Not available) |
| `vobs/.../d_exis_apt_nna_voice.sql` | Oracle PL/SQL | This SQL script exports voice-related data by joining several DWH tables, applying filters, and reformatting some columns. The output is intended for a CSV file. | `retire` | (Not available) | (Not available) |

**Note on missing data:** Complexity tier and migration flags were not available for any of the component files from the `file_complexity` table. This indicates a gap in the static analysis results.

## 3. Target Architecture

The target architecture on Google Cloud Platform will consist of:

*   **Orchestration:** Google Cloud Composer (Apache Airflow) will replace the UC4 scheduler to manage the end-to-end workflow.
*   **Data Transformation:** Google BigQuery will host the transformed data and execute the SQL extraction logic. Source tables (`DWH$VI_L_MAP_FA_TARIF`, `BL_D_TARIF`, `DWH$VI_C_VERTRAG`, `DWH$VI_F_NNV_TVD_12_MONATE`, `DWH$VI_L_TVD_LEISTUNGSKLASSE`) are assumed to be migrated to BigQuery as external tables or native BigQuery tables.
*   **Data Processing:** Google Cloud Dataproc will be used to execute a PySpark job (derived from the `r_exis_v2` shell script and config file logic) responsible for data extraction, reformatting (e.g., `nawk` equivalent), and gzip compression. Alternatively, some of this logic might be absorbed into BigQuery UDFs or Cloud Functions/Run depending on complexity.
*   **File Storage:** Google Cloud Storage will be used as a staging area for the exported CSV files before distribution.
*   **External Data Transfer:** Google Cloud Storage Transfer Service or a custom Cloud Run service will replace the SFTP distribution for secure and automated transfer to external systems.

## 4. Data Flow & Lineage

The current data flow can be inferred as follows:

1.  **UC4 Job (`DW.DWH_EXIS_SD_APT_NNA_VOIC.xml`)** triggers the execution.
2.  The UC4 job executes a shell command, specifically `$HOME/aktuell/exporter/is/bin/r_exis_v2`, passing the configuration file `h_exis_apt_nna_voice.var` and a dynamically generated `MONAT_ID` parameter.
3.  The `r_exis_v2` process (likely a shell script itself) reads the configuration from `h_exis_apt_nna_voice.var`.
4.  The `h_exis_apt_nna_voice.var` configuration file points to `d_exis_apt_nna_voice.sql` for the core data extraction logic.
5.  The `d_exis_apt_nna_voice.sql` script queries several Oracle DWH tables (`DWH$VI_L_MAP_FA_TARIF`, `BL_D_TARIF`, `DWH$VI_C_VERTRAG`, `DWH$VI_F_NNV_TVD_12_MONATE`, `DWH$VI_L_TVD_LEISTUNGSKLASSE`) to extract and transform the telephone system master data. The `<FROM YYYYMM>` placeholder in the SQL is populated by the `MONAT_ID` passed from the UC4 job.
6.  The extracted data is then processed by `nawk` and `gzip` commands specified in the `<POSTPROCESSING>` section of `h_exis_apt_nna_voice.var`.
7.  Finally, the gzipped CSV file (`DWHM_APT_NNA_Daten_<SYSDATE YYYYMMDDHH24MISS>.csv.gz`) is distributed via SFTP, as defined in the `<DISTRIBUTION>` section of `h_exis_apt_nna_voice.var`.

In the target BigQuery environment, this flow will be re-engineered:

*   An **Airflow DAG** will orchestrate the entire process.
*   A **BigQuery SQL query** (derived from `d_exis_apt_nna_voice.sql`) will perform the data extraction and transformation from BigQuery source tables. This query will dynamically receive the `YYYYMM` parameter.
*   The output of the BigQuery query will be loaded into a temporary BigQuery table or directly exported to **Cloud Storage** as a CSV.
*   Post-processing steps (e.g., `nawk` equivalent for adding metadata/trailer, `gzip`) will be implemented as a **Cloud Dataflow pipeline**, a **Cloud Run service**, or within a **PySpark job on Dataproc**.
*   The final gzipped CSV file from Cloud Storage will be transferred to the external target system using **Cloud Storage Transfer Service** or a dedicated **Cloud Run service** configured for secure external transfer.

## 5. Transformation Logic

### 5.1 UC4 Orchestration (`DW.DWH_EXIS_SD_APT_NNA_VOIC.xml`) to Airflow DAG

The UC4 `JOBS_UNIX` object will be migrated to an Airflow DAG named `dw_dwh_exis_sd_apt_nna_voic`.
The DAG will execute a single task: `run_dw_dwh_exis_sd_apt_nna_voic`.
This task will be implemented using a `DataprocSubmitJobOperator` to submit a PySpark job (e.g., `r_exis_v2.py`) to a Dataproc cluster.
The `MONAT_ID` parameter, currently derived from `SYS_DATE('YYYYMMDD')` and `SUBSTR(...,1,6)` in UC4, will be calculated dynamically within the Airflow DAG using Python's `datetime` module to derive the `YYYYMM` format.

### 5.2 SQL Data Extraction (`d_exis_apt_nna_voice.sql`) to BigQuery SQL

The Oracle SQL script will be converted to BigQuery SQL. Key transformations include:

*   **Date Conversion:** `TO_DATE('47121231','YYYYMMDD')` becomes `DATE '4712-12-31'`.
*   **Numeric Conversion:** `TO_NUMBER(<FROM YYYYMM>)` becomes `CAST(<FROM YYYYMM> AS INT64)`.
*   **Arithmetic/Rounding:** `ROUND(NNA.DAUER_SEK/60,2)` becomes `ROUND(SAFE_DIVIDE(CAST(NNA.DAUER_SEK AS NUMERIC), 60), 2)`. Similarly for `RBETRAG_VBUD_NETTO_CENT`.
*   **String Concatenation:** `(TAR.MP_MARKTPRODUKT_BEZ||','|| TAR.MP_EG_JN_BEZ||','|| TAR.MP_GENERATION_BEZ)` becomes `CONCAT(TAR.MP_MARKTPRODUKT_BEZ, ',', TAR.MP_EG_JN_BEZ, ',', TAR.MP_GENERATION_BEZ)`.
*   **Truncation:** `TRUNC(TVD.LEISTUNGSKLASSE_ID/1000)` becomes `CAST(FLOOR(CAST(TVD.LEISTUNGSKLASSE_ID AS NUMERIC) / 1000) AS INT64)`.
*   **Table References:** Oracle table names (e.g., `DWH$VI_L_MAP_FA_TARIF`) will be mapped to their corresponding BigQuery table names, using backticks (e.g., `` `DWH_VI_L_MAP_FA_TARIF` ``).
*   **PARALLEL hints:** Oracle `/*+ PARALLEL(...) */` hints will be removed as BigQuery's execution engine automatically parallelizes queries.

**Converted BigQuery SQL Query:**

```sql
SELECT
  NNA.MONATS_ID,
  NNA.RAHMENVERTRAG,
  VER.MSISDN,
  VER.KUNDENKONTO,
  VER.T_MOBILE_KUNDENNUMMER,
  TAR.TARIF_ID,
  CONCAT(TAR.MP_MARKTPRODUKT_BEZ, ',', TAR.MP_EG_JN_BEZ, ',', TAR.MP_GENERATION_BEZ) AS TARIF,
  TVD.LEISTUNGSKLASSE_ID,
  TVD.LEISTUNGSKLASSE_TEXT,
  NNA.VERBINDUNGEN,
  ROUND(SAFE_DIVIDE(CAST(NNA.DAUER_SEK AS NUMERIC), 60), 2) AS DAUER_MIN,
  ROUND(SAFE_DIVIDE(CAST(NNA.RBETRAG_VBUD_NETTO_CENT AS NUMERIC), 100), 2) AS RBETRAG_VBUD_NETTO_EURO,
  TAR.MP_EG_JN_ID,
  TAR.MP_EG_JN_BEZ,
  TAR.MP_GENERATION_ID,
  TAR.MP_GENERATION_BEZ
FROM (
  SELECT
    TRF.DWH_TARIF_ID,
    TRF.TARIF_ID,
    D.MP_MARKTPRODUKT_BEZ,
    D.MP_EG_JN_BEZ,
    D.MP_GENERATION_BEZ,
    TRF.GUELTIG_BIS,
    D.MP_EG_JN_ID,
    D.MP_GENERATION_ID
  FROM `DWH_VI_L_MAP_FA_TARIF` TRF
  JOIN `BL_D_TARIF` D
    ON TRF.TARIF_ID = D.TARIF_ID
) TAR
JOIN `DWH_VI_C_VERTRAG` VER
  ON TAR.DWH_TARIF_ID = VER.DWH_TARIF_ID
JOIN `DWH_VI_F_NNV_TVD_12_MONATE` NNA
  ON VER.DWH_VERTRAG_ID = NNA.DWH_VERTRAG_ID
JOIN `DWH_VI_L_TVD_LEISTUNGSKLASSE` TVD
  ON NNA.LEISTUNGSKLASSE_ID = TVD.LEISTUNGSKLASSE_ID
WHERE NNA.RAHMENVERTRAG IS NOT NULL
  AND NNA.MONATS_ID = CAST(@FROM_YYYYMM AS INT64) -- Parameterized
  AND TAR.GUELTIG_BIS = DATE '4712-12-31'
  AND (
    (TVD.LEISTUNGSKLASSEGR_ID = 1
      AND (TVD.LEISTUNGSKLASSE_ID < 300 OR TVD.LEISTUNGSKLASSE_ID > 399))
    OR (
      LENGTH(TRIM(CAST(TVD.LEISTUNGSKLASSE_ID AS STRING))) = 6
      AND TVD.LEISTUNGSKLASSE_ID < 699999
      AND CAST(FLOOR(CAST(TVD.LEISTUNGSKLASSE_ID AS NUMERIC) / 1000) AS INT64) <> 622
    )
  );
```

### 5.3 Configuration File Logic (`h_exis_apt_nna_voice.var`)

The logic defined in this configuration file (which specifies `nawk` for post-processing and `gzip` for compression, followed by SFTP distribution) will need to be re-implemented. Given its `retire` migration bucket, a direct conversion is not expected; instead, its functionality will be absorbed into the target GCP components:
*   **`nawk` equivalent**: The `nawk` command adds a header/trailer with metadata to the CSV. This can be implemented within the PySpark job, as part of a Dataflow pipeline (e.g., using a Python transform), or as a custom Cloud Function/Run triggered after BigQuery export to Cloud Storage.
*   **`gzip` compression**: This can be handled by BigQuery's export options to Cloud Storage (if supported for CSV) or performed by the PySpark job/Cloud Function/Run before the file is finalized in Cloud Storage.

## 6. External Dependencies

| Legacy System | Type | How it's Used | GCP Replacement Strategy |
| :------------ | :--- | :------------ | :----------------------- |
| Oracle DWH | Database | Source of master data | **BigQuery:** Legacy Oracle DWH tables will be migrated to BigQuery. The SQL queries will directly access these BigQuery tables. |
| SFTP | File Transfer | Used for distributing the final gzipped CSV file to an external target system. | **Cloud Storage Transfer Service / Cloud Run:** The gzipped CSV will be stored in Cloud Storage. Distribution to the external SFTP target will be managed by either Cloud Storage Transfer Service configured for SFTP transfers, or a custom Cloud Run service that handles SFTP connectivity and transfer logic. |
| `DW.HOLE_PFAD` | UC4 Include | External UC4 include file, likely for defining paths or environment variables. | Will be integrated as environment variables, Airflow variables, or configuration files accessible by the Airflow DAG and Dataproc jobs. |
| `DW.LESE_LOG` | UC4 Include | External UC4 include file, likely for logging functions. | Will be replaced by native Airflow logging (to Cloud Logging) and structured logging within Dataproc/Cloud Run jobs. |

## 7. Unresolved / Risks

*   **Missing Complexity Data:** The absence of `file_complexity` data (tier and migration flags) for all source files is a risk. This information is crucial for accurately estimating migration effort and identifying specific technical challenges. Manual review may be required to fill this gap.
*   **Incomplete Lineage Edges:** The database did not provide explicit `lineage_edges` for any of the files, making automated understanding of execution order and data flow challenging. The current design relies on inferred relationships from file content analysis.
*   **"Retire" Bucket for Config/SQL:** Both the config (`.var`) and SQL (`.sql`) files are marked for `retire`. This suggests that their logic might not be directly migrated but rather redesigned or absorbed into other components, potentially requiring significant manual effort.
*   **`r_exis_v2` Executable:** The exact functionality of the `r_exis_v2` shell script is unknown without its source code. The MCP tool assumed a PySpark job on Dataproc, which might be a viable approach if `r_exis_v2` performs significant data processing. However, if it's a thin wrapper for simple shell commands, a Cloud Function/Run might be more appropriate. A manual review of `r_exis_v2` (if available) is required.
*   **Dynamic `MONAT_ID` Derivation:** The UC4 dynamic date derivation for `MONAT_ID` needs careful implementation in Python within Airflow to ensure correctness and maintainability.
*   **SFTP Distribution Confirmation:** The exact mechanism and credentials for the outgoing SFTP distribution need to be confirmed and securely configured in the chosen GCP transfer service (Storage Transfer Service or Cloud Run).
*   **Scheduling:** No UC4 scheduling information (e.g., `EVNT_TIME` file) was provided, so the Airflow DAG is initially designed with `schedule=None`. The actual scheduling requirements need to be identified and configured in Airflow.

## 8. Build Plan

1.  **Migrate Oracle DWH Tables to BigQuery:** (Prerequisite) All source Oracle DWH tables will be migrated to BigQuery.
2.  **Generate BigQuery SQL (from `d_exis_apt_nna_voice.sql`):**
    *   **Language:** BigQuery SQL
    *   **Content:** The converted SQL query provided in Section 5.2.
    *   **Output:** `d_exis_apt_nna_voice.bqsql`
3.  **Develop `r_exis_v2.py` (PySpark/Python for config/post-processing):**
    *   **Language:** Python (PySpark if Dataproc, or native Python for Cloud Run/Functions)
    *   **Content:** Re-implement the logic from `h_exis_apt_nna_voice.var` and the assumed functionality of `r_exis_v2`. This includes:
        *   Reading the `YYYYMM` parameter.
        *   Executing the BigQuery SQL query (e.g., via BigQuery Python client).
        *   Fetching the query results (if not directly exported by BigQuery).
        *   Implementing the `nawk`-like post-processing logic (adding header/trailer).
        *   Performing `gzip` compression.
        *   Storing the final file in Cloud Storage.
    *   **Output:** `r_exis_v2.py`
4.  **Generate Airflow DAG (from `DW.DWH_EXIS_SD_APT_NNA_VOIC.xml`):**
    *   **Language:** Python
    *   **Content:** An Airflow DAG (`dw_dwh_exis_sd_apt_nna_voic.py`) using `DataprocSubmitJobOperator` (or other operators like `BigQueryOperator`, `CloudRunOperator`) to orchestrate the steps. It will:
        *   Calculate the `MONAT_ID` parameter.
        *   Execute the BigQuery SQL for data extraction.
        *   Trigger the `r_exis_v2.py` (or equivalent Cloud Run/Function) for post-processing and compression.
        *   Trigger the Cloud Storage Transfer Service (or custom Cloud Run service) for SFTP distribution.
    *   **Output:** `dw_dwh_exis_sd_apt_nna_voic.py`
5.  **Configure Cloud Storage Transfer Service / Cloud Run for SFTP:**
    *   **Language:** Configuration (YAML/JSON) or Python (for Cloud Run)
    *   **Content:** Define the source (Cloud Storage bucket), target (external SFTP server), credentials, and transfer schedule (if needed, or triggered by Airflow).
    *   **Output:** Configuration files or Cloud Run service definition.