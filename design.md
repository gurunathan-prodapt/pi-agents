# Migration Design — EXIS_SD_APT_NNA_VOIC

## 1. Purpose & Scope
The purpose of this job, `EXIS_SD_APT_NNA_VOIC`, is to export voice-related telephone system master data from various DWH (Data Warehouse) tables into a gzipped CSV file. This file is then distributed to a target system via SFTP. The job is orchestrated by a UC4 job scheduler and leverages an Oracle PL/SQL script for data extraction, configured by a `.var` file. The migration aims to re-implement this functionality using Google Cloud Platform services, specifically BigQuery for data processing and Airflow for orchestration.

## 2. Source Inventory

| File Path                                                                                                                          | Technology    | Tier   | Automation Bucket | Summary                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| :--------------------------------------------------------------------------------------------------------------------------------- | :------------ | :----- | :---------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isdwh/exporter/apt/cfg/h_exis_apt_nna_voice.var`                                                                    | Config        | medium | retire            | Configuration file for an ETL job that exports data from DWH tables to a gzipped CSV file and then distributes it via SFTP. It defines the job ID, separator, destination file path, and includes the SQL script.                                                                                                                                                                                                                                                                               |
| `vobs/dw_source/isdwh/exporter/apt/sql/d_exis_apt_nna_voice.sql`                                                                    | Oracle PL/SQL | medium | retire            | This SQL script exports voice-related data by joining several DWH tables, applying filters, and reformatting some columns. The output is intended for a CSV file. It queries tables like `DWH$VI_L_MAP_FA_TARIF`, `BL_D_TARIF`, `DWH$VI_C_VERTRAG`, `DWH$VI_F_NNV_TVD_12_MONATE`, `DWH$VI_L_TVD_LEISTUNGSKLASSE`.                                                                                                                                                                                    |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_MONATLICH_JP/DW.DWH_EXIS_SD_APT_NNA_VOIC.xml` | UC4/Automic   | medium | semi_auto         | This UC4 JOBS_UNIX object defines a job for exporting telephone system master data. It executes a shell script (`r_exis_v2`) which uses the `.var` configuration file to run the SQL query, export data to a compressed CSV file, and distribute it to a target system via SFTP.                                                                                                                                                                                                             |

## 3. Target Architecture

The target architecture will leverage Google Cloud Platform (GCP) services:

*   **Orchestration**: Apache Airflow will replace the UC4 scheduler. A new Airflow DAG will be created to manage the execution flow.
*   **Data Processing**: Google BigQuery will be used as the target data warehouse. The Oracle PL/SQL query will be converted to BigQuery SQL. The source DWH tables (e.g., `DWH$VI_L_MAP_FA_TARIF`, `BL_D_TARIF`, `DWH$VI_C_VERTRAG`, `DWH$VI_F_NNV_TVD_12_MONATE`, `DWH$VI_L_TVD_LEISTUNGSKLASSE`) will be migrated to corresponding BigQuery tables, likely within a dedicated dataset (e.g., `raw_dwh`).
*   **Data Export & Distribution**: A Python application will be responsible for:
    *   Executing the BigQuery SQL query.
    *   Extracting the results to Google Cloud Storage (GCS).
    *   Gzipping the CSV file if not already handled by BigQuery export.
    *   Performing the SFTP transfer to the external system. This Python application will be invoked by the Airflow DAG.

## 4. Data Flow & Lineage

The migrated data flow will be as follows:

1.  **Airflow DAG Trigger**: The Airflow DAG (`dwh_exis_sd_apt_nna_voic_dag`) is scheduled to run.
2.  **BigQuery SQL Execution**: A task within the Airflow DAG will execute the converted BigQuery SQL query against the `raw_dwh` dataset. This query will join the migrated DWH tables (`raw_dwh.VI_L_MAP_FA_TARIF`, `raw_dwh.BL_D_TARIF`, `raw_dwh.VI_C_VERTRAG`, `raw_dwh.VI_F_NNV_TVD_12_MONATE`, `raw_dwh.VI_L_TVD_LEISTUNGSKLASSE`).
3.  **Data Export to GCS**: The results of the BigQuery query will be exported to a temporary GCS bucket as a gzipped CSV file (e.g., `gs://<gcs_bucket>/dwhm_apt_nna_voice_<timestamp>.csv.gz`).
4.  **SFTP Transfer**: Another task in the Airflow DAG will invoke a Python script. This script will download the gzipped CSV from GCS and then initiate an SFTP transfer to the external target system, using the credentials and directory configured for SFTP.
5.  **Cleanup**: Optionally, a final task will clean up the temporary file from GCS.

**Execution Order:**
1.  Airflow DAG starts.
2.  BigQuery data extraction task runs.
3.  GCS export task runs.
4.  Python SFTP transfer task runs.
5.  (Optional) GCS cleanup task runs.

## 5. Transformation Logic

The core transformation logic resides in the `d_exis_apt_nna_voice.sql` file. This Oracle PL/SQL query will be converted to BigQuery SQL, incorporating the following changes identified by the `hql_sql_to_bqsql_design` tool:

**Source Query (Oracle-like):**
```sql
SELECT /*+ PARALLEL(VER,4) PARALLEL(NNA,4) PARALLEL(TVD,4)*/
  NNA.MONATS_ID ,
  NNA.RAHMENVERTRAG ,
  VER.MSISDN ,
  VER.KUNDENKONTO ,
  VER.T_MOBILE_KUNDENNUMMER ,
  TAR.TARIF_ID ,
  (TAR.MP_MARKTPRODUKT_BEZ||','|| TAR.MP_EG_JN_BEZ||','|| TAR.MP_GENERATION_BEZ) AS TARIF ,
  TVD.LEISTUNGSKLASSE_ID ,
  TVD.LEISTUNGSKLASSE_TEXT ,
  NNA.VERBINDUNGEN ,
  ROUND(NNA.DAUER_SEK/60,2) DAUER_MIN,
  ROUND(NNA.RBETRAG_VBUD_NETTO_CENT/100,2) RBETRAG_VBUD_NETTO_EURO
  ,TAR.MP_EG_JN_ID
  ,TAR.MP_EG_JN_BEZ
  ,TAR.MP_GENERATION_ID
  ,TAR.MP_GENERATION_BEZ
FROM
  (SELECT /*+ PARALLEL(TRF,4) PARALLEL(D,4)*/
	TRF.DWH_TARIF_ID, TRF.TARIF_ID ,  D.MP_MARKTPRODUKT_BEZ, D.MP_EG_JN_BEZ,D.MP_GENERATION_BEZ,TRF.GUELTIG_BIS
	,D.MP_EG_JN_ID ,D.MP_GENERATION_ID
  FROM DWH$VI_L_MAP_FA_TARIF TRF,  BL_D_TARIF D
  WHERE TRF.TARIF_ID=D.TARIF_ID) TAR
  ,DWH$VI_C_VERTRAG VER
  ,DWH$VI_F_NNV_TVD_12_MONATE NNA
  ,DWH$VI_L_TVD_LEISTUNGSKLASSE TVD
WHERE TAR.DWH_TARIF_ID=VER.DWH_TARIF_ID
AND VER.DWH_VERTRAG_ID=NNA.DWH_VERTRAG_ID
AND NNA.RAHMENVERTRAG  IS NOT NULL
AND NNA.MONATS_ID=TO_NUMBER(<FROM YYYYMM>)
AND NNA.LEISTUNGSKLASSE_ID=TVD.LEISTUNGSKLASSE_ID
AND TAR.GUELTIG_BIS = TO_DATE('47121231','YYYYMMDD')
AND ((TVD.LEISTUNGSKLASSEGR_ID = 1 AND (TVD.LEISTUNGSKLASSE_ID < 300 or TVD.LEISTUNGSKLASSE_ID > 399))
  OR (
    LENGTH(TRIM(TVD.LEISTUNGSKLASSE_ID))=6
    AND TVD.LEISTUNGSKLASSE_ID < 699999
	AND TRUNC(TVD.LEISTUNGSKLASSE_ID/1000) <> 622
  ));
```

**BigQuery SQL Conversion (`d_exis_apt_nna_voice.bq.sql`):**
The BigQuery SQL will adhere to BigQuery syntax and best practices. The derived design by `hql_sql_to_bqsql_design` provides a solid starting point:

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
  ROUND(CAST(NNA.DAUER_SEK AS NUMERIC) / 60, 2) AS DAUER_MIN,
  ROUND(CAST(NNA.RBETRAG_VBUD_NETTO_CENT AS NUMERIC) / 100, 2) AS RBETRAG_VBUD_NETTO_EURO,
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
  FROM `raw_dwh.VI_L_MAP_FA_TARIF` TRF  -- Assumed target BigQuery table name
  JOIN `raw_dwh.BL_D_TARIF` D           -- Assumed target BigQuery table name
    ON TRF.TARIF_ID = D.TARIF_ID
) TAR
JOIN `raw_dwh.VI_C_VERTRAG` VER        -- Assumed target BigQuery table name
  ON TAR.DWH_TARIF_ID = VER.DWH_TARIF_ID
JOIN `raw_dwh.VI_F_NNV_TVD_12_MONATE` NNA -- Assumed target BigQuery table name
  ON VER.DWH_VERTRAG_ID = NNA.DWH_VERTRAG_ID
JOIN `raw_dwh.VI_L_TVD_LEISTUNGSKLASSE` TVD -- Assumed target BigQuery table name
  ON NNA.LEISTUNGSKLASSE_ID = TVD.LEISTUNGSKLASSE_ID
WHERE NNA.RAHMENVERTRAG IS NOT NULL
  AND NNA.MONATS_ID = CAST(@FROM_YYYYMM AS INT64)  -- Parameterized
  AND TAR.GUELTIG_BIS = DATE '4712-12-31'
  AND (
    (TVD.LEISTUNGSKLASSEGR_ID = 1
      AND (TVD.LEISTUNGSKLASSE_ID < 300 OR TVD.LEISTUNGSKLASSE_ID > 399)
    )
    OR (
      LENGTH(TRIM(CAST(TVD.LEISTUNGSKLASSE_ID AS STRING))) = 6
      AND TVD.LEISTUNGSKLASSE_ID < 699999
      AND CAST(FLOOR(CAST(TVD.LEISTUNGSKLASSE_ID AS NUMERIC) / 1000) AS INT64) <> 622
    )
  );
```
**Parameter Handling**: The `<FROM YYYYMM>` parameter will be passed to the BigQuery SQL query, likely through Airflow's templating or directly via the Python client library.

## 6. External Dependencies

The primary external dependency is the SFTP server for distributing the generated CSV file.

*   **SFTP Server**:
    *   **Legacy Configuration**: The `h_exis_apt_nna_voice.var` file contains SFTP connection details (`PORT`, `USER`, `HOST`, `DIR`) via environment variables (`$DW_APT_SFTP_PORT`, `$DW_APT_SFTP_USER`, `$DW_APT_SFTP_SERVER`, `$DW_APT_SFTP_DIR`).
    *   **Target Implementation**: In the GCP environment, these credentials and connection details should be securely stored, e.g., in Google Secret Manager. The Python script handling the SFTP transfer will retrieve these credentials and use an SFTP client library (e.g., `paramiko`) to connect to the external SFTP server and upload the gzipped CSV file.

## 7. Unresolved / Risks

*   **`r_exis_v2` Shell Script**: The UC4 job invokes an external shell script `r_exis_v2`. The content of this script was not part of the provided analysis. It's assumed this script's primary function is to interpret the `.var` file, execute the SQL, and manage the SFTP process. This functionality will be fully reimplemented in Python as part of the Airflow DAG's tasks. The exact logic of `r_exis_v2` (e.g., error handling, logging, specific file manipulations beyond simple gzip) needs to be reverse-engineered or confirmed.
*   **Full UC4 Workflow**: The analysis indicated that the provided UC4 XML is a partial workflow. While this specific job can be migrated, there might be upstream or downstream UC4 dependencies that are not yet understood or included in this scope.
*   **Data Types and Schemas**: The BigQuery SQL conversion assumed the existence and schema of the DWH tables in BigQuery. The exact data types of columns in the source Oracle DWH tables need to be accurately mapped to BigQuery data types during the initial data migration phase.
*   **Parameter `MONATS_ID`**: The `<FROM YYYYMM>` parameter's source and how it's dynamically generated (`: set &MONAT_ID = SYS_DATE('YYYYMMDD') : set &MONAT_ID = SUBSTR(&MONAT_ID,1,6)`) in the UC4 script needs to be precisely replicated in the Airflow DAG for consistency.

## 8. Build Plan

The migration will involve building the following artifacts:

1.  **BigQuery SQL Script (`d_exis_apt_nna_voice.bq.sql`)**:
    *   **Language**: BigQuery Standard SQL
    *   **Content**: The converted SQL query from section 5, referencing the new BigQuery table names.
    *   **Purpose**: Extracts the required data from BigQuery.

2.  **Airflow DAG (`dwh_exis_sd_apt_nna_voic_dag.py`)**:
    *   **Language**: Python
    *   **Content**:
        *   Defines the DAG, its schedule, and default arguments.
        *   A `BigQueryExecuteQueryOperator` (or similar) to run `d_exis_apt_nna_voice.bq.sql`.
        *   A `BigQueryToGCSOperator` to export query results to a gzipped CSV in GCS.
        *   A `PythonOperator` to execute a custom Python function for SFTP transfer (see next point).
    *   **Purpose**: Orchestrates the entire data extraction and distribution process.

3.  **SFTP Transfer Python Script (`sftp_exporter.py`)**:
    *   **Language**: Python
    *   **Content**:
        *   Takes GCS path, SFTP host, user, port, and remote directory as input.
        *   Downloads the gzipped CSV file from GCS.
        *   Connects to the SFTP server using `paramiko` (or similar library) and securely stored credentials.
        *   Uploads the file to the specified remote directory.
    *   **Purpose**: Handles the secure transfer of the exported data to the external system.

4.  **BigQuery Table DDLs**:
    *   **Language**: BigQuery DDL
    *   **Content**: `CREATE TABLE` statements for `raw_dwh.VI_L_MAP_FA_TARIF`, `raw_dwh.BL_D_TARIF`, `raw_dwh.VI_C_VERTRAG`, `raw_dwh.VI_F_NNV_TVD_12_MONATE`, `raw_dwh.VI_L_TVD_LEISTUNGSKLASSE`, mirroring the schema of the source Oracle tables.
    *   **Purpose**: To set up the target tables in BigQuery before data loading.