# Migration Design — EXIS_SD_APT_BESTANDS

## 1. Purpose & Scope

The job `EXIS_SD_APT_BESTANDS` is responsible for extracting stock data from several Oracle database tables, processing it, compressing it into a CSV.gz file, and distributing it via SFTP. This process is orchestrated by a UC4 `JOBS_UNIX` object which invokes a custom executable (`r_exis_v2`) using a configuration file (`h_exis_apt_bestandsdaten.var`). The core data extraction logic is defined in an Oracle PL/SQL script (`d_exis_apt_bestandsdaten.sql`).

The scope of this migration is to re-platform the UC4 orchestration to Google Cloud Composer (Airflow), convert the Oracle SQL extraction logic to BigQuery SQL, and replace the custom exporter framework (including post-processing and SFTP distribution) with appropriate Google Cloud services.

## 2. Source Inventory

The following source files constitute the `EXIS_SD_APT_BESTANDS` job:

| File Name (relative_path) | Technology | Purpose | Complexity Tier | Automation Bucket | Summary |
| :------------------------ | :--------- | :------ | :-------------- | :---------------- | :------ |
| `vobs/dw_source/isdwh/uc4_prod_exports/.../DW.DWH_EXIS_SD_APT_BESTANDS.xml` | UC4/Automic | Orchestration | unknown | semi_auto | This UC4 JOBS_UNIX definition orchestrates the daily export of stock data from source tables into a compressed CSV file using a custom executable. |
| `vobs/dw_source/isdwh/exporter/apt/cfg/h_exis_apt_bestandsdaten.var` | Custom Exporter Framework | Configuration | unknown | retire | This file configures an export job named EXIS_SD_APT_BESTANDS, specifying the SQL source, output file destination, post-processing steps (nawk, gzip), and SFTP distribution details. |
| `vobs/dw_source/isdwh/exporter/apt/sql/d_exis_apt_bestandsdaten.sql` | Oracle PL/SQL | Data Extraction | unknown | retire | This SQL script selects and aggregates data from several tables for export to a CSV file. |

## 3. Target Architecture

The migrated job will leverage Google Cloud Platform services:

*   **Orchestration:** Google Cloud Composer (Airflow) will manage the workflow execution.
*   **Data Extraction & Transformation:** BigQuery will be used for SQL-based data extraction and transformation. The existing Oracle tables `RPT$TA_S_D1_VERTRAG`, `SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG` are assumed to be migrated to BigQuery tables.
*   **Post-processing (Compression, SFTP):** Google Cloud Storage for intermediate and final file storage, possibly Cloud Functions or Cloud Run for specific post-processing steps like `nawk` logic if not handled directly in BigQuery export. Storage Transfer Service or a custom Cloud Function/Cloud Run service will handle SFTP distribution.
*   **Output Data:** The final compressed CSV file (`DWHM_APT_BESTANDSREPORT_<yyyymmddhhmmss>.csv.gz`) will be stored in a Cloud Storage bucket before SFTP distribution.

## 4. Data Flow & Lineage

The original job flow is as follows:
1.  **UC4 Job (`DW.DWH_EXIS_SD_APT_BESTANDS.xml`)** triggers.
2.  The UC4 job executes a shell command involving `r_exis_v2` with `h_exis_apt_bestandsdaten.var` as a configuration.
3.  The `h_exis_apt_bestandsdaten.var` file points to `d_exis_apt_bestandsdaten.sql` for data extraction.
4.  **Oracle SQL (`d_exis_apt_bestandsdaten.sql`)** reads data from:
    *   `RPT$TA_S_D1_VERTRAG`
    *   `SOF$TA_BPR_OPTIONEN`
    *   `SOF$VI_L_OPTIONZUORDNUNG`
5.  The extracted data is processed (including `LISTAGG`, `TO_CHAR` operations).
6.  Post-processing steps defined in `h_exis_apt_bestandsdaten.var` (e.g., `nawk`, `gzip`) are applied.
7.  The final `DWHM_APT_BESTANDSREPORT_<yyyymmddhhmmss>.csv.gz` file is generated and then distributed via SFTP.

**Migrated Data Flow:**
1.  **Cloud Composer (Airflow DAG `dw_dwh_exis_sd_apt_bestands`)** triggers.
2.  An Airflow task executes a BigQuery SQL query (derived from `d_exis_apt_bestandsdaten.sql`).
3.  The BigQuery SQL query reads from migrated BigQuery tables:
    *   `RPT.TA_S_D1_VERTRAG`
    *   `SOF.TA_BPR_OPTIONEN`
    *   `SOF.VI_L_OPTIONZUORDNUNG`
4.  The result of the BigQuery query is exported directly to a Cloud Storage bucket as a compressed CSV file. BigQuery's export functionality supports CSV and GZIP compression.
5.  An Airflow task or a Cloud Function triggered by the Cloud Storage object creation performs any remaining `nawk`-like post-processing if not already handled by BigQuery's SQL.
6.  A subsequent Airflow task, or a dedicated Cloud Function/Cloud Run service, uses Storage Transfer Service or a custom SFTP client to transfer the file from Cloud Storage to the external SFTP target system.

## 5. Transformation Logic

### `DW.DWH_EXIS_SD_APT_BESTANDS.xml` (UC4 Orchestration)

This UC4 job will be re-platformed to an Airflow DAG.

*   **DAG ID:** `dw_dwh_exis_sd_apt_bestands`
*   **Schedule:** Not derivable from the provided UC4 XML. A cron schedule must be determined from external sources or set to `None` for manual/event-driven triggers.
*   **Tasks:** A single main task will be responsible for orchestrating the data extraction, transformation, and distribution. This task could be implemented as a `BigQueryInsertJobOperator` for the SQL execution followed by `GCSToSFTPOperator` (or similar for custom SFTP). The current `DataprocSubmitJobOperator` placeholder in the tool output needs to be adjusted based on the specific implementation of the extraction/export.
*   **Variables:** UC4 variables like `&DWH_JOB_KENNUNG` and includes like `DW.HOLE_PFAD`, `DW.LESE_LOG` need to be translated to Airflow variables, XComs, or Python functions/operators.
*   **Error Handling:** Default Airflow retry mechanisms can be used, with `retries=0` as per the UC4 analysis unless a specific retry policy is defined for the target.

### `h_exis_apt_bestandsdaten.var` (Custom Exporter Configuration)

This configuration file defines the SQL source, output path, post-processing, and SFTP details. Its logic will be decomposed and integrated into the Airflow DAG and associated BigQuery/Cloud Storage/SFTP tasks.

*   **SQL Source:** The reference to `d_exis_apt_bestandsdaten.sql` will be directly translated into the BigQuery SQL task.
*   **Destination:** The output path `$DW_DIR_EXP_APT/work/DWHM_APT_BESTANDSREPORT_<SYSDATE YYYYMMDDHH24MISS>.csv.gz` will be mapped to a Cloud Storage bucket path, with BigQuery's export functionality handling the dynamic timestamping and `.gz` compression.
*   **Post-processing (`nawk`, `gzip`):** `gzip` will be handled by BigQuery's export options. The `nawk` logic for adding a header line (`X|<DESTINATION_FILE>|<FROM YYYYMMDD>|` NR `|V_S_Bestandsreport|<SYSDATE YYYYMMDD>`) will need to be implemented either as a custom Python operator in Airflow, a Cloud Function, or potentially as part of the BigQuery SQL if complex enough (e.g., using `UNION ALL` for a header row).
*   **Distribution (`SFTP`):** The SFTP parameters (PORT, USER, HOST, DIR) will be configured in an Airflow Connection and used by an SFTP operator or custom Cloud Function/Cloud Run for file transfer.

### `d_exis_apt_bestandsdaten.sql` (Oracle PL/SQL)

The Oracle SQL query will be directly translated to BigQuery SQL.

**Original Oracle SQL:**

```sql
SELECT /*+ parallel(RPT,4)*/
RPT.RAHMENVERTRAG_ID
,RPT.SV_ID TARIF_ID
,RPT.PARTNER_ID_CARMEN T_MOBILE_KUNDENNUMMER
,RPT.KUNDENKONTO,
RPT.MSISDN,
RPT.GEPLANT_KUEND,
RPT.BINDEFRIST,
TO_CHAR(RPT.VERTRAGSBEGINN,'DD.MM.YYYY') VERTRAGSBEGINN,
RPT.VERTRAGSBINDUNG,
RPT.DWH_TARIFGR_TEXT
,CAST (LISTAGG(A.BPR_ID,',') WITHIN GROUP( ORDER BY RPT.RAHMENVERTRAG_ID) AS VARCHAR2(500)) BASISPRODUKTE
FROM RPT$TA_S_D1_VERTRAG RPT
,(SELECT /*+ parallel(BPR,4) parallel(OPT,4)*/
	BPR.CNTRCT_ID, BPR.BPR_ID
  FROM SOF$TA_BPR_OPTIONEN BPR,SOF$VI_L_OPTIONZUORDNUNG OPT
    WHERE BPR.BPR_ID = OPT.OPTION_ID ) A
WHERE RPT.VERTRAG_ID_CARMEN=A.CNTRCT_ID
GROUP BY RPT.RAHMENVERTRAG_ID
,RPT.VERTRAG_ID_CARMEN
,RPT.SV_ID
,RPT.PARTNER_ID_CARMEN
,RPT.KUNDENKONTO,
RPT.MSISDN,
RPT.GEPLANT_KUEND,
RPT.BINDEFRIST,
RPT.VERTRAGSBEGINN,
RPT.VERTRAGSBINDUNG,
RPT.DWH_TARIFGR_TEXT,RPT.VERTRAGSSTATUS
ORDER BY RPT.VERTRAG_ID_CARMEN;
```

**Translated BigQuery SQL:**

```sql
SELECT
  RPT.RAHMENVERTRAG_ID,
  RPT.SV_ID AS TARIF_ID,
  RPT.PARTNER_ID_CARMEN AS T_MOBILE_KUNDENNUMMER,
  RPT.KUNDENKONTO,
  RPT.MSISDN,
  RPT.GEPLANT_KUEND,
  RPT.BINDEFRIST,
  FORMAT_DATE('%d.%m.%Y', DATE(RPT.VERTRAGSBEGINN)) AS VERTRAGSBEGINN,
  RPT.VERTRAGSBINDUNG,
  RPT.DWH_TARIFGR_TEXT,
  SUBSTR(
    CAST(
      STRING_AGG(CAST(A.BPR_ID AS STRING), ',' ORDER BY RPT.RAHMENVERTRAG_ID)
      AS STRING
    ),
    1,
    500
  ) AS BASISPRODUKTE
FROM `your_project.your_dataset.RPT_TA_S_D1_VERTRAG` AS RPT -- Assuming tables are migrated and prefixed
JOIN (
  SELECT
    BPR.CNTRCT_ID,
    BPR.BPR_ID
  FROM `your_project.your_dataset.SOF_TA_BPR_OPTIONEN` AS BPR
  JOIN `your_project.your_dataset.SOF_VI_L_OPTIONZUORDNUNG` AS OPT
    ON BPR.BPR_ID = OPT.OPTION_ID
) AS A
  ON RPT.VERTRAG_ID_CARMEN = A.CNTRCT_ID
GROUP BY
  RPT.RAHMENVERTRAG_ID,
  RPT.VERTRAG_ID_CARMEN,
  RPT.SV_ID,
  RPT.PARTNER_ID_CARMEN,
  RPT.KUNDENKONTO,
  RPT.MSISDN,
  RPT.GEPLANT_KUEND,
  RPT.BINDEFRIST,
  RPT.VERTRAGSBEGINN,
  RPT.VERTRAGSBINDUNG,
  RPT.DWH_TARIFGR_TEXT,
  RPT.VERTRAGSSTATUS
ORDER BY
  RPT.VERTRAG_ID_CARMEN;
```

**Key Transformations:**
*   `TO_CHAR(RPT.VERTRAGSBEGINN,'DD.MM.YYYY')` becomes `FORMAT_DATE('%d.%m.%Y', DATE(RPT.VERTRAGSBEGINN))`.
*   `LISTAGG(A.BPR_ID,',') WITHIN GROUP( ORDER BY RPT.RAHMENVERTRAG_ID) AS VARCHAR2(500)` becomes `SUBSTR(CAST(STRING_AGG(CAST(A.BPR_ID AS STRING), ',' ORDER BY RPT.RAHMENVERTRAG_ID) AS STRING), 1, 500)`.
*   Oracle parallel hints (`/*+ parallel(...)*/`) are removed as BigQuery automatically handles query parallelization.
*   Table names are adapted to BigQuery's `project.dataset.table` naming convention, assuming `RPT$TA_S_D1_VERTRAG` becomes `your_project.your_dataset.RPT_TA_S_D1_VERTRAG` and similar for other tables.

## 6. External Dependencies

| Original External System | Reference in Source | Proposed GCP Replacement | Notes |
| :----------------------- | :------------------ | :----------------------- | :---- |
| **Oracle Database** | `RPT$TA_S_D1_VERTRAG`, `SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG` (tables read by SQL) | BigQuery Tables | These source tables must be migrated to BigQuery before this job can run. |
| **SFTP Server** | `<SFTP>` block in `h_exis_apt_bestandsdaten.var` (Host, Port, User, Dir) | Cloud Storage + Cloud Function / Cloud Run with custom SFTP client, or Storage Transfer Service | The SFTP target details will be stored securely (e.g., in Secret Manager) and used by an Airflow task or a dedicated service for file transfer. |
| **Host `DWHDWH5P`** | UC4 `HostDst` attribute | Dataproc Cluster or BigQuery Compute | Represents the execution environment; will be abstracted by Cloud Composer and BigQuery. |
| **Login `DW.UNIX.ISTNS`** | UC4 `Login` attribute | Google Cloud Service Account | Access to Google Cloud resources will be managed via a service account associated with the Airflow environment. |
| **Custom Executable `r_exis_v2`** | Invoked by UC4 job | PySpark on Dataproc Serverless, or a custom Cloud Function/Cloud Run application | The exact functionality of `r_exis_v2` needs to be fully understood to determine the best migration target. The current design assumes a PySpark rewrite as a placeholder. If it's a simple shell script, it could also be run via a `BashOperator` in Airflow, but for complex ETL logic, Dataproc or Cloud Run is preferred. |
| **Shell includes (`DW.HOLE_PFAD`, `DW.LESE_LOG`)** | Referenced in UC4 script | Airflow Python functions or operators, or incorporated into target scripts | These helper scripts/functions need to be analyzed and rewritten as appropriate for the Airflow environment. |

## 7. Unresolved / Risks

*   **UC4 Schedule:** The specific schedule of the `EXIS_SD_APT_BESTANDS` job could not be determined from the provided UC4 XML. This needs to be identified from external UC4 scheduling objects (`EVNT_TIME` or `JOBP`) to configure the Airflow DAG schedule correctly.
*   **`r_exis_v2` functionality:** The exact behavior and dependencies of the custom executable `r_exis_v2` are not fully known. The current design assumes it performs ETL logic that can be rewritten in PySpark or similar. If it has complex external calls or proprietary logic, this may increase migration complexity.
*   **`nawk` Post-processing Logic:** The `nawk` command (`nawk '{print $0} END {print "X|<DESTINATION_FILE>|<FROM YYYYMMDD>|" NR "|V_S_Bestandsreport|<SYSDATE YYYYMMDD>"}'`) needs to be carefully translated. While BigQuery can generate CSVs, adding a custom footer/header with dynamic values might require an additional step (e.g., Cloud Function) after BigQuery export.
*   **Oracle Tables to BigQuery Mapping:** The assumption is that the Oracle source tables will be migrated to BigQuery. The exact BigQuery dataset and table names (`your_project.your_dataset.table_name`) need to be finalized.
*   **`h_exis_apt_bestandsdaten.var` 'retire' bucket:** This configuration file is marked for `retire`, indicating it will not be directly migrated but its functionality will be absorbed into the new GCP components. This aligns with the proposed approach of translating its various sections into Airflow, BigQuery, and other Cloud services.

## 8. Build Plan

This section outlines the ordered steps and target languages for building the migrated components.

1.  **Migrate Oracle Source Tables to BigQuery:**
    *   **Action:** Migrate `RPT$TA_S_D1_VERTRAG`, `SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG` to BigQuery.
    *   **Language:** DDL for BigQuery.
    *   **Notes:** This is a prerequisite.
2.  **Develop BigQuery SQL for Data Extraction:**
    *   **Action:** Create a BigQuery SQL script for the core data extraction and transformation logic.
    *   **Language:** BigQuery SQL.
    *   **Source:** `d_exis_apt_bestandsdaten.sql`
    *   **Output:** BigQuery SQL script (`d_exis_apt_bestandsdaten.bqsql`)
3.  **Implement Custom Post-processing (if needed):**
    *   **Action:** Develop a Cloud Function or Cloud Run service to handle the `nawk` logic if not feasible within BigQuery export or the SFTP transfer.
    *   **Language:** Python (or Node.js, Go, etc., for Cloud Functions/Run).
    *   **Source:** `nawk` command in `h_exis_apt_bestandsdaten.var`
    *   **Output:** `nawk_postprocessor.py` (Cloud Function/Run code)
4.  **Develop SFTP Distribution Service:**
    *   **Action:** Implement the SFTP transfer mechanism. This could be an Airflow operator, or a separate Cloud Function/Cloud Run if more control or specific libraries are needed.
    *   **Language:** Python.
    *   **Source:** `<SFTP>` block in `h_exis_apt_bestandsdaten.var`
    *   **Output:** `sftp_transfer_operator.py` (Airflow custom operator) or `sftp_uploader.py` (Cloud Function/Run code)
5.  **Design and Implement Airflow DAG:**
    *   **Action:** Create an Airflow DAG to orchestrate the entire process.
    *   **Language:** Python.
    *   **Source:** `DW.DWH_EXIS_SD_APT_BESTANDS.xml` (UC4 job structure), `h_exis_apt_bestandsdaten.var` (flow definition).
    *   **Output:** `dw_dwh_exis_sd_apt_bestands_dag.py` (Airflow DAG file)
    *   **Components:**
        *   `BigQueryInsertJobOperator` to run the BigQuery SQL.
        *   `GCSToGCSOperator` if temporary file manipulation is needed before SFTP.
        *   `CloudFunctionInvokeOperator` or `CloudRunOperator` to trigger custom post-processing/SFTP (if implemented as separate services).
        *   Custom PythonOperator for in-DAG post-processing or SFTP.
        *   Setup Airflow Connections for SFTP credentials.