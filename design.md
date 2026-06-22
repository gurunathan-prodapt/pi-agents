# Migration Design — EXIS_SD_APT_RABATT

## 1. Purpose & Scope
The purpose of the `EXIS_SD_APT_RABATT` job is to extract discount-related data from an Oracle database, transform it, and export it as a compressed CSV file. This file is then distributed via SFTP and moved to a local archive directory. The job is orchestrated by UC4 (Automic Workload Automation). The scope of this migration is to re-implement this ETL process on Google Cloud Platform, using BigQuery for data storage and transformations, and Airflow for orchestration.

## 2. Source Inventory
This job comprises three main components:

*   **`vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_TAEGLICH_JP/DW.DWH_EXIS_SD_APT_RABATT.xml`**
    *   **Technology:** UC4/Automic Workload Automation (UNIX Job type)
    *   **Summary:** Defines the scheduling and execution of the data export. It invokes a custom shell script `r_exis_v2` with a configuration file.
    *   **Complexity Tier:** `medium`
    *   **Automation Bucket:** `semi_auto`

*   **`vobs/dw_source/isdwh/exporter/apt/cfg/h_exis_apt_rabattdaten.var`**
    *   **Technology:** Custom ETL Framework Configuration (Shell script/INI-like format)
    *   **Summary:** Configuration file for the ETL exporter. It specifies the SQL source, output destination (`DWHM_APT_RABATTREPORT_<SYSDATE YYYYMMDDHH24MISS>.csv.gz`), post-processing steps (nawk, gzip), and distribution methods (SFTP, local move).
    *   **Complexity Tier:** `medium`
    *   **Automation Bucket:** `retire`

*   **`vobs/dw_source/isdwh/exporter/apt/sql/d_exis_apt_rabattdaten.sql`**
    *   **Technology:** Oracle PL/SQL
    *   **Summary:** The core SQL script that selects and transforms discount data from multiple Oracle tables, aggregates `BPR_ID` using `LISTAGG`, and prepares the data for CSV export.
    *   **Complexity Tier:** `medium`
    *   **Automation Bucket:** `retire`

## 3. Target Architecture
The target architecture will leverage Google Cloud Platform services:
*   **Orchestration:** Apache Airflow on Cloud Composer.
*   **Data Storage:** BigQuery datasets for raw and transformed data. Source Oracle tables will be ingested into BigQuery.
*   **Transformation:** BigQuery SQL for data transformations.
*   **Data Export:** Google Cloud Storage for temporary CSV output files, then Cloud Data Transfer or similar for SFTP distribution.
*   **File Archiving:** Google Cloud Storage for archived CSV files.

## 4. Data Flow & Lineage

The original job flow is as follows:
1.  **UC4 Job (`DW.DWH_EXIS_SD_APT_RABATT.xml`):** Triggers the execution.
2.  **Custom ETL Framework (`r_exis_v2` using `h_exis_apt_rabattdaten.var`):**
    *   Reads configuration from `h_exis_apt_rabattdaten.var`.
    *   Executes the SQL query defined in `d_exis_apt_rabattdaten.sql`.
3.  **Oracle SQL Query (`d_exis_apt_rabattdaten.sql`):**
    *   Reads data from source Oracle tables:
        *   `RPT$TA_S_D1_VERTRAG`
        *   `RPT$TA_S_D1_DISCOUNT_RR`
        *   `SOF$TA_BPR_OPTIONEN`
        *   `SOF$VI_L_OPTIONZUORDNUNG`
    *   Performs joins and aggregations.
    *   Outputs a dataset.
4.  **Post-processing (defined in `h_exis_apt_rabattdaten.var`):**
    *   `nawk`: Performs text manipulation on the output.
    *   `gzip`: Compresses the output file.
5.  **Distribution (defined in `h_exis_apt_rabattdaten.var`):**
    *   **SFTP:** Transfers the compressed CSV file to an external system.
    *   **MOVE:** Moves a copy of the file to a local archive directory (`$DW_DIR_EXP_APT/store`).

**Target Data Flow:**
1.  **Airflow DAG (`dw_dwh_exis_sd_apt_rabatt`):** Orchestrates the entire process.
2.  **Data Ingestion:** Source Oracle tables (`RPT$TA_S_D1_VERTRAG`, `RPT$TA_S_D1_DISCOUNT_RR`, `SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG`) are continuously or periodically ingested into BigQuery (e.g., using Datastream or Fivetran). Let's assume they exist as `oracle_source.RPT_TA_S_D1_VERTRAG`, `oracle_source.RPT_TA_S_D1_DISCOUNT_RR`, `oracle_source.SOF_TA_BPR_OPTIONEN`, `oracle_source.SOF_VI_L_OPTIONZUORDNUNG`.
3.  **BigQuery Transformation Task:** An Airflow task executes a BigQuery SQL query (derived from `d_exis_apt_rabattdaten.sql`) to transform the ingested data. The output is a temporary BigQuery table.
4.  **BigQuery Export Task:** An Airflow task exports the data from the temporary BigQuery table to a compressed CSV file in a Google Cloud Storage bucket (e.g., `gs://export-bucket/DWHM_APT_RABATTREPORT_<YYYYMMDDHHMMSS>.csv.gz`).
5.  **Post-processing (GCS to GCS / Cloud Functions):** The `nawk` logic might be integrated directly into the BigQuery SQL transformation or handled by a Cloud Function triggered by the GCS export. The `gzip` compression is handled during the BigQuery export to GCS.
6.  **SFTP Distribution Task:** An Airflow task (e.g., using SFTP hook or Cloud Functions with external connectivity) transfers the compressed CSV from GCS to the external SFTP target.
7.  **GCS Archiving Task:** The exported CSV file in GCS serves as the archive.

## 5. Transformation Logic
The core transformation logic resides in `d_exis_apt_rabattdaten.sql`. This will be translated into BigQuery Standard SQL.

**Original Oracle SQL:**
```sql
SELECT /*+ PARALLEL(4)*/
    RAHMENVERTRAG_ID,
    CNTRCT_TEMPLATE_ID TARIF_ID,
    DWH_TARIFGR_TEXT,
    RABATTIERTE_RECH_POS,
    DISC_INVOICE_ITEM_ID RABATTIERTE_RECH_POS_ID,
    RABATTHOEHE,
    CAST (LISTAGG(BPR_ID,',') WITHIN GROUP( ORDER BY BPR_ID) AS VARCHAR2(500)) BASISPRODUKTE
FROM (
    SELECT /*+ parallel(RPT, 4) parallel(DISC, 4) parallel(BPR, 4) parallel(OPT, 4)  USE_HASH(RPT,DISC) USE_HASH(RPT,BPR) USE_HASH(BPR,OPT)*/
        DISTINCT
        RPT.RAHMENVERTRAG_ID
        ,RPT.DWH_TARIFGR_TEXT
        ,DISC.CNTRCT_TEMPLATE_ID
        ,DISC.RABATTIERTE_RECH_POS
        ,DISC.DISC_INVOICE_ITEM_ID
        ,DISC.RABATTHOEHE
        ,BPR.BPR_ID
    FROM RPT$TA_S_D1_VERTRAG RPT
        ,RPT$TA_S_D1_DISCOUNT_RR DISC
      ,SOF$TA_BPR_OPTIONEN BPR
      ,SOF$VI_L_OPTIONZUORDNUNG OPT
    WHERE
        RPT.RAHMENVERTRAG_ID=DISC.CONTRACT_NUMBER
      AND RPT.SV_ID=DISC.CNTRCT_TEMPLATE_ID
      AND RPT.VERTRAG_ID_CARMEN=BPR.CNTRCT_ID
      AND BPR.BPR_ID = OPT.OPTION_ID
    )
  GROUP BY RAHMENVERTRAG_ID,
    CNTRCT_TEMPLATE_ID,
    DWH_TARIFGR_TEXT,
    RABATTIERTE_RECH_POS,
    DISC_INVOICE_ITEM_ID,
    RABATTHOEHE
;
```

**BigQuery SQL Translation Considerations:**
*   **Oracle Hints (`/*+ PARALLEL(4)*/`, `USE_HASH`):** These are Oracle-specific performance hints and will be removed. BigQuery's execution engine automatically optimizes queries.
*   **`LISTAGG`:** BigQuery equivalent is `STRING_AGG`.
*   **`VARCHAR2(500)`:** BigQuery uses `STRING` type.
*   **Table Naming:** `$TA` and `$VI` prefixes usually denote views or tables in Oracle. These will be mapped to appropriate BigQuery table names, likely within a `oracle_source` dataset.
    *   `RPT$TA_S_D1_VERTRAG` -> `oracle_source.RPT_TA_S_D1_VERTRAG`
    *   `RPT$TA_S_D1_DISCOUNT_RR` -> `oracle_source.RPT_TA_S_D1_DISCOUNT_RR`
    *   `SOF$TA_BPR_OPTIONEN` -> `oracle_source.SOF_TA_BPR_OPTIONEN`
    *   `SOF$VI_L_OPTIONZUORDNUNG` -> `oracle_source.SOF_VI_L_OPTIONZUORDNUNG`
*   **Implicit Joins:** The Oracle query uses implicit joins (comma-separated tables in `FROM` clause). These will be converted to explicit `JOIN` clauses in BigQuery for clarity and best practice.
*   **Column Aliases:** `TARIF_ID` and `RABATTIERTE_RECH_POS_ID` will be preserved.
*   **`nawk` Post-processing:** The `nawk` script modifies the output by prepending "D|" to each line and adding a footer line. This logic needs to be integrated. It can be done by modifying the SELECT statement in BigQuery or by a small Cloud Function/Python script invoked by Airflow after export. For simplicity in the BigQuery SQL, the data selection will focus on the core transformation. The `nawk` logic can be implemented as a separate step or part of the export process.

**Example BigQuery SQL (core logic):**
```sql
SELECT
    t.RAHMENVERTRAG_ID,
    t.TARIF_ID,
    t.DWH_TARIFGR_TEXT,
    t.RABATTIERTE_RECH_POS,
    t.RABATTIERTE_RECH_POS_ID,
    t.RABATTHOEHE,
    STRING_AGG(t.BPR_ID, ',' ORDER BY t.BPR_ID) AS BASISPRODUKTE
FROM (
    SELECT DISTINCT
        RPT.RAHMENVERTRAG_ID,
        RPT.DWH_TARIFGR_TEXT,
        DISC.CNTRCT_TEMPLATE_ID AS TARIF_ID,
        DISC.RABATTIERTE_RECH_POS,
        DISC.DISC_INVOICE_ITEM_ID AS RABATTIERTE_RECH_POS_ID,
        DISC.RABATTHOEHE,
        BPR.BPR_ID
    FROM
        `oracle_source.RPT_TA_S_D1_VERTRAG` AS RPT
    INNER JOIN
        `oracle_source.RPT_TA_S_D1_DISCOUNT_RR` AS DISC
        ON RPT.RAHMENVERTRAG_ID = DISC.CONTRACT_NUMBER
        AND RPT.SV_ID = DISC.CNTRCT_TEMPLATE_ID
    INNER JOIN
        `oracle_source.SOF_TA_BPR_OPTIONEN` AS BPR
        ON RPT.VERTRAG_ID_CARMEN = BPR.CNTRCT_ID
    INNER JOIN
        `oracle_source.SOF_VI_L_OPTIONZUORDNUNG` AS OPT
        ON BPR.BPR_ID = OPT.OPTION_ID
) AS t
GROUP BY
    RAHMENVERTRAG_ID,
    TARIF_ID,
    DWH_TARIFGR_TEXT,
    RABATTIERTE_RECH_POS,
    RABATTIERTE_RECH_POS_ID,
    RABATTHOEHE;
```

## 6. External Dependencies
*   **Source Oracle Database:** Provides the input tables (`RPT$TA_S_D1_VERTRAG`, `RPT$TA_S_D1_DISCOUNT_RR`, `SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG`).
    *   **Replacement:** Data will be ingested into BigQuery (e.g., `oracle_source` dataset).
*   **SFTP Server:** External system where the compressed CSV file is distributed.
    *   **Replacement:** An Airflow task will use an SFTP connection or a dedicated Cloud Function/Cloud Run service to transfer the file from Google Cloud Storage to the target SFTP server.
*   **Local File System (`$DW_DIR_EXP_APT/store`):** For archiving the output file.
    *   **Replacement:** Google Cloud Storage bucket (e.g., `gs://export-archive-bucket/`).

## 7. Unresolved / Risks
*   **UC4 Workflow Context:** The `uc4_to_airflow_dag_design` tool indicated that no `EVNT_TIME`, `JOBP`, or `JSCH` objects were provided. This means the complete scheduling and inter-job dependencies are not fully known. Manual investigation of the broader UC4 environment might be required to accurately define the Airflow DAG's schedule and upstream dependencies.
*   **Custom ETL Framework (`r_exis_v2`):** The exact functionality of `r_exis_v2` beyond simply executing the script with the configuration is not fully detailed. It seems to be a wrapper. This will need to be replaced with a Python operator in Airflow that coordinates the BigQuery transformation, GCS export, and SFTP transfer.
*   **`nawk` Logic:** The `nawk` post-processing step (`nawk '{print $0} END {print "X|<DESTINATION_FILE>|<FROM YYYYMMDD>|" NR "|V_S_Rabattreport|<SYSDATE YYYYMMDD>"}'`) needs to be replicated. This can be integrated into the BigQuery `SELECT` statement using `UNION ALL` for the footer and `CONCAT` for the prefix, or handled by a separate Python script / Cloud Function after data export to GCS. The BigQuery `SELECT` approach is preferred for simplicity if feasible.
*   **`SYSDATE YYYYMMDDHH24MISS`:** The timestamp for the output filename needs to be dynamically generated in Airflow.
*   **`DW_DIR_EXP_APT`, `DW_DIR_ROOT`, `DW_APT_SFTP_PORT`, `DW_APT_SFTP_USER`, `DW_APT_SFTP_SERVER`, `DW_APT_SFTP_DIR`:** These are environment variables or configuration parameters that need to be mapped to Airflow Variables, Connections, or hardcoded if stable.

## 8. Build Plan
1.  **BigQuery Schema Creation:**
    *   Create `oracle_source` dataset in BigQuery.
    *   Define schemas for the four source tables (`RPT_TA_S_D1_VERTRAG`, `RPT_TA_S_D1_DISCOUNT_RR`, `SOF_TA_BPR_OPTIONEN`, `SOF_VI_L_OPTIONZUORDNUNG`) based on their Oracle definitions.
    *   **(Language: DDL)**

2.  **BigQuery Transformation SQL (`dwh_exis_sd_apt_rabatt_transform.sql`):**
    *   Translate `d_exis_apt_rabattdaten.sql` into BigQuery Standard SQL, incorporating `STRING_AGG` for `LISTAGG` and explicit `JOIN` clauses.
    *   Address the `nawk` prefix and footer logic within the BigQuery SQL if possible, or design a separate Python script.
    *   This SQL will create a temporary table or CTE that holds the final export data.
    *   **(Language: BigQuery SQL)**

3.  **Airflow DAG (`dw_dwh_exis_sd_apt_rabatt_dag.py`):**
    *   **DAG Definition:** Create a new Airflow DAG with `dag_id='dw_dwh_exis_sd_apt_rabatt'`.
    *   **Parameters:** Define Airflow Variables for SFTP connection details and GCS bucket names.
    *   **Task 1: `execute_bq_transformation`:**
        *   Uses `BigQueryOperator` to run `dwh_exis_sd_apt_rabatt_transform.sql`.
        *   Outputs data to a temporary BigQuery table.
    *   **Task 2: `export_to_gcs`:**
        *   Uses `BigQueryToGCSOperator` to export the temporary BigQuery table to a compressed CSV file in a GCS bucket (e.g., `gs://export-bucket/DWHM_APT_RABATTREPORT_{{ ds_nodash }}.csv.gz`).
    *   **Task 3: `sftp_transfer`:**
        *   Uses `SFTPOperator` (or a custom PythonOperator/Cloud Function) to transfer the CSV from GCS to the external SFTP server, using parameters from Airflow Connections/Variables.
    *   **Task 4: `archive_gcs_file`:**
        *   A simple task (e.g., `GCSToGCSOperator` or just retaining the file in the export bucket if it doubles as archive) to ensure the file is archived in GCS.
    *   **Dependencies:** `execute_bq_transformation >> export_to_gcs >> sftp_transfer`
    *   **(Language: Python for Airflow DAG)**

4.  **SFTP Connection Setup:**
    *   Create an Airflow SFTP Connection with necessary credentials and host details.

5.  **Testing:**
    *   Unit tests for BigQuery SQL.
    *   Integration tests for the Airflow DAG, including BigQuery transformation, GCS export, and SFTP transfer.