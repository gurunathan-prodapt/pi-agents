# Migration Design — EXIS_SD_APT_NNA_VOIC

## 1. Purpose & Scope

The original job, `EXIS_SD_APT_NNA_VOIC`, is an ETL workflow orchestrated by a UC4 JOBS_UNIX object. Its primary purpose is to export telephone system master data, specifically voice-related data, from Oracle Data Warehouse (DWH) tables. This data is extracted, transformed, and then exported to a gzipped CSV file, which is subsequently distributed via SFTP to a target system. The migration aims to re-implement this functionality on the BigQuery platform, orchestrated by Apache Airflow.

## 2. Source Inventory

The `EXIS_SD_APT_NNA_VOIC` job comprises three key components:

*   **`vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_MONATLICH_JP/DW.DWH_EXIS_SD_APT_NNA_VOIC.xml`**
    *   **Technology:** UC4/Automic (UC4 JOBS_UNIX object)
    *   **Category:** UC4 Scheduler
    *   **Complexity Tier:** Medium
    *   **Migration Bucket:** Semi-automated
    *   **Description:** This is the main orchestrator, defining the job for exporting telephone system master data. It executes a shell script that, in turn, handles the data extraction and distribution.

*   **`vobs/dw_source/isdwh/exporter/apt/cfg/h_exis_apt_nna_voice.var`**
    *   **Technology:** Other ETL Config (Configuration file)
    *   **Category:** Config
    *   **Complexity Tier:** Medium
    *   **Migration Bucket:** Retire
    *   **Description:** A configuration file for the ETL job, specifying job metadata, separator, destination, post-processing steps (nawk, gzip), and SFTP distribution parameters. It includes the SQL script for data extraction.

*   **`vobs/dw_source/isdwh/exporter/apt/sql/d_exis_apt_nna_voice.sql`**
    *   **Technology:** Oracle PL/SQL (SQL script)
    *   **Category:** SQL
    *   **Complexity Tier:** Medium
    *   **Migration Bucket:** Retire
    *   **Description:** This SQL script performs the core data extraction. It joins several DWH tables, applies filters, and reformats columns to produce voice-related data for the CSV export.

## 3. Target Architecture

The target architecture will leverage Google Cloud Platform services:

*   **Orchestration:** Apache Airflow on Cloud Composer will replace UC4 for job scheduling and workflow management.
*   **Data Storage & Processing:** Google BigQuery will serve as the primary data warehouse, replacing the Oracle DWH for the exported data.
*   **File Storage:** Google Cloud Storage (GCS) will be used for temporary storage of exported CSV files before distribution.
*   **External Data Transfer:** Cloud Functions or a similar mechanism will be employed to handle SFTP distribution, replacing the legacy SFTP process.

## 4. Data Flow & Lineage

The current data flow is as follows:

1.  The UC4 job `DW.DWH_EXIS_SD_APT_NNA_VOIC.xml` is triggered.
2.  It executes a shell script (implied `r_exis_v2`) which reads the configuration from `h_exis_apt_nna_voice.var`.
3.  The `.var` file, in turn, includes and executes `d_exis_apt_nna_voice.sql`.
4.  `d_exis_apt_nna_voice.sql` reads data from the following Oracle DWH tables:
    *   `DWH$VI_L_MAP_FA_TARIF`
    *   `BL_D_TARIF`
    *   `DWH$VI_C_VERTRAG`
    *   `DWH$VI_F_NNV_TVD_12_MONATE`
    *   `DWH$VI_L_TVD_LEISTUNGSKLASSE`
5.  The query results are processed by `nawk` (for adding a header/trailer) and `gzip` (for compression).
6.  The compressed CSV file is then distributed to an external system via SFTP as specified in the `.var` file.

The target BigQuery/Airflow data flow will be:

1.  An Airflow DAG `dw_dwh_exis_sd_apt_nna_voic` is scheduled to run (e.g., monthly).
2.  A `BigQueryExecuteQueryOperator` task within the DAG will execute a BigQuery SQL query.
3.  This query will read from the migrated BigQuery equivalent tables of the original Oracle DWH tables.
4.  The query will transform the data and load it into a new BigQuery table: `your_project.your_dataset.DWHM_APT_NNA_Voice`.
5.  Following the BigQuery load, a separate Airflow task (e.g., `BashOperator` calling `gsutil` and a custom script, or a `PythonOperator` interacting with Cloud Storage and a secure external transfer service) will:
    *   Export the data from `your_project.your_dataset.DWHM_APT_NNA_Voice` to a gzipped CSV file in a Cloud Storage bucket.
    *   Apply any necessary post-processing (equivalent to original `nawk` logic) to the exported CSV.
    *   Distribute the gzipped CSV file to the external target system via a secure transfer mechanism (e.g., SFTP through a Cloud Function or managed transfer service).

## 5. Transformation Logic

The core transformation logic from `d_exis_apt_nna_voice.sql` will be directly translated to BigQuery SQL. Key aspects include:

*   **Source Tables (Oracle -> BigQuery):**
    *   `DWH$VI_L_MAP_FA_TARIF` -> `your_project.your_dataset.DWH_VI_L_MAP_FA_TARIF`
    *   `BL_D_TARIF` -> `your_project.your_dataset.BL_D_TARIF`
    *   `DWH$VI_C_VERTRAG` -> `your_project.your_dataset.DWH_VI_C_VERTRAG`
    *   `DWH$VI_F_NNV_TVD_12_MONATE` -> `your_project.your_dataset.DWH_VI_F_NNV_TVD_12_MONATE`
    *   `DWH$VI_L_TVD_LEISTUNGSKLASSE` -> `your_project.your_dataset.DWH_VI_L_TVD_LEISTUNGSKLASSE`
*   **Joins:** The existing join conditions between these tables will be preserved.
    *   `TAR.DWH_TARIF_ID = VER.DWH_TARIF_ID`
    *   `VER.DWH_VERTRAG_ID = NNA.DWH_VERTRAG_ID`
    *   `NNA.LEISTUNGSKLASSE_ID = TVD.LEISTUNGSKLASSE_ID`
*   **Filtering:**
    *   `NNA.RAHMENVERTRAG IS NOT NULL` remains as is.
    *   `NNA.MONATS_ID = TO_NUMBER(<FROM YYYYMM>)` will be replaced with `NNA.MONATS_ID = CAST(FORMAT_DATE('%Y%m', CURRENT_DATE()) AS INT64)` assuming monthly execution for current data. If a specific historical month is needed, this parameterization must be handled by Airflow variables or DAG configuration.
    *   `TAR.GUELTIG_BIS = TO_DATE('47121231','YYYYMMDD')` will be translated to `TAR.GUELTIG_BIS = DATE '4712-12-31'`.
    *   The complex `TVD.LEISTUNGSKLASSE_ID` filtering logic involving `LENGTH`, `TRIM`, `TRUNC` will be adapted to BigQuery's SQL functions.
*   **Column Transformations:**
    *   `TARIF`: Oracle `(TAR.MP_MARKTPRODUKT_BEZ||','|| TAR.MP_EG_JN_BEZ||','|| TAR.MP_GENERATION_BEZ)` translates to BigQuery `CONCAT(TAR.MP_MARKTPRODUKT_BEZ, ',', TAR.MP_EG_JN_BEZ, ',', TAR.MP_GENERATION_BEZ)`.
    *   `DAUER_MIN` and `RBETRAG_VBUD_NETTO_EURO`: `ROUND(NNA.DAUER_SEK/60,2)` and `ROUND(NNA.RBETRAG_VBUD_NETTO_CENT/100,2)` will be directly translated.

## 6. External Dependencies

*   **Oracle Database:** The source Oracle DWH tables will be migrated to BigQuery datasets and tables.
*   **SFTP Server:** The external SFTP server used for distribution remains a target. The distribution mechanism will need to be re-engineered using Google Cloud services (e.g., Cloud Storage to stage the file, followed by a Cloud Function or an Airflow task utilizing `sftp` libraries or a managed service to push the file).

## 7. Unresolved / Risks

*   **Post-processing Logic:** The `nawk` command (`nawk '{print $0} END {print "X|<DESTINATION_FILE>|<FROM YYYYMMDD>|" NR "|V_F_NNA_Voice|<SYSDATE YYYYMMDD>"}'`) and `gzip` operation from the `h_exis_apt_nna_voice.var` file are not directly translated by the `design_doc` tool. This logic will need to be implemented as a separate Airflow task, likely a PythonOperator that reads the BigQuery table, performs the row count and metadata injection, and then compresses the output, or by using BigQuery's export functionality with appropriate templating for the metadata.
*   **Parameterization:** The original job used `<PARAM_1>` and `<FROM YYYYMM>`. The generated BigQuery SQL defaults `MONATS_ID` to the current month. The exact mechanism for historical or arbitrary month processing needs to be confirmed and implemented using Airflow's templating or DAG parameters.
*   **Migration Bucket "Retire" for config and SQL:** The `h_exis_apt_nna_voice.var` and `d_exis_apt_nna_voice.sql` files are flagged for "retire". This indicates they will not be directly migrated but their functionality will be absorbed into the new BigQuery/Airflow solution. This aligns with the generated output which consolidates the logic into a single SQL script within an Airflow DAG.
*   **SFTP Security and Credentials:** Secure handling of SFTP credentials for the distribution step will be critical in the new cloud environment (e.g., using Secret Manager).

## 8. Build Plan

The migration will involve building the following components:

1.  **Airflow DAG (`dw_dwh_exis_sd_apt_nna_voic.py`)**
    *   **Language:** Python
    *   **Purpose:** Orchestrate the entire data export process, including data extraction, transformation, BigQuery table loading, and downstream distribution.
    *   **Content:**
        *   Standard Airflow DAG definition (`DAG`, `default_args`).
        *   A `BigQueryExecuteQueryOperator` task named `process_voice_export` to execute the core SQL logic.
        *   The embedded BigQuery SQL query as described in Section 5.
        *   (To be added) A task (e.g., `BashOperator` or `PythonOperator`) to export data from the `DWHM_APT_NNA_Voice` BigQuery table to a gzipped CSV in Cloud Storage.
        *   (To be added) A task (e.g., `PythonOperator` interacting with an SFTP client or a Cloud Function trigger) to handle the SFTP distribution to the external target, including the `nawk`-like post-processing.
    *   **Example (Core SQL part embedded):**
        ```python
        from datetime import timedelta
        from airflow import DAG
        from airflow.operators.python import PythonOperator
        from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
        from airflow.utils.dates import days_ago

        default_args = {
            "owner": "airflow",
            "depends_on_past": False,
            "start_date": days_ago(1),
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        }

        with DAG(
            dag_id="dw_dwh_exis_sd_apt_nna_voic",
            default_args=default_args,
            description="Data export of telephone system masterdata.",
            schedule_interval=None,
            catchup=False,
            tags=["bigquery", "export", "telephone", "masterdata"],
        ) as dag:

            def build_bigquery_sql():
                sql = """
                CREATE TABLE IF NOT EXISTS `your_project.your_dataset.DWHM_APT_NNA_Voice`
                OPTIONS(
                  description = 'Data export of telephone system masterdata.'
                )
                AS
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
                  ROUND(NNA.DAUER_SEK / 60, 2) AS DAUER_MIN,
                  ROUND(NNA.RBETRAG_VBUD_NETTO_CENT / 100, 2) AS RBETRAG_VBUD_NETTO_EURO,
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
                  FROM `your_project.your_dataset.DWH_VI_L_MAP_FA_TARIF` AS TRF
                  JOIN `your_project.your_dataset.BL_D_TARIF` AS D
                    ON TRF.TARIF_ID = D.TARIF_ID
                ) AS TAR
                JOIN `your_project.your_dataset.DWH_VI_C_VERTRAG` AS VER
                  ON TAR.DWH_TARIF_ID = VER.DWH_TARIF_ID
                JOIN `your_project.your_dataset.DWH_VI_F_NNV_TVD_12_MONATE` AS NNA
                  ON VER.DWH_VERTRAG_ID = NNA.DWH_VERTRAG_ID
                JOIN `your_project.your_dataset.DWH_VI_L_TVD_LEISTUNGSKLASSE` AS TVD
                  ON NNA.LEISTUNGSKLASSE_ID = TVD.LEISTUNGSKLASSE_ID
                WHERE NNA.RAHMENVERTRAG IS NOT NULL
                  AND NNA.MONATS_ID = CAST(FORMAT_DATE('%Y%m', CURRENT_DATE()) AS INT64)
                  AND TAR.GUELTIG_BIS = DATE '4712-12-31'
                  AND (
                    (TVD.LEISTUNGSKLASSEGR_ID = 1 AND (TVD.LEISTUNGSKLASSE_ID < 300 OR TVD.LEISTUNGSKLASSE_ID > 399))
                    OR (
                      LENGTH(TRIM(CAST(TVD.LEISTUNGSKLASSE_ID AS STRING))) = 6
                      AND TVD.LEISTUNGSKLASSE_ID < 699999
                      AND CAST(FLOOR(TVD.LEISTUNGSKLASSE_ID / 1000) AS INT64) <> 622
                    )
                  );
                """
                return sql

            process_voice_export = BigQueryExecuteQueryOperator(
                task_id="process_voice_export",
                sql=build_bigquery_sql(),
                use_legacy_sql=False,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_TRUNCATE",
                location="US",
                gcp_conn_id="google_cloud_default",
            )
            # Add subsequent tasks for GCS export and SFTP distribution here
        ```

2.  **BigQuery Tables**
    *   **Language:** BigQuery DDL/SQL
    *   **Purpose:** Create the target `DWHM_APT_NNA_Voice` table and ensure all source Oracle tables have their corresponding BigQuery representations.
    *   **Content:** CREATE TABLE statements for all required BigQuery tables, including schema definitions.

3.  **GCS Export and SFTP Distribution Script(s)**
    *   **Language:** Python / Bash (using `gcloud` and `gsutil`)
    *   **Purpose:** Handle the extraction of data from BigQuery to GCS, post-processing (header/trailer and gzip), and secure transfer to the external SFTP target.
    *   **Content:** Scripts to be developed for these specific tasks, potentially integrated as Python functions or Bash commands within the Airflow DAG.