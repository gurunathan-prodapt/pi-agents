# Migration Design — DW.BERT_AUSD_BP_TA_RN_VERTRAG

## 1. Purpose & Scope
This document outlines the migration plan for the ETL job `DW.BERT_AUSD_BP_TA_RN_VERTRAG` from its current UC4/KornShell/Oracle environment to Google Cloud Platform, specifically utilizing Airflow for orchestration, Dataproc for script execution, and BigQuery for data warehousing.

The primary purpose of this job is the "preparation of instantiated basic products" (`Aufbereitung der instantiierten Basisprodukte`) for the BERT system. This involves processing contract-related data to generate a consolidated view of phone numbers and their statuses associated with contracts.

The scope of this migration includes:
*   Converting the UC4 job scheduling and orchestration logic to an Airflow DAG.
*   Migrating the KornShell scripts to PySpark or Python, preserving the orchestration and parameter handling.
*   Translating the Oracle SQL transformation logic to BigQuery SQL.
*   Establishing data lineage from source Oracle tables to target BigQuery tables.
*   Addressing external dependencies on the Oracle database.

## 2. Source Inventory

The following components comprise the `DW.BERT_AUSD_BP_TA_RN_VERTRAG` job:

| Component Type | File Path                                                                                                   | Technology  | Tier   | Migration Bucket | Purpose                                                                                                                                                                                                                            |
|:---------------|:------------------------------------------------------------------------------------------------------------|:------------|:-------|:-----------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| UC4 Job        | `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_RN_VERTRAG.xml` | UC4/Automic | medium | semi_auto        | Scheduler and orchestrator, initiating the execution of the main KornShell script.                                                                                                                                   |
| KornShell Script | `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_vertrag.ksh`                      | KornShell   | medium | semi_auto        | Parent script: Orchestrates the overall process, handles parameters (`Stichtag`, `Wiederanlaufwert`), sources utility scripts, and calls the core KornShell script.                                                                      |
| KornShell Script | `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_vertrag.ksh`                      | KornShell   | medium | semi_auto        | Core script: Parses parameters, validates input dates, sources utility scripts for error handling and SQL*Plus execution, and orchestrates the execution of the main SQL script (`d_ausd_bp_ta_rn_vertrag.sql`).                     |
| SQL Script     | `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_rn_vertrag.sql`                      | Oracle SQL  | medium | semi_auto        | Main data transformation logic: Truncates `SOF$TA_RN_VERTRAG` and populates it by aggregating and reshaping data from `SOF$TA_RN_EINZELN`, using `MAX` functions grouped by contract ID. Defines a date variable from `DWTK_MELDUNGEN`. |
| Utility Script | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh`                           | KornShell   |        |                  | Provides error handling, logging, and status management.                                                                                                                                                                       |
| Utility Script | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh`                             | KornShell   |        |                  | Provides utility functions for date calculations and validation.                                                                                                                                                               |
| Utility Script | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh`                        | KornShell   |        |                  | Provides utility functions for parsing, validating, and converting parameters.                                                                                                                                                 |
| Utility Script | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh`                          | KornShell   |        |                  | Provides helper routines for executing SQL*Plus scripts.                                                                                                                                                                       |
| Utility Script | `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh`                                      | KornShell   |        |                  | Calculates and formats today's date and yesterday's date.                                                                                                                                                                      |
| Utility Script | `vobs/dw_source/istools/seu/template/.dw_init`                                                              | KornShell   |        |                  | Initializes environment variables.                                                                                                                                                                                             |

## 3. Target Architecture
The migrated job will run on Google Cloud Platform, utilizing the following services:
*   **Orchestration**: Apache Airflow on Cloud Composer.
*   **Processing**: Dataproc cluster for executing PySpark scripts (replacing KornShell logic).
*   **Data Storage**: BigQuery for all transformed data.
*   **External Data Ingestion**: Depending on the migration strategy for Oracle sources, data will either be replicated to BigQuery (e.g., via Cloud Data Fusion, DataStream, or batch loads) or accessed via BigQuery external tables.

The target BigQuery environment will include:
*   A BigQuery dataset for the `isbert_schema`.
*   A target table `isbert_schema.SOF_TA_RN_VERTRAG` to replace the existing Oracle `SOF$TA_RN_VERTRAG`.
*   BigQuery representations of `SOF$TA_RN_EINZELN` and `DWTK_MELDUNGEN` as source tables, likely within the same `isbert_schema` dataset.

## 4. Data Flow & Lineage

The migrated data flow will be orchestrated by an Airflow DAG and will follow these steps:

1.  **Airflow DAG Trigger**: The `dw_bert_ausd_bp_ta_rn_vertrag` Airflow DAG will be triggered by its schedule.
2.  **PySpark Execution (Orchestration)**: An Airflow task (`DataprocSubmitJobOperator`) will launch a PySpark job on Dataproc. This PySpark job will be the functional equivalent of `r_ausd_bp_ta_rn_vertrag.ksh` and `k_ausd_bp_ta_rn_vertrag.ksh`.
    *   It will handle parameter passing (e.g., `Stichtag`, `Wiederanlaufwert`).
    *   It will incorporate the logic from the utility KornShell scripts (date handling, parameter validation, error logging).
3.  **BigQuery Transformation (Data Processing)**: The PySpark job will then execute the core transformation logic, which will be translated from `d_ausd_bp_ta_rn_vertrag.sql` into BigQuery SQL.
    *   This SQL will truncate `isbert_schema.SOF_TA_RN_VERTRAG`.
    *   It will `INSERT` data into `isbert_schema.SOF_TA_RN_VERTRAG` by selecting from `isbert_schema.SOF_TA_RN_EINZELN`, performing the `MAX()` aggregations grouped by `CNTRCT_ID`.
    *   The `Stichtag` parameter, potentially derived from `DWTK_MELDUNGEN` in the legacy system, will be passed to the BigQuery SQL.
4.  **Logging/Monitoring**: A placeholder Airflow task will exist to represent the former `:inc DW.BERT_LESE_LOG` step, which can be adapted for Cloud Logging or other monitoring integrations.

**Lineage Summary:**
`Oracle (SOF$TA_RN_EINZELN, DWTK_MELDUNGEN)` -> `BigQuery (isbert_schema.SOF_TA_RN_EINZELN, isbert_schema.DWTK_MELDUNGEN)` -> `BigQuery (isbert_schema.SOF_TA_RN_VERTRAG)`

## 5. Transformation Logic

The core transformation logic resides in `d_ausd_bp_ta_rn_vertrag.sql`. This logic will be directly translated into BigQuery SQL.

*   **Initialization**: The script defines a `v_datum` from `isbert_schema.dwtk_meldungen`. In BigQuery, this would be either directly queried from `isbert_schema.DWTK_MELDUNGEN` or passed as a parameter.
*   **Target Table Truncation**: The `TRUNCATE TABLE sof$ta_rn_vertrag REUSE STORAGE` operation will be translated to `TRUNCATE TABLE \`your-gcp-project.isbert_schema.SOF_TA_RN_VERTRAG\`;`.
*   **Data Insertion and Reshaping**: The main `INSERT INTO ... SELECT ... GROUP BY cntrct_id` statement will be converted to BigQuery standard SQL.
    *   The `FULL` hint and `PARALLEL(rp,4)` hint are Oracle-specific and will be removed, as BigQuery's columnar storage and distributed processing handle performance automatically.
    *   The `MAX()` aggregations across numerous fields (e.g., `TN_TEL_msisdn`, `TN_FAX_status`, `MS_RN_1_msisdn`) grouped by `CNTRCT_ID` indicate a pivot-like operation where multiple rows for a `CNTRCT_ID` are consolidated into one, with the "latest" or "highest" value for each attribute being selected. This logic will be maintained as-is in BigQuery SQL.
*   **KornShell Logic**: The parameter handling, date calculations, and environment setup from `r_ausd_bp_ta_rn_vertrag.ksh` and `k_ausd_bp_ta_rn_vertrag.ksh` will be rewritten in Python/PySpark within the Airflow context. This will involve:
    *   Parsing `Stichtag` and `Wiederanlaufwert` parameters.
    *   Implementing date checks and calculations from `h_alis_date.ksh` and `gestern.ksh` using Python's `datetime` library.
    *   Integrating error handling from `f_alis_msgerr.ksh` into Python logging mechanisms.
    *   Executing the BigQuery SQL transformation.

## 6. External Dependencies

The original job has a strong dependency on an Oracle database.

*   **Oracle Database**: This is the primary external system.
*   **`SOF$TA_RN_EINZELN` (Source Table)**: This table needs to be migrated to BigQuery. A continuous data replication (e.g., DataStream, Cloud Data Fusion) or periodic batch ingestion pipeline from Oracle to `isbert_schema.SOF_TA_RN_EINZELN` in BigQuery will be required.
*   **`DWTK_MELDUNGEN` (Source Table)**: Similar to `SOF$TA_RN_EINZELN`, this table needs to be migrated to BigQuery as `isbert_schema.DWTK_MELDUNGEN` to provide the `v_datum` parameter source.
*   **`SOF$TA_RN_VERTRAG` (Target Table)**: This table will be natively created and managed in BigQuery as `isbert_schema.SOF_TA_RN_VERTRAG`.

**Replacement Strategy:**
The Oracle database will be replaced by BigQuery for all data storage and transformation. The source tables `SOF$TA_RN_EINZELN` and `DWTK_MELDUNGEN` will have their data continuously or regularly ingested into BigQuery. The `DW.UNIX.ISBERT` login and `DWHDWH2P` host from the UC4 job will be replaced by appropriate GCP service accounts and Dataproc cluster configurations.

## 7. Unresolved / Risks

*   **UC4 Schedule**: The exact schedule of the UC4 job is not derivable from the provided XML. The Airflow DAG will initially be created with `schedule=None` and requires manual definition based on business requirements.
*   **UC4 `DW.HOLE_PFAD` and `DW.BERT_LESE_LOG` Includes**: The exact functionality of these UC4 includes is not fully detailed in the provided XML. `DW.BERT_LESE_LOG` is treated as a placeholder for a post-processing/logging step, but its specific actions might need further investigation. `DW.HOLE_PFAD` is an `:inc` statement without an explicit file path in the given XML, its purpose should be verified.
*   **GCP Configuration Placeholders**: The Airflow DAG design includes placeholders for `YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_REGION`, `YOUR_DATAPROC_CLUSTER_NAME`, and `YOUR_BUCKET_NAME`. These need to be manually configured for the target GCP environment.
*   **Utility Script Translation**: While the purpose of utility scripts like `f_alis_msgerr.ksh` is understood, their exact implementation details in KornShell might contain nuances that need careful translation to Python for robustness.
*   **Commented-Out Logic**: The commented-out `sed`, `sort`, and `join` commands in `k_ausd_bp_ta_rn_vertrag.ksh` indicate potential inactive post-processing or data merging steps. These are not part of the current migration scope but should be noted as a potential future requirement if they are ever reactivated in the legacy system.
*   **Oracle Schema Conversion**: The Oracle `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` procedure call is Oracle-specific. If this procedure contains additional logic beyond `TRUNCATE TABLE`, it needs to be identified and migrated. However, based on the provided context, it appears to be a wrapper for `TRUNCATE TABLE`.
*   **Data Types and Implicit Conversions**: Oracle SQL and BigQuery SQL have differences in data types and implicit conversion rules. A thorough review of the SQL script will be necessary to ensure data integrity during migration.

## 8. Build Plan

The migration build plan will consist of the following ordered steps:

1.  **BigQuery Schema and Table Creation**:
    *   Create the `isbert_schema` dataset in BigQuery.
    *   Define and create the target table `isbert_schema.SOF_TA_RN_VERTRAG` in BigQuery, inferring schema from the Oracle `SOF$TA_RN_VERTRAG` and the `INSERT` statement.
    *   Define and create the source tables `isbert_schema.SOF_TA_RN_EINZELN` and `isbert_schema.DWTK_MELDUNGEN` in BigQuery, inferring schemas from their Oracle counterparts.
2.  **Oracle Data Ingestion to BigQuery**:
    *   Implement a data ingestion pipeline (e.g., using DataStream for CDC or Cloud Data Fusion for batch loading) to populate `isbert_schema.SOF_TA_RN_EINZELN` and `isbert_schema.DWTK_MELDUNGEN` in BigQuery from their Oracle sources. This pipeline must ensure data freshness as per business requirements.
3.  **KornShell to PySpark/Python Conversion**:
    *   Translate `r_ausd_bp_ta_rn_vertrag.ksh` and `k_ausd_bp_ta_rn_vertrag.ksh` into a single PySpark or Python script. This script will handle:
        *   Parameter parsing (e.g., `Stichtag`, `Wiederanlaufwert`).
        *   Date calculations (Python `datetime`).
        *   Error handling and logging (Python `logging`).
        *   Execution of the BigQuery SQL transformation.
    *   Translate utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`, `.dw_init`) functions into reusable Python modules or functions to be called by the main PySpark/Python script.
4.  **Oracle SQL to BigQuery SQL Translation**:
    *   Convert `d_ausd_bp_ta_rn_vertrag.sql` into BigQuery standard SQL, ensuring all data types and functions are compatible.
    *   Replace `TRUNCATE TABLE` with BigQuery equivalent.
    *   Ensure `MAX()` aggregations and `GROUP BY` logic are correctly translated.
5.  **Airflow DAG Development**:
    *   Create a new Airflow DAG named `dw_bert_ausd_bp_ta_rn_vertrag`.
    *   Define a `DataprocSubmitJobOperator` task to run the converted PySpark/Python script on a Dataproc cluster.
    *   Include a placeholder task for the `DW.BERT_LESE_LOG` functionality, to be refined based on specific logging requirements.
    *   Configure DAG properties like `schedule`, `start_date`, and `default_args` using appropriate GCP values.
6.  **Testing**:
    *   Unit test individual PySpark/Python modules and BigQuery SQL components.
    *   Integration test the Airflow DAG on Cloud Composer, verifying end-to-end data flow and transformation.
    *   Perform data validation to ensure consistency between legacy Oracle output and new BigQuery output.
7.  **Deployment**:
    *   Deploy the Airflow DAG to Cloud Composer.
    *   Upload PySpark/Python scripts to a GCS bucket accessible by Dataproc.
    *   Deploy BigQuery SQL scripts.

This concludes the migration design document.