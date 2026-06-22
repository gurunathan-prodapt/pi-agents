# Migration Design — DW.BERT_AUSD_V_TA_CNTRCT_TEMPL

## 1. Purpose & Scope

This job is responsible for mirroring Carmen contract templates. Specifically, it involves extracting contract template data and associated descriptions from source systems, applying filtering logic based on dates and production status, and loading the processed data into a target table (`sof$ta_cntrct_templ`). The process is orchestrated by a UC4 job, which invokes KornShell scripts that, in turn, execute an Oracle SQL*Plus script. The migration aims to re-implement this entire workflow on Google Cloud Platform, using BigQuery for data storage and transformation, and Airflow for orchestration.

## 2. Source Inventory

The job consists of the following components:

| File Path                                                                                                   | Technology      | Category | Tool            | Tier   | Automation Bucket | Summary                                                                                                                                                                                                                                                                                                      |
| :---------------------------------------------------------------------------------------------------------- | :-------------- | :------- | :-------------- | :----- | :---------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_CNTRCT_TEMPL.xml` | UC4             | uc4      | UC4/Automic     | medium | semi_auto         | Defines a UC4 UNIX job named DW.BERT_AUSD_V_TA_CNTRCT_TEMPL that executes a KornShell script to mirror Carmen contract templates.                                                                                                                                                                           |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_templ.ksh`                     | KornShell       | shell    | KornShell       | medium | semi_auto         | This KornShell script serves as a wrapper for the contract data reconciliation process for the `ta_cntrct_templ` table, handling environment setup, parameter parsing, and error logging before invoking a core processing script (`k_ausd_v_ta_cntrct_templ.ksh`).                                     |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_templ.ksh`                     | KornShell       | shell    | KornShell       | medium | semi_auto         | This is a control script for `r_ausd_vertrag.ksh` (likely a typo, should be `r_ausd_v_ta_cntrct_templ.ksh`), responsible for environment setup, parameter parsing, error handling, and orchestrating the execution of an SQL script (`d_ausd_v_ta_cntrct_templ.sql`) which likely updates the `ta_cntrct_templ` table. |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_cntrct_templ.sql`                     | Oracle SQL*Plus | sql      | Oracle SQL*Plus | medium | semi_auto         | This SQL*Plus script truncates a target table and then populates it by selecting and joining data from source tables, applying date-based filtering. It determines a processing date from a metadata table.                                                                                                    |

## 3. Target Architecture

The target architecture will leverage Google Cloud Platform services:

*   **Orchestration:** Apache Airflow on Cloud Composer. The UC4 job will be converted into an Airflow DAG.
*   **Data Transformation & Storage:** BigQuery will be used for storing both source and target tables, and for executing the data transformation logic.
*   **Data Ingestion:** For external source systems (e.g., Carmen, `dwtk_meldungen`), data will be ingested into BigQuery via appropriate mechanisms such as Cloud Data Fusion, Dataflow, or a batch load process.
*   **Logging & Monitoring:** Cloud Logging and Cloud Monitoring will replace the shell script's custom logging mechanisms.

## 4. Data Flow & Lineage

The current data flow is as follows:

1.  **UC4 Job (`DW.BERT_AUSD_V_TA_CNTRCT_TEMPL.xml`)**: Scheduled to run, it invokes the main KornShell wrapper script.
    *   Invokes `SCRIPT:R_AUSD_V_TA_CNTRCT_TEMPL.KSH`
    *   Uses external resources like `HOST:DWHDWH1P` and `LOGIN:DW.UNIX.ISBERT`.
2.  **Wrapper KornShell Script (`r_ausd_v_ta_cntrct_templ.ksh`)**:
    *   Initializes environment, sets up logging, and parses parameters.
    *   Invokes the control KornShell script (`k_ausd_v_ta_cntrct_templ.ksh`).
3.  **Control KornShell Script (`k_ausd_v_ta_cntrct_templ.ksh`)**:
    *   Further sets up the environment (e.g., `h_alis_sqlplus.ksh` for SQL*Plus).
    *   Determines the path to the core SQL script.
    *   Executes the Oracle SQL*Plus script (`d_ausd_v_ta_cntrct_templ.sql`).
    *   Captures the number of processed records.
4.  **Oracle SQL*Plus Script (`d_ausd_v_ta_cntrct_templ.sql`)**:
    *   Reads the processing date from `isbert_schema.dwtk_meldungen`.
    *   Truncates the target table `sof$ta_cntrct_templ`.
    *   Reads from source tables `cds$ta_cntrct_template` and `cds$ta_care_description` (via a DB-link `v_carmen` to `@pcrs1`, presumably the Carmen database).
    *   Applies filtering logic based on `insert_at`, `modified_at`, `valid_from`, `valid_to`, `is_production`, and `language`.
    *   Inserts transformed data into `sof$ta_cntrct_templ`.
    *   Uses the Oracle package `isbert_schema.DWPA_UTIL_SKRIPT` for truncation.

**Target Data Flow (BigQuery & Airflow):**

1.  **Airflow DAG (`dw_bert_ausd_v_ta_cntrct_templ`)**: The UC4 job will be replaced by an Airflow DAG.
    *   The DAG will orchestrate the execution of BigQuery operations, potentially wrapping Python scripts or BigQuery stored procedures.
2.  **BigQuery Stored Procedure (Orchestration)**: The logic from `r_ausd_v_ta_cntrct_templ.ksh` and `k_ausd_v_ta_cntrct_templ.ksh` will be consolidated into a BigQuery Stored Procedure or Python Operators within Airflow. This procedure will handle:
    *   Parameter passing (e.g., job identifier, entry number).
    *   Logging to BigQuery audit tables.
    *   Invoking the core data transformation logic.
    *   Error handling.
3.  **BigQuery SQL (Transformation)**: The logic from `d_ausd_v_ta_cntrct_templ.sql` will be directly translated into a BigQuery SQL script or another BigQuery Stored Procedure.
    *   This script will read from BigQuery tables equivalent to `isbert_schema.dwtk_meldungen`, `cds$ta_cntrct_template`, and `cds$ta_care_description`.
    *   It will perform the `TRUNCATE` and `INSERT...SELECT` operations on the BigQuery `sof_ta_cntrct_templ` table.

## 5. Transformation Logic

**Original (Oracle SQL*Plus - `d_ausd_v_ta_cntrct_templ.sql`):**

```sql
-- Determine processing date
SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum
  FROM isbert_schema.dwtk_meldungen m
 WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';

-- Truncate target table
begin
 isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_cntrct_templ');
end;
/

-- Insert data into target table
INSERT INTO sof$ta_cntrct_templ
(CNTRCT_TEMPLATE_ID, CDS_DESCRIPTION_ID, CDS_DESCRIPTION )
SELECT
        ct.cntrct_template_id,
        ct.cds_description_id,
        cd.cds_description
FROM
        cds$ta_cntrct_template &v_carmen ct,
        cds$ta_care_description &v_carmen cd
WHERE
        ct.cds_description_id = cd.cds_description_id
AND
        ct.insert_at <= TO_DATE('&v_datum','YYYYMMDD')
AND     (   ct.modified_at IS NULL OR ct.modified_at > TO_DATE('&v_datum','YYYYMMDD')     )
AND     ct.valid_from <= TO_DATE('&v_datum','YYYYMMDD')
AND     (   ct.valid_to IS NULL OR ct.valid_to > TO_DATE('&v_datum','YYYYMMDD')       )
AND     ct.is_production = 1
AND     cd.language = 1;

COMMIT;
```

**Target (BigQuery SQL):**

```sql
-- BigQuery SQL for d_ausd_v_ta_cntrct_templ.sql

-- Declare processing date variable
DECLARE v_datum STRING DEFAULT (\n
  SELECT IFNULL(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
  FROM `your_project.isbert_schema.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Truncate target table
TRUNCATE TABLE `your_project.your_dataset.sof_ta_cntrct_templ`;

-- Insert data into target table
INSERT INTO `your_project.your_dataset.sof_ta_cntrct_templ`
(
  CNTRCT_TEMPLATE_ID,
  CDS_DESCRIPTION_ID,
  CDS_DESCRIPTION
)
SELECT
  ct.cntrct_template_id,
  ct.cds_description_id,
  cd.cds_description
FROM `your_project.your_dataset.cds_ta_cntrct_template` ct -- Assumes source data is migrated to BigQuery
JOIN `your_project.your_dataset.cds_ta_care_description` cd -- Assumes source data is migrated to BigQuery
  ON ct.cds_description_id = cd.cds_description_id
WHERE ct.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
  AND (ct.modified_at IS NULL OR ct.modified_at > PARSE_DATE('%Y%m%d', v_datum))
  AND ct.valid_from <= PARSE_DATE('%Y%m%d', v_datum)
  AND (ct.valid_to IS NULL OR ct.valid_to > PARSE_DATE('%Y%m%d', v_datum))
  AND ct.is_production = 1
  AND cd.language = 1;
```

**Shell Script Logic to BigQuery Stored Procedure (Pseudocode):**

```sql
CREATE OR REPLACE PROCEDURE `your_project.your_dataset.r_ausd_v_ta_cntrct_templ`(
  IN p_JobKennung STRING,
  IN p_EintragsNr INT64
)
BEGIN
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE DW_EintragsNr INT64 DEFAULT p_EintragsNr;
  DECLARE v_sysdate DATE DEFAULT CURRENT_DATE();
  DECLARE v_TabName STRING DEFAULT 'ta_cntrct_templ';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_status STRING DEFAULT 'OK';

  -- Parameter validation (equivalent to shell script's parameter checks)
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET ErrNr = 193;
    SET ErrArg = 'Jobkennung';
  END IF;
  -- ... more parameter checks ...

  IF ErrNr != 0 THEN
    -- Log error to BigQuery error log table
    INSERT INTO `your_project.your_dataset.job_error_log` (...) VALUES (...);
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = CONCAT('FEHLER: ', CAST(ErrNr AS STRING), ' ', ErrArg);
  END IF;

  -- Log job start
  INSERT INTO `your_project.your_dataset.job_log` (...) VALUES (...);

  BEGIN -- Main SQL execution block
    -- Execute core data transformation (equivalent of d_ausd_v_ta_cntrct_templ.sql)
    -- This could be inline or a call to another stored procedure
    CALL `your_project.your_dataset.d_ausd_v_ta_cntrct_templ_transform`(v_datum_param); -- Example

    SET v_records = @@row_count; -- Capture affected rows

    -- Log job result
    INSERT INTO `your_project.your_dataset.job_result` (...) VALUES (...);

  EXCEPTION WHEN ERROR THEN
    SET v_status = 'ERROR';
    -- Log core transformation error
    INSERT INTO `your_project.your_dataset.job_error_log` (...) VALUES (...);
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'AppError: Abbruch';
  END;

  -- Log job status
  INSERT INTO `your_project.your_dataset.job_status` (...) VALUES (...);

EXCEPTION WHEN ERROR THEN
  -- Log overall job failure
  INSERT INTO `your_project.your_dataset.job_status` (...) VALUES (...);
  RESIGNAL;
END;
```

## 6. External Dependencies

| Original System / Object          | Class              | Reference Count | Inbound Files | Migration Strategy                                                                   |
| :-------------------------------- | :----------------- | :-------------- | :------------ | :----------------------------------------------------------------------------------- |
| `isbert_schema.dwtk_meldungen`    | Oracle Table       | 1               |               | Replicate as a BigQuery table `your_project.isbert_schema.dwtk_meldungen`. Data to be ingested from source. |
| `cds$ta_cntrct_template`          | Oracle Table       | 1               |               | Replicate as a BigQuery table `your_project.your_dataset.cds_ta_cntrct_template`. Data to be ingested from Carmen (source of `&v_carmen`). |
| `cds$ta_care_description`         | Oracle Table       | 1               |               | Replicate as a BigQuery table `your_project.your_dataset.cds_ta_care_description`. Data to be ingested from Carmen (source of `&v_carmen`). |
| `sof$ta_cntrct_templ`             | Oracle Table       | 1               |               | Replicate as a BigQuery table `your_project.your_dataset.sof_ta_cntrct_templ`. This is the target table. |
| `isbert_schema.DWPA_UTIL_SKRIPT`  | Oracle Package     | 1               |               | `runstatement` for TRUNCATE will be replaced by BigQuery's native `TRUNCATE TABLE`. Other package functionalities (if any) to be identified and re-implemented in BigQuery SQL or Python. |
| `DW.HOLE_PFAD`, `DW.BERT_LESE_LOG`| UC4 Include Objects|                 |               | These UC4 includes represent reusable components. Their functionality should be assessed if they contribute to environment setup or logging that needs to be replicated in Airflow/BigQuery. Current analysis indicates they are used for basic path/logging, which can be handled by Airflow context or BigQuery logging. |
| `HOST:DWHDWH1P`, `LOGIN:DW.UNIX.ISBERT`| UC4 Host/Login     |                 |               | These define the execution environment for the shell scripts. In GCP, this maps to the Airflow worker environment (e.g., Kubernetes Pod) and Dataproc cluster configuration (for PySpark jobs). Specific login details will be handled by GCP service accounts. |
| `v_carmen @pcrs1`                 | Oracle DB-Link     | 1               |               | The database link implies accessing a remote Oracle database (Carmen). This will require a data ingestion pipeline to bring `cds$ta_cntrct_template` and `cds$ta_care_description` data into BigQuery. Cloud Data Fusion or Dataflow are suitable options. |

## 7. Unresolved / Risks

*   **Detailed `DWMSG_*` Utilities:** The shell scripts rely on several `DWMSG_*` utilities for error handling and logging. While a BigQuery logging table pseudocode has been provided, the exact logic and functionality of these utilities (e.g., `DWMSG_MeldeFehler`, `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK`) need to be thoroughly analyzed and re-implemented in BigQuery Stored Procedures or Python code within Airflow.
*   **Shell Utilities:** Similarly, the shell scripts source helper scripts like `f_alis_msgerr.ksh`, `h_alis_job.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh`. The functionalities of these helper scripts need to be understood and migrated. Parameter parsing (`getopts`) can be handled by Airflow task parameters or BigQuery stored procedure parameters. Date handling can use BigQuery date functions.
*   **`trap` and `eval` Commands:** The `trap` and `eval` commands in the KornShell scripts are not directly portable to BigQuery SQL or standard Airflow Python. The error handling (`trap`) can be replicated using `TRY...CATCH` blocks in BigQuery Stored Procedures or Python `try...except` blocks in Airflow. `eval` for reading from a temp file will be replaced by direct variable assignment or query results in BigQuery.
*   **Temporary File (`tmpFile`)**: The use of a temporary file for record counts (`$DW_DIR_UTL/bert_k_ausd_v_ta_cntrct_templ_$$.tmp`) will be replaced by directly capturing `@@row_count` in BigQuery SQL or by Airflow XComs for inter-task communication.
*   **Source Table Schemas:** The exact schemas (column names and data types) for `isbert_schema.dwtk_meldungen`, `cds$ta_cntrct_template`, and `cds$ta_care_description` were not provided. These need to be accurately determined for BigQuery table creation and data ingestion.
*   **Placeholder Replacement:** All `YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_REGION`, `YOUR_DATAPROC_CLUSTER_NAME`, `YOUR_BUCKET_NAME`, `your_project.your_dataset` placeholders must be replaced with actual GCP resource names.
*   **`v_carmen` DB-Link Implementation:** The method for ingesting data from the "Carmen" database (accessed via `v_carmen @pcrs1`) needs to be fully defined. This will likely involve a separate data ingestion pipeline.

## 8. Build Plan

The migration will involve the following steps:

1.  **Data Ingestion Pipeline Development:**
    *   **Task:** Implement data ingestion jobs for `isbert_schema.dwtk_meldungen`, `cds$ta_cntrct_template`, and `cds$ta_care_description` from their respective source systems (Oracle, Carmen) into BigQuery.
    *   **Tools:** Cloud Data Fusion, Dataflow, or custom Python scripts.
    *   **Output:** BigQuery tables: `your_project.isbert_schema.dwtk_meldungen`, `your_project.your_dataset.cds_ta_cntrct_template`, `your_project.your_dataset.cds_ta_care_description`.

2.  **BigQuery Target Table Creation:**
    *   **Task:** Create the target BigQuery table `your_project.your_dataset.sof_ta_cntrct_templ` with an appropriate schema based on the source definition and target columns.
    *   **Tools:** BigQuery DDL.
    *   **Output:** BigQuery table.

3.  **BigQuery Logging & Audit Tables Creation:**
    *   **Task:** Define and create BigQuery tables for `job_log`, `job_error_log`, `job_result`, and `job_status` to replace the shell script's logging and error handling.
    *   **Tools:** BigQuery DDL.
    *   **Output:** BigQuery tables.

4.  **BigQuery SQL Transformation Logic Development:**
    *   **Task:** Translate `d_ausd_v_ta_cntrct_templ.sql` into a BigQuery SQL script or a BigQuery Stored Procedure (`d_ausd_v_ta_cntrct_templ_transform`).
    *   **Tools:** BigQuery SQL.
    *   **Output:** BigQuery SQL script / BigQuery Stored Procedure.

5.  **BigQuery Stored Procedure for Orchestration:**
    *   **Task:** Develop a BigQuery Stored Procedure (`r_ausd_v_ta_cntrct_templ`) that encapsulates the orchestration, parameter handling, and error management logic from `r_ausd_v_ta_cntrct_templ.ksh` and `k_ausd_v_ta_cntrct_templ.ksh`. This procedure will call the SQL transformation logic from step 4.
    *   **Tools:** BigQuery SQL (for stored procedures).
    *   **Output:** BigQuery Stored Procedure.

6.  **Airflow DAG Development:**
    *   **Task:** Create an Airflow DAG (`dw_bert_ausd_v_ta_cntrct_templ`) on Cloud Composer to schedule and execute the BigQuery Stored Procedure.
    *   **Tools:** Python (Airflow DAG).
    *   **Output:** Python `.py` file for Airflow DAG. The DAG will contain a `BigQueryExecuteQueryOperator` or `DataprocSubmitJobOperator` (if a PySpark wrapper is preferred for the stored procedure call, as suggested by the `uc4_to_airflow_dag_design` tool) that invokes the BigQuery Stored Procedure from step 5.

7.  **Testing and Validation:**
    *   **Task:** Thoroughly test each component (ingestion, BigQuery procedures, Airflow DAG) to ensure data integrity and functional equivalence with the legacy system.
    *   **Tools:** BigQuery test queries, Airflow logs, Cloud Logging.

8.  **Deployment:**
    *   **Task:** Deploy the BigQuery tables, stored procedures, and Airflow DAG to the production environment.
    *   **Tools:** `gcloud` CLI, Terraform (for IaC).