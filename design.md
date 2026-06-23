# Migration Design — DW.BERT_AUSD_V_TA_P_VERTRAG

## 1. Purpose & Scope

The primary purpose of this job is to process and synchronize contract-related data, specifically focusing on "twin-bill" contracts, and populate the `sof$ta_p_vertrag` table. The job orchestrates a series of shell scripts that ultimately execute an Oracle SQL*Plus script. This SQL script performs data selection, transformation, and insertion from a temporary table (`sof$ta_vertrag_tmp`) into the target table (`sof$ta_p_vertrag`), followed by the cleanup of various temporary tables.

The scope of this migration design covers the conversion of the UC4 job orchestration, the KornShell wrapper and control scripts, and the core Oracle SQL*Plus transformation logic to a BigQuery-compatible ecosystem, likely orchestrated by Airflow.

## 2. Source Inventory

| File Path | Technology | Tier | Automation Bucket | Description |
|---|---|---|---|---|
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_P_VERTRAG.xml` | UC4/Automic | N/A (no complexity data) | `semi_auto` | Orchestrates the execution of a shell script to update contract information. |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh` | KornShell | N/A (no complexity data) | `semi_auto` | Framework script for synchronizing contract data, handling environment setup, parameter parsing, and error trapping, calling the core script. |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh` | KornShell | N/A (no complexity data) | `semi_auto` | Control script for `r_ausd_v_ta_p_vertrag.ksh`, managing SQL script execution and job status. |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_p_vertrag.sql` | Oracle SQL*Plus | N/A (no complexity data) | `semi_auto` | Core SQL script processing twinbill contracts, populating `sof$ta_p_vertrag` from `sof$ta_vertrag_tmp`. |

## 3. Target Architecture

The target architecture will leverage Google Cloud Platform (GCP) services.

*   **Orchestration**: Apache Airflow will replace UC4 for job scheduling and workflow management.
*   **Data Processing**: BigQuery will serve as the primary data warehouse for all transformed data. SQL transformations will be converted to BigQuery SQL.
*   **Intermediate Processing (if needed)**: Dataproc (for PySpark) may be used to handle any complex shell scripting logic or non-SQL transformations if they cannot be directly translated to BigQuery SQL or Airflow operators. Based on the current analysis, the shell scripts primarily act as wrappers around the core SQL logic, so a direct translation to BigQuery SQL and Airflow tasks is likely possible.
*   **Source Data Ingestion**: Data from the legacy Oracle system (including tables like `sof$ta_vertrag_tmp` and `isbert_schema.dwtk_meldungen`) will be ingested into BigQuery using appropriate data ingestion tools (e.g., Cloud Data Fusion, database migration services, or custom data pipelines). Temporary tables (`sof$ta_vertrag_tmp`) will be represented as temporary or staging tables in BigQuery.
*   **Logging and Monitoring**: Cloud Logging and Cloud Monitoring will be used for job execution logging and performance monitoring.

## 4. Data Flow & Lineage

The current data flow can be described as follows:

1.  **UC4 Job `DW.BERT_AUSD_V_TA_P_VERTRAG` (Orchestrator)**: Initiates the process, invoking the KornShell wrapper script.
2.  **KornShell Wrapper `r_ausd_v_ta_p_vertrag.ksh`**: Sets up the environment, handles parameters, and calls the control script. It also includes utility scripts for error handling and logging (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`).
3.  **KornShell Control `k_ausd_v_ta_p_vertrag.ksh`**: Manages the execution of the core SQL script, passing parameters for job identification (`p_JobKennung`, `p_EintragsNr`). It also includes utility scripts for job management (`h_alis_sqlplus.ksh`).
4.  **Oracle SQL*Plus Script `d_ausd_v_ta_p_vertrag.sql` (Core Logic)**:
    *   Reads `isbert_schema.dwtk_meldungen` to determine a processing date (`v_datum`).
    *   Truncates the target table `sof$ta_p_vertrag`.
    *   Performs an `INSERT INTO` statement into `sof$ta_p_vertrag` by selecting from `sof$ta_vertrag_tmp` (aliased as `v` and `pv`). The join condition `v.twin_vertrag_id = pv.vertrag_id_carmen (+)` suggests a `LEFT JOIN` on `twin_vertrag_id`.
    *   Commits the transaction.
    *   Truncates a long list of other `sof$ta_` temporary tables.
    *   References an external system `v_carmen = "@pcrs1"` via a DB-Link.

**Target Data Flow (Airflow & BigQuery):**

1.  **Airflow DAG `dw_bert_ausd_v_ta_p_vertrag`**:
    *   **Task 1: Data Ingestion (e.g., `ingest_sof_ta_vertrag_tmp`, `ingest_dwtk_meldungen`)**: Ingest data from the source Oracle system (tables like `sof$ta_vertrag_tmp`, `isbert_schema.dwtk_meldungen`, and potentially others referenced by utility scripts or implied data sources) into BigQuery staging tables.
    *   **Task 2: `truncate_sof_ta_p_vertrag`**: Truncate the BigQuery target table `sof_ta_p_vertrag`.
    *   **Task 3: `transform_and_load_sof_ta_p_vertrag`**: Execute a BigQuery SQL query to perform the main transformation and `INSERT` operation, translating the Oracle SQL logic into BigQuery SQL. This task will read from the BigQuery staging tables representing `sof$ta_vertrag_tmp` and potentially `isbert_schema.dwtk_meldungen`.
    *   **Task 4: `cleanup_temp_tables`**: Execute BigQuery DDL statements to truncate or drop any temporary BigQuery tables corresponding to the various `sof$ta_` tables that were truncated in the Oracle script.

**Lineage:**
`Oracle Sources (CARMEN DB, sof$ta_vertrag_tmp, isbert_schema.dwtk_meldungen) -> BigQuery Staging Tables -> BigQuery sof_ta_p_vertrag`

## 5. Transformation Logic

The core transformation logic resides in `d_ausd_v_ta_p_vertrag.sql`.

**Original Oracle SQL Logic:**

```sql
INSERT INTO sof$ta_p_vertrag
       (vertrag_id_carmen,
       partner_id_carmen,
       rechdef_id_carmen,
       kundenkonto,
       mwst_kennzeichen,
       rahmenvertrag_id,
       rechnungslauf,
       vo_kenn,
       geplant_kuend,
       eingang_kuend,
       vertragsbeginn,
       vertragsstatus,
       sperrart,
       sperrgrund,
       stillegungszeitraum,
       twincard,
       dwh_tarifgr_text,
       bindefrist,
       letztes_upgrade,
       vertragsbindung,
       vertragsbindungseinheit,
       rechnungszahlart,
       rechnungsmedium,
       twin_vertrag_id,
       upgradeberechtigt,
       apn,
       upgradegrund,
       sv_id,
       vda,
       cost_centre,
       cost_centre_user,
       cntrct_ty,
       segment_id,
       rv_action_id,
       rechn_inh_konfig_text,
       order_number,
       commitment_reference_date,
       cntrct_validity_id)
SELECT /*+ parallel(v,4) parallel(pv,4) */
       v.vertrag_id_carmen,
       v.partner_id_carmen,
       v.rechdef_id_carmen,
       v.kundenkonto,
       v.mwst_kennzeichen,
       v.rahmenvertrag_id as rahmenvertrag_id,
       v.rechnungslauf,
       v.vo_kenn as vo_kenn,
       v.geplant_kuend,
       v.eingang_kuend,
       v.vertragsbeginn,
       v.vertragsstatus,
       v.sperrart,
       v.sperrgrund,
       v.stillegungszeitraum,
       v.twincard,
       v.dwh_tarifgr_text,
       v.bindefrist,
       v.letztes_upgrade,
       v.vertragsbindung,
       v.vertragsbindungseinheit,
       v.rechnungszahlart,
       v.rechnungsmedium,
       v.twin_vertrag_id,
       v.upgradeberechtigt,
       v.apn,
       v.upgradegrund,
       v.sv_id,
       v.vda,
       v.cost_centre,
       v.cost_centre_user,
       v.cntrct_ty,
       v.segment_id,
       v.rv_action_id,
       v.rechn_inh_konfig_text,
       v.order_number,
       v.commitment_reference_date,
       v.cntrct_validity_id
  FROM
        sof$ta_vertrag_tmp     v,
        sof$ta_vertrag_tmp     pv
  WHERE
        v.twin_vertrag_id = pv.vertrag_id_carmen (+);
```

**Proposed BigQuery SQL Translation:**

1.  **Date Determination**: The `v_datum` variable, derived from `isbert_schema.dwtk_meldungen`, needs to be translated. In BigQuery, this can be achieved by a subquery or a separate CTE if the table `isbert_schema.dwtk_meldungen` is ingested as `isbert_schema.dwtk_meldungen` in BigQuery.

    ```sql
    DECLARE v_datum STRING;
    SET v_datum = (SELECT IFNULL(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')
                   FROM `your_project.isbert_schema.dwtk_meldungen` m
                   WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE');
    ```

2.  **Main INSERT/SELECT**: The Oracle `INSERT ... SELECT` statement with a `LEFT JOIN` (indicated by `(+)`) will be translated directly to BigQuery SQL. Oracle's `/*+ parallel(v,4) parallel(pv,4) */` hint will be removed as BigQuery automatically handles parallelism.

    ```sql
    TRUNCATE TABLE `your_project.your_dataset.sof_ta_p_vertrag`;

    INSERT INTO `your_project.your_dataset.sof_ta_p_vertrag`
           (vertrag_id_carmen,
           partner_id_carmen,
           rechdef_id_carmen,
           kundenkonto,
           mwst_kennzeichen,
           rahmenvertrag_id,
           rechnungslauf,
           vo_kenn,
           geplant_kuend,
           eingang_kuend,
           vertragsbeginn,
           vertragsstatus,
           sperrart,
           sperrgrund,
           stillegungszeitraum,
           twincard,
           dwh_tarifgr_text,
           bindefrist,
           letztes_upgrade,
           vertragsbindung,
           vertragsbindungseinheit,
           rechnungszahlart,
           rechnungsmedium,
           twin_vertrag_id,
           upgradeberechtigt,
           apn,
           upgradegrund,
           sv_id,
           vda,
           cost_centre,
           cost_centre_user,
           cntrct_ty,
           segment_id,
           rv_action_id,
           rechn_inh_konfig_text,
           order_number,
           commitment_reference_date,
           cntrct_validity_id)
    SELECT
           v.vertrag_id_carmen,
           v.partner_id_carmen,
           v.rechdef_id_carmen,
           v.kundenkonto,
           v.mwst_kennzeichen,
           v.rahmenvertrag_id,
           v.rechnungslauf,
           v.vo_kenn,
           v.geplant_kuend,
           v.eingang_kuend,
           v.vertragsbeginn,
           v.vertragsstatus,
           v.sperrart,
           v.sperrgrund,
           v.stillegungszeitraum,
           v.twincard,
           v.dwh_tarifgr_text,
           v.bindefrist,
           v.letztes_upgrade,
           v.vertragsbindung,
           v.vertragsbindungseinheit,
           v.rechnungszahlart,
           v.rechnungsmedium,
           v.twin_vertrag_id,
           v.upgradeberechtigt,
           v.apn,
           v.upgradegrund,
           v.sv_id,
           v.vda,
           v.cost_centre,
           v.cost_centre_user,
           v.cntrct_ty,
           v.segment_id,
           v.rv_action_id,
           v.rechn_inh_konfig_text,
           v.order_number,
           v.commitment_reference_date,
           v.cntrct_validity_id
      FROM
            `your_project.your_dataset.sof_ta_vertrag_tmp` v
            LEFT JOIN `your_project.your_dataset.sof_ta_vertrag_tmp` pv
                 ON v.twin_vertrag_id = pv.vertrag_id_carmen;
    ```

3.  **Truncation of Temporary Tables**: The `TRUNCATE TABLE` commands will be directly translated to BigQuery DDL for the corresponding staging tables.

    ```sql
    TRUNCATE TABLE `your_project.your_dataset.sof_ta_disc_zusgf`;
    TRUNCATE TABLE `your_project.your_dataset.sof_ta_discount`;
    -- ... and so on for all listed tables
    TRUNCATE TABLE `your_project.your_dataset.sof_ta_vertrag_tmp`;
    TRUNCATE TABLE `your_project.your_dataset.sof_ta_action_assoc`;
    ```

**Shell Script Logic Translation:**
The shell scripts `r_ausd_v_ta_p_vertrag.ksh` and `k_ausd_v_ta_p_vertrag.ksh` contain environment setup, parameter parsing, error handling, and logging. This logic will be primarily handled by Airflow DAG structure and operators:
*   Environment variables will be managed as Airflow Variables or within the Airflow environment.
*   Parameter parsing will be handled by Airflow's templating (Jinja) or Python operators.
*   Error handling and logging will be managed by Airflow's native mechanisms, including task retry logic and integration with Cloud Logging.
*   The `starteSQLSkript` function call in `k_ausd_v_ta_p_vertrag.ksh` will be replaced by a BigQueryOperator in Airflow executing the translated SQL.

## 6. External Dependencies

*   **CARMEN DB (`@pcrs1`)**: The Oracle SQL script references a DB-Link to `CARMEN DB`. This indicates that `sof$ta_vertrag_tmp` and potentially other tables are sourced from or derived from data within the CARMEN system.
    *   **Replacement Strategy**: Data from CARMEN DB must be continuously ingested into BigQuery. This will likely involve setting up a data pipeline using Cloud Data Fusion, Database Migration Service (DMS), or custom Change Data Capture (CDC) solutions to replicate relevant CARMEN tables into BigQuery. The `sof$ta_vertrag_tmp` table should be created as a staging table in BigQuery, populated from the ingested CARMEN data before the main transformation runs.
*   **Oracle Utilities (`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`)**: The SQL script calls `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` for truncating tables.
    *   **Replacement Strategy**: This will be replaced by native BigQuery DDL commands executed via Airflow's `BigQueryOperator` or a Python operator that interacts with the BigQuery API.
*   **Oracle `isbert_schema.dwtk_meldungen`**: Used to determine `v_datum`.
    *   **Replacement Strategy**: Ingest this table into BigQuery. The date determination logic will be translated to BigQuery SQL.

## 7. Unresolved / Risks

*   **Complexity of `sof$ta_vertrag_tmp` Population**: The process by which `sof$ta_vertrag_tmp` is populated in the legacy environment is not fully detailed in the provided files. It's crucial to understand this upstream process to ensure accurate data ingestion into BigQuery.
*   **Utility Scripts**: The content of the various sourced KornShell utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`) is not provided. Their functionalities need to be analyzed to ensure all necessary environmental setups, parameter handling, and error logging are replicated or replaced appropriately in the Airflow/BigQuery environment.
*   **SQL*Plus Specific Features**: While the main `INSERT` statement is standard SQL, other SQL*Plus specific commands (like `WHENEVER SQLERROR CONTINUE/EXIT FAILURE`, `SET TIMING ON`, `SET SERVEROUTPUT ON`, `COLUMN ... NEW_VALUE`) need to be handled. Most of these are for client-side control or logging and will be replaced by Airflow's task management and logging capabilities.
*   **Performance Tuning**: The `/*+ parallel(v,4) parallel(pv,4) */` hint in Oracle suggests performance considerations. While BigQuery handles parallelism automatically, thorough testing will be required to ensure the BigQuery SQL performs optimally, especially with large datasets.
*   **Error Handling and Restartability**: The existing shell scripts have explicit error handling (`ErrNr`, `ErrArg`, `DWMSG_MeldeFehler`) and possibly restartability features. These need to be carefully mapped to Airflow's retry mechanisms, `on_failure_callback` functions, and idempotent BigQuery operations.
*   **Missing `file_complexity` Data**: The absence of `file_complexity` information for all files means that potential hidden complexities or specific migration flags are not identified, increasing the risk of unexpected challenges during implementation.

## 8. Build Plan

The migration will be executed in phases, focusing on translating each component:

1.  **Ingestion of Source Data to BigQuery**:
    *   **Language**: SQL (BigQuery DDL for table creation), Data Fusion pipelines or DMS configuration (for continuous ingestion).
    *   **Files to Generate**:
        *   BigQuery DDL for `sof_ta_p_vertrag` (target table).
        *   BigQuery DDL for `sof_ta_vertrag_tmp` (staging table).
        *   BigQuery DDL for `isbert_schema.dwtk_meldungen` (staging table).
        *   Data ingestion configurations/scripts for populating `sof_ta_vertrag_tmp` and `isbert_schema.dwtk_meldungen` from CARMEN DB.
        *   BigQuery DDL for all other `sof_ta_` temporary tables.

2.  **Translate Core SQL Transformation**:
    *   **Language**: BigQuery SQL.
    *   **Files to Generate**: `d_ausd_v_ta_p_vertrag_bq.sql` (containing the translated `DECLARE`, `TRUNCATE`, `INSERT`, and subsequent `TRUNCATE` statements).

3.  **Translate Shell Script Logic and Orchestration to Airflow DAG**:
    *   **Language**: Python (for Airflow DAG).
    *   **Files to Generate**: `dw_bert_ausd_v_ta_p_vertrag_dag.py` (Airflow DAG definition). This DAG will include:
        *   `BigQueryOperator` tasks for each SQL step (truncate, insert, cleanup).
        *   PythonOperators or other suitable Airflow operators to replicate any parameter handling or conditional logic from the KornShell scripts.
        *   Tasks for data ingestion, ensuring `sof_ta_vertrag_tmp` and `isbert_schema.dwtk_meldungen` are ready before the main transformation.
        *   Appropriate error handling and retry mechanisms.

4.  **Unit and Integration Testing**:
    *   Develop test cases to validate data accuracy and transformation logic in BigQuery against the legacy Oracle system.
    *   Test the Airflow DAG for correct execution order, error handling, and restartability.

5.  **Deployment and Monitoring**:
    *   Deploy the Airflow DAG to a managed Airflow environment (Cloud Composer).
    *   Set up Cloud Monitoring alerts for job failures or performance degradations.

**Order of Operations for Build:**

1.  Develop BigQuery DDL for all involved tables.
2.  Implement data ingestion pipelines for `sof_ta_vertrag_tmp` and `isbert_schema.dwtk_meldungen` from CARMEN DB to BigQuery.
3.  Translate `d_ausd_v_ta_p_vertrag.sql` to `d_ausd_v_ta_p_vertrag_bq.sql`.
4.  Develop `dw_bert_ausd_v_ta_p_vertrag_dag.py`.
5.  Perform testing (unit, integration, performance).
6.  Deploy to production.