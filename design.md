# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh
## 1. Purpose & Scope
This job serves as an initial provisioning mechanism for selected base products for the BERT system. It acts as a Korn shell wrapper, preparing the runtime environment, validating input parameters, initializing logging and error handling, and determining an effective cutoff date ("Stichtag"). The core data processing logic is then delegated to a separate kernel script. The business purpose is to create a snapshot extraction of contract cache data from the Data Warehouse (DWH) and make it available for scoring and the FOS (Forderungsscoring) system. It supports a restart/resume capability via a "Wiederanlaufwert" that intelligently filters contract IDs and manages existing records in the target FOS table.

## 2. Source Inventory
The job is composed of a single KornShell script.

*   **File Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh`
*   **Technology:** KornShell
*   **Category:** shell
*   **Complexity Tier:** medium
*   **Migration Bucket:** semi_auto
*   **Migration Flags:** No specific flags identified.
*   **Purpose:** Orchestration, parameter handling, logging, and invoking a core script.

## 3. Target Architecture
The migration target is Google BigQuery. The current shell script logic, primarily focused on orchestration and parameter handling, will be migrated to a BigQuery Stored Procedure.

*   **Main Component:** A BigQuery Stored Procedure (`project.dataset.ausd_bp_ta_bpr_opt_text`) will encapsulate the wrapper's logic, including parameter parsing, date calculations, and restart handling.
*   **Logging and Auditing:** The script's logging and error handling will be replaced by inserts into dedicated BigQuery audit and log tables (e.g., `project.dataset.job_audit` and `project.dataset.job_log`).
*   **Core Logic:** The logic currently residing in the invoked kernel script (`k_ausd_bp_ta_bpr_opt_text.ksh`) will need to be analyzed separately and subsequently migrated into the main BigQuery Stored Procedure or a set of dependent BigQuery SQL statements/procedures.
*   **Orchestration:** While the immediate script functionality will be a BigQuery Stored Procedure, external orchestration using Cloud Workflows or Cloud Composer (for an Airflow DAG) might be considered if the overall BERT job stream requires more complex inter-job dependencies or scheduling beyond BigQuery's native capabilities.

## 4. Data Flow & Lineage
The `r_ausd_bp_ta_bpr_opt_text.ksh` script performs the following logical steps:

1.  **Initialization:** Sources environment variables (`$HOME/.dw_init`) and helper scripts for error handling, parameter parsing, and date manipulation.
2.  **Parameter Parsing:** Reads command-line arguments (`-s` for Stichtag/cutoff date, `-l` for Wiederanlaufwert/restart threshold).
3.  **Parameter Defaulting:** Initializes `p_wiederanlaufWert` to 0 if not provided. Determines `p_stichtag` (cutoff date), defaulting to the system date if not explicitly set.
4.  **Parameter Validation:** Checks if necessary parameters are set and exits with an error if not.
5.  **Logging Setup:** Sets up job identifiers, log file names, and initial log entries using internal DWMSG framework functions.
6.  **Core Script Invocation:** Invokes a "kernel script" (`${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_opt_text.ksh`) passing all determined parameters. This invoked script is where the actual data extraction and transformation are expected to occur.
    *   **Inferred Read:** The pseudocode suggests reading from a `source_contract_cache` (likely `DWH$TA_C_VERTRAG` or similar from the original system based on comments).
    *   **Inferred Write:** The pseudocode suggests writing to a `fos_table`.
7.  **Post-execution Logging:** Records success or failure status into log files via DWMSG functions.

The script acts as an entry point, orchestrating the execution of a more complex data processing task. The primary data flow involves reading data from internal DWH sources (as implied by the `k_ausd_bp_ta_bpr_opt_text.ksh` context and the job description) and writing to a target FOS table.

## 5. Transformation Logic
The `r_ausd_bp_ta_bpr_opt_text.ksh` script itself implements the following transformation/orchestration logic:

*   **Parameter Handling:**
    *   `p_stichtag` (cutoff date - `DDMMYYYY`): Passed as `STRING`, parsed to `DATE`. Defaults to `CURRENT_DATE()` if not provided.
    *   `p_wiederanlaufWert` (restart value): Passed as `INT64`. Defaults to 0 if not provided.
*   **Date Determination:** Uses `DWDate_Gib_Zeitraum` (equivalent to `CURRENT_DATE()` in BQ) to get the current system date. If `p_stichtag` is not provided, it is set to the system date.
*   **Error Handling:** Utilizes `f_alis_msgerr.ksh` and `DWMSG_` functions for error logging and exits. In BigQuery, this will be handled by `EXCEPTION WHEN ERROR THEN` blocks and inserts into audit tables.
*   **Restart Logic:** If `p_wiederanlaufWert` > 0, existing records in the target `fos_table` with `DWH_VERTRAG_ID >= p_wiederanlaufWert` are deleted before new data is inserted. This ensures idempotent processing for restarts.
*   **Core Processing (Delegated):** The script passes control and parameters to `k_ausd_bp_ta_bpr_opt_text.ksh`. The pseudocode suggests this core script selects records based on:
    *   `GUELTIG_VON <= v_stichtag`
    *   `v_stichtag < GUELTIG_BIS`
    *   `LADEDATUM < v_stichtag`
    *   And if restart is active: `DWH_VERTRAG_ID > v_wiederanlaufWert`
    These conditions are critical and will be translated directly into BigQuery SQL `WHERE` clauses.

## 6. External Dependencies
The current script has several dependencies that need to be addressed in the BigQuery migration:

*   **Helper Scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`):** These shell utility scripts handle environment setup, error management, parameter parsing, and date calculations. In BigQuery, their functionality will be absorbed directly into the Stored Procedure logic (e.g., `CURRENT_DATE()`, `PARSE_DATE` for dates; explicit parameter handling for arguments; `EXCEPTION` blocks for error management; configuration for environment variables).
*   **DWMSG Framework Functions:** Functions like `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_Fehlerbehandlung`, `DWMSG_MeldeFehler`, `DWMSG_SetzeStatusOK` are used for logging and status management. These will be replaced by inserts into dedicated BigQuery logging and audit tables.
*   **Kernel Script (`k_ausd_bp_ta_bpr_opt_text.ksh`):** This is the most significant dependency. Its entire logic needs to be migrated into the BigQuery Stored Procedure as direct SQL operations (SELECT, INSERT, DELETE, MERGE) or potentially into a separate Python script callable from BigQuery (e.g., a BigQuery Remote Function via Cloud Run) if it contains highly procedural or external system interactions not suitable for pure SQL.
*   **UC4 Job (implied by lineage):** The `lineage_edges` showed that an UC4 XML job (`DW.BERT_AUSD_BP_TA_BPR_OPT_TEXT.xml`) invokes this script. The UC4 orchestration will need to be migrated to Airflow (via Cloud Composer) or another suitable BigQuery-native scheduler (e.g., scheduled queries, Dataform).

## 7. Unresolved / Risks

*   **Core Logic in `k_ausd_bp_ta_bpr_opt_text.ksh`:** The actual data extraction and transformation logic resides in the invoked kernel script. This script was not part of the initial `component_files` and its content is unknown. **Risk:** Without analyzing `k_ausd_bp_ta_bpr_opt_text.ksh`, the full scope and complexity of the data transformations, source tables, and target table schema cannot be accurately determined, potentially leading to incomplete or incorrect migration. This script needs to be analyzed to complete the design.
*   **Dynamic SQL/Table Names:** The current script has comments like `FOSHoleLadedatum "DWH\\$TA_C_VERTRAG"`. While commented out, this suggests potential for dynamic table or view references. The kernel script might also use dynamic SQL. **Risk:** If dynamic table names are used, a robust strategy for mapping these to BigQuery objects is required, potentially involving metadata-driven pipelines.
*   **Date Handling (DDMMYYYY):** The script uses `DDMMYYYY` format for dates. BigQuery's native date functions will handle this, but explicit `PARSE_DATE` and `FORMAT_DATE` will be necessary to ensure correct interpretation and consistency.
*   **Restart Deletion Strategy:** The current `DELETE` for `p_wiederanlaufWert` relies on `DWH_VERTRAG_ID`. If the target `fos_table` in BigQuery is partitioned or clustered, an optimized delete strategy leveraging these features should be implemented for performance.
*   **External System Interactions:** While `lineage_external_systems` was empty for this specific job, the helper scripts or the kernel script might interact with other external systems (e.g., databases not directly in DWH, SFTP). If identified in the kernel script analysis, these will need separate migration plans (e.g., Cloud Storage for files, federated queries, Dataflow).

## 8. Build Plan

1.  **Analyze `k_ausd_bp_ta_bpr_opt_text.ksh`:** Obtain and analyze the content of the kernel script to fully understand its data sources, target schema, and transformation logic. This is critical for completing the design.
2.  **Define BigQuery Target Schema:** Create DDL for the `fos_table` (target table) and any other tables inferred from the kernel script's analysis.
3.  **Define BigQuery Audit & Log Tables:** Create DDL for `job_audit` and `job_log` tables to capture execution metadata, status, and messages.
4.  **Develop BigQuery Stored Procedure (Wrapper):** Write the BigQuery SQL for the `project.dataset.ausd_bp_ta_bpr_opt_text` stored procedure, implementing:
    *   Parameter declaration and handling.
    *   Date calculation logic.
    *   Logging and error handling (inserts into audit/log tables).
    *   Restart `DELETE` logic.
5.  **Implement Core Transformation Logic:** Integrate the logic from `k_ausd_bp_ta_bpr_opt_text.ksh` into the BigQuery Stored Procedure, likely as `INSERT INTO ... SELECT FROM ...` statements or `MERGE` statements.
6.  **Migrate UC4 Orchestration:** Convert the UC4 job (`DW.BERT_AUSD_BP_TA_BPR_OPT_TEXT.xml`) into an Airflow DAG (Python) in Cloud Composer or a scheduled BigQuery query that invokes the new BigQuery Stored Procedure.
7.  **Testing:** Thoroughly test the BigQuery Stored Procedure for functional correctness, performance, and idempotency, especially for the restart logic.
8.  **Deployment:** Deploy the BigQuery DDL and Stored Procedure, and the new orchestration component.