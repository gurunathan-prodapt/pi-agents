# MIGRATION DESIGN DOCUMENT

## VERBATIM MCP TOOL OUTPUT

Below is the complete, verbatim output from the `ksh_design_python` tool.

```markdown
=== FILE: ksh_unsupported/d_call_sp_template.ksh ===
#!/bin/ksh_dwh
#                               -*- Mode: Sh -*- 
# d_call_sp_template.ksh --- Beispiel fuer einen SP Aufruf von Shell aus
# Autor               : Thomas Bregulla
# Erzeugt am          : Fri Feb  6 23:08:32 1998
# Letzte Aenderung von: Thomas Bregulla
# Letzte Aenderung am : Fri Feb  6 23:11:33 1998
# Status              : Unbekannt, bitte Vorsicht!
# $Id$
# $Locker$
# Versions-Anmerkungen
# $Log$
# 
# Zweck
#     Beispielhafter Aufruf einer Stored Procedure

. $DWH_ROOT/.dwh_init

fachl_name1=$1
fachl_name2=$2

sqlplus $user/$pass <<EOF
   start d_call_sp_template.sql $fachl_name1 $fachl_name2
EOF


# DESIGN DOCUMENT: d_call_sp_template.ksh Conversion

## 1. SCRIPT OVERVIEW
The `d_call_sp_template.ksh` script is an exemplary legacy KornShell template designed to orchestrate the execution of an Oracle Stored Procedure from a shell environment. It is triggered externally (typically by an enterprise scheduler such as UC4/Automic), reads two positional command-line parameters, and executes an external SQL script (`d_call_sp_template.sql`) via Oracle SQL\*Plus using credentials sourced from an environment initialization file. This script serves as a reusable operational pattern for integrating database-side stored procedures into batch-processing workflows.

---

## 2. INVOCATION CONTEXT
*   **Caller / Orchestrator:** Typically invoked by a UC4/Automic Job (e.g., within a `JOBS_UNIX` object). The command line call typically passes two arguments:
    ```bash
    d_call_sp_template.ksh <fachl_name1> <fachl_name2>
    ```
*   **UC4 Native Includes:** 
    *   *None referenced in this extraction.*
*   **Environment Files Sourced:**
    *   `. $DWH_ROOT/.dwh_init`
    *   # REVIEW-STRUCT: environment file $DWH_ROOT/.dwh_init not supplied — variables it sets are unknown; do not guess their names or values

---

## 3. PARAMETERS / INPUTS
*   **fachl_name1:**
    *   *Name:* `fachl_name1` (assigned from positional parameter `$1`)
    *   *Source:* UC4 Job positional argument
    *   *Used in Body:* Yes, passed as the first parameter to the SQL\*Plus script execution
    *   *Python Mapping:* `sys.argv[1]` or parsed via `argparse.ArgumentParser()`
*   **fachl_name2:**
    *   *Name:* `fachl_name2` (assigned from positional parameter `$2`)
    *   *Source:* UC4 Job positional argument
    *   *Used in Body:* Yes, passed as the second parameter to the SQL\*Plus script execution
    *   *Python Mapping:* `sys.argv[2]` or parsed via `argparse.ArgumentParser()`
*   **user:**
    *   *Name:* `user`
    *   *Source:* Environment variable (typically initialized in `$DWH_ROOT/.dwh_init`)
    *   *Used in Body:* Yes, used for SQL\*Plus connection authentication
    *   *Python Mapping:* `os.environ.get("user")`
*   **pass:**
    *   *Name:* `pass`
    *   *Source:* Environment variable (typically initialized in `$DWH_ROOT/.dwh_init`)
    *   *Used in Body:* Yes, used for SQL\*Plus connection authentication
    *   *Python Mapping:* `os.environ.get("pass")`
*   **DWH_ROOT:**
    *   *Name:* `DWH_ROOT`
    *   *Source:* System environment variable
    *   *Used in Body:* Yes, used to resolve the path to `.dwh_init`
    *   *Python Mapping:* `os.environ.get("DWH_ROOT")`

---

## 4. EXTERNAL COMMANDS / PROGRAMS INVOKED
*   **Command Line (Verbatim):**
    ```bash
    sqlplus $user/$pass <<EOF
       start d_call_sp_template.sql $fachl_name1 $fachl_name2
    EOF
    ```
*   **Purpose:** Connects to the Oracle Database via SQL\*Plus and runs the SQL script `d_call_sp_template.sql` while passing the two positional arguments as substitution variables.
*   **Python Conversion Strategy:** 
    *   *Option A (Subprocess Preservation):* Execute `sqlplus` as an external process via `subprocess.run()`.
    *   *Option B (Native DB Client):* Migrate to native Python `oracledb` (formerly `cx_Oracle`) execution.
*   **Resolvable Launcher Evaluation:**
    *   The command is a direct call to the native `sqlplus` binary, not an opaque custom script launcher. However, because the content of `d_call_sp_template.sql` is not provided in this extraction, we cannot confirm whether it contains complex SQL\*Plus formatting commands (e.g., `SET PAGESIZE`, `SPOOL`) or direct PL/SQL blocks.
    *   *Recommendation:* Since we are targeting Oracle (unambiguously identified by `sqlplus`), the modern target should ideally use the `oracledb` library. However, because the SQL file contents are missing, we must flag this structure.
    *   # REVIEW-STRUCT: SQL file d_call_sp_template.sql not supplied — exact SQL statements, tables touched, and execution logic are unknown. If the SQL file contains only a standard PL/SQL procedure call, convert this execution to native `oracledb.Cursor.callproc()`. Otherwise, preserve as a `subprocess` call to `sqlplus`.

---

## 5. EMBEDDED SQL
*   **Source File / Label:** `d_call_sp_template.sql` (referenced inside the SQL\*Plus Heredoc)
*   **Full SQL Text (Verbatim):**
    *   *Not supplied in extraction.*
*   **Statement Type:** Unknown (highly likely a PL/SQL block or a stored procedure call, as indicated by the script header `"Beispielhafter Aufruf einer Stored Procedure"`).
*   **Table(s) Touched:** Unknown.
*   **Dialect Identification:** Oracle SQL\*Plus (unambiguously identified by the `sqlplus` wrapper command and the `start` syntax).

---

## 6. CONTROL FLOW
1.  **Environment Setup:** Sources `.dwh_init` from the path defined by `$DWH_ROOT`.
2.  **Argument Assignment:** Maps positional parameters `$1` and `$2` to local shell variables `fachl_name1` and `fachl_name2`.
3.  **Database Connection & Execution:** Launches `sqlplus` in a heredoc block, passing credentials and initiating the external SQL script with the parsed arguments.
4.  **Implicit Termination:** Exits execution once the `sqlplus` process completes.

---

## 7. ERROR HANDLING & EXIT CODES
*   **Failure Detection:** The script does not configure explicit shell-level error detection (no `set -e`, no `trap`, and no check of the `$?` exit status of `sqlplus`).
*   **Failure Action:** If SQL\*Plus fails to connect or the SQL script fails, the shell script will silently proceed to exit (and propagate the final exit status of the last executed command, which is `sqlplus`).
*   **Success Exit Code:** Implicitly relies on `sqlplus` returning `0` on success.
*   **Python Translation Strategy:** 
    *   Implement robust exception handling.
    *   If using `subprocess.run()`, enforce `check=True` to raise a `subprocess.CalledProcessError` on non-zero exit.
    *   If utilizing native `oracledb`, wrap database transactions in `try...except oracledb.Error` blocks, logging structural details and executing `sys.exit(1)` on failure.

---

## 8. OUTPUTS / SIDE EFFECTS
*   **Database Changes:** Modifies state inside the target Oracle Database (via the stored procedure executed by `d_call_sp_template.sql`).
*   **Logs:** Outputs standard execution output and error streams from SQL\*Plus directly to the console (which is captured by the UC4 run log).

---

## 9. BUSINESS SUMMARY
*   Provides a standardized, reusable template to invoke Oracle Stored Procedures from shell-based workflows.
*   Encapsulates operational database parameters and credentials securely via a sourced environment file (`.dwh_init`).
*   Enables dynamic parameter passing (`fachl_name1`, `fachl_name2`) to guide the business logic executed inside the database.
*   Facilitates integration of core database-level transformations into higher-level corporate enterprise schedulers (UC4/Automic).

---

# PSEUDOCODE OUTLINE

```python
# Step 1: Import required standard and database libraries
import os
import sys
import subprocess
import argparse

# # REVIEW-STRUCT: environment file $DWH_ROOT/.dwh_init not supplied — variables it sets are unknown; do not guess their names or values
# Note: In a productionized Python version, read these environment variables directly or parse .dwh_init if required.

def main():
    # Step 2: Set up argument parser to handle the positional parameters
    parser = argparse.ArgumentParser(description="Convert d_call_sp_template.ksh to Python")
    parser.add_argument("fachl_name1", help="First functional parameter")
    parser.add_argument("fachl_name2", help="Second functional parameter")
    args = parser.parse_args()

    # Step 3: Retrieve database credentials and environment configurations from environment variables
    dwh_root = os.environ.get("DWH_ROOT")
    db_user = os.environ.get("user")
    db_pass = os.environ.get("pass")

    # Validate credential availability
    if not db_user or not db_pass:
        print("Error: Database credentials ('user' and/or 'pass') are not defined in the environment.", file=sys.stderr)
        sys.exit(1)

    # Step 4: Execute the SQL script via SQL*Plus subprocess
    # # REVIEW-STRUCT: SQL file d_call_sp_template.sql not supplied — exact SQL statements and execution logic are unknown.
    # We maintain the legacy behavior of invoking SQL*Plus via subprocess as the primary translation fallback.
    sql_script = "d_call_sp_template.sql"
    
    # Constructing the heredoc input equivalent for SQL*Plus
    sqlplus_input = f"start {sql_script} {args.fachl_name1} {args.fachl_name2}\n"

    try:
        print(f"Starting execution of {sql_script} via sqlplus...")
        # Execute the external sqlplus command
        process_result = subprocess.run(
            ["sqlplus", "-S", f"{db_user}/{db_pass}"],
            input=sqlplus_input,
            capture_output=False, # Output goes directly to stdout/stderr for UC4 logging
            text=True,
            check=True
        )
        print("SQL*Plus execution completed successfully.")
        
    except subprocess.CalledProcessError as e:
        print(f"Error during SQL*Plus execution: {e}", file=sys.stderr)
        sys.exit(e.returncode if e.returncode else 1)
    except FileNotFoundError:
        print("Error: 'sqlplus' executable not found in system PATH.", file=sys.stderr)
        sys.exit(127)

if __name__ == "__main__":
    main()
```
```

---

## CONTEXT THE MCP COULD NOT SEE & TARGET MAPPING

Because the target platform is **BigQuery** and the orchestration platform is **Cloud Composer (Airflow)**, we translate the legacy Oracle SQL\*Plus execution pattern into a Google Cloud native approach. Rather than executing a subprocess with `sqlplus`, the target architecture calls a native BigQuery Stored Procedure via the BigQuery Python Client within a Cloud Composer DAG.

### 1. JOB DEPENDENCIES & LINEAGE
*   **Upstream Dependencies:** None discovered in the pre-collected metadata.
*   **Downstream Dependencies:** None discovered in the pre-collected metadata.
*   **Cross-Job Hand-offs:** None discovered in the pre-collected metadata.
*   **Lineage Edges:**
    *   `ksh_unsupported/d_call_sp_template.ksh` --[USES_CONFIG]--> `.DWH_INIT` (Resolved as: **NO SOURCE NEEDED / RETIRED**).

### 2. EXECUTION ORDER
*   No explicit execution ordering is prescribed in the pre-collected scheduler context. 
*   The execution flow is sequential inside the script:
    1.  Parse arguments `fachl_name1` and `fachl_name2`.
    2.  Execute BigQuery native client to call the migrated BigQuery Stored Procedure.

### 3. SCHEDULING & VARIABLES — MUST BE RETAINED
*   **Scheduling Linkage:** None present in the pre-collected metadata. The job is designed to be triggered externally (e.g., via Cloud Composer DAG trigger or enterprise schedule).
*   **Retained Variables / Dynamic Inputs:**
    *   `fachl_name1` (passed as command line positional parameter `$1`).
    *   `fachl_name2` (passed as command line positional parameter `$2`).

### 4. EXTERNAL SYSTEM REPLACEMENTS
*   **Database Engine:** Oracle Database $\rightarrow$ **BigQuery**.
*   **Database Client:** Oracle SQL\*Plus (`sqlplus`) $\rightarrow$ **Google Cloud BigQuery Client (`google-cloud-bigquery`)**.
*   **Authentication:** Clear-text credentials `$user/$pass` from `.dwh_init` $\rightarrow$ **Google Cloud IAM Authentication** (Composer Service Account running with BigQuery Admin/Data Editor roles). Credentials are fully retired.

### 5. TARGET FILE PLAN
We will create a clean, modern Python utility designed to run inside Cloud Composer or as an independent task execution. 

*   **Target File Path:** `ksh_unsupported/d_call_sp_template.py` (preserving folder structure per the Folder Integrity Rule).
*   **Language:** Python (3.11+ compatible).
*   **Sourced From:** `ksh_unsupported/d_call_sp_template.ksh`.

#### Target BigQuery-Compatible Code Implementation:
```python
#!/usr/bin/env python3
#                               -*- Mode: Sh -*- 
# d_call_sp_template.ksh --- Beispiel fuer einen SP Aufruf von Shell aus
# Autor               : Thomas Bregulla
# Erzeugt am          : Fri Feb  6 23:08:32 1998
# Letzte Aenderung von: Thomas Bregulla
# Letzte Aenderung am : Fri Feb  6 23:11:33 1998
# Status              : Unbekannt, bitte Vorsicht!
# $Id$
# $Locker$
# Versions-Anmerkungen
# $Log$
# 
# Zweck
#     Beispielhafter Aufruf einer Stored Procedure

import os
import sys
import argparse
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

def call_bigquery_stored_procedure(fachl_name1: str, fachl_name2: str):
    """
    Executes the BigQuery equivalent of the legacy Oracle Stored Procedure.
    Uses Google Cloud IAM authentication derived from the runtime environment.
    """
    # Environment variable resolutions (No prose placeholders)
    gcp_project = os.environ.get("GCP_PROJECT")
    bq_dataset = os.environ.get("BQ_DATASET")
    bq_location = os.environ.get("BQ_LOCATION", "EU")

    if not gcp_project or not bq_dataset:
        print("Error: Required environment variables GCP_PROJECT or BQ_DATASET are missing.", file=sys.stderr)
        sys.exit(1)

    # Initialize native BigQuery Client
    client = bigquery.Client(project=gcp_project, location=bq_location)

    # The migrated stored procedure identifier (fully-qualified)
    # Mapping legacy 'd_call_sp_template.sql' script invocation to BQ procedure call
    procedure_name = f"`{gcp_project}.{bq_dataset}.d_call_sp_template`"
    
    query = f"CALL {procedure_name}(@param1, @param2);"
    
    # Configure parameters to prevent SQL injection and map parameters safely
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("param1", "STRING", fachl_name1),
            bigquery.ScalarQueryParameter("param2", "STRING", fachl_name2),
        ]
    )

    try:
        print(f"Starting execution of stored procedure {procedure_name}...")
        query_job = client.query(query, job_config=job_config)
        
        # Wait for stored procedure execution to complete
        query_job.result()
        print("Stored procedure execution completed successfully in BigQuery.")

    except GoogleCloudError as e:
        print(f"Error during BigQuery stored procedure execution: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected application error occurred: {e}", file=sys.stderr)
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
        description="Migrated Python launcher for BigQuery Stored Procedure d_call_sp_template"
    )
    parser.add_argument("fachl_name1", help="First functional parameter (fachl_name1)")
    parser.add_argument("fachl_name2", help="Second functional parameter (fachl_name2)")
    args = parser.parse_args()

    call_bigquery_stored_procedure(args.fachl_name1, args.fachl_name2)

if __name__ == "__main__":
    main()
```

### 6. ENVIRONMENT-SPECIFIC VALUES CLASSIFICATION
Per the **ENVIRONMENT VALUES** rule, legacy credential/directory mappings are retired, and environment variables are classified strictly by their role in the target architecture:

1.  **GLOBAL (Environment-wide infrastructure)**
    *   `GCP_PROJECT`: Sourced via `os.environ.get("GCP_PROJECT")`. Identifies the active target GCP Project ID.
    *   `BQ_DATASET`: Sourced via `os.environ.get("BQ_DATASET")`. Identifies the BigQuery dataset containing the stored procedure.
    *   `BQ_LOCATION`: Sourced via `os.environ.get("BQ_LOCATION")` (defaults to `"EU"` if omitted). Identifies the processing region.
2.  **JOB-SPECIFIC**
    *   `fachl_name1`: Sourced dynamically at runtime via the first command-line argument (`sys.argv[1]`).
    *   `fachl_name2`: Sourced dynamically at runtime via the second command-line argument (`sys.argv[2]`).
3.  **RETIRED / OBSOLETE**
    *   `user` & `pass`: Obsolete. Replaced by Google Application Default Credentials (ADC) / Composer IAM execution role.
    *   `DWH_ROOT`: Obsolete. All target assets utilize structured paths inside Airflow/Python environments.

### 7. RISKS & MANUAL ACTIONS
*   **SOURCE: NOT FOUND — d_call_sp_template.sql — no candidate**
    *   *Risk:* The legacy SQL script `d_call_sp_template.sql` is missing from the scanned source codebase. The internal logic of the Oracle Stored Procedure (such as arguments, type signatures, and data transformations) cannot be validated.
    *   *Mitigation:* Before testing the Python workflow, database developers must manually extract the DDL of the legacy Oracle stored procedure called by `d_call_sp_template.sql`, translate it to BigQuery SQL, and deploy it as a native BigQuery stored procedure under `d_call_sp_template` in the target dataset.
*   **Missing `.dwh_init` Environmental Setup Details:**
    *   *Risk:* Sourced configuration variables in `.dwh_init` might have controlled dynamic session parameters (e.g., timezone, schemas).
    *   *Mitigation:* Handled via environment variables set directly in Cloud Composer or within Airflow variables. Ensure execution variables align with GCP standards.

---

## FILE DISPOSITION

Every file present in the pre-collected context is accounted for below.

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `ksh_unsupported/d_call_sp_template.ksh` | `ksh_unsupported/d_call_sp_template.py` | Migrates legacy KornShell script logic to native Python, calling the BigQuery Stored Procedure using GCP native SDK and IAM auth. |
| `.dwh_init` (or `.DWH_INIT`) | **Retired** | Legacy environment and shell credential setup is completely obsolete in Google Cloud IAM and Cloud Composer environments. Confirmed as NO SOURCE NEEDED by human review. |