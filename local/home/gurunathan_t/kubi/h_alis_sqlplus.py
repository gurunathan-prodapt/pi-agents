#!/usr/bin/env python3
import os
import sys
import subprocess
import f_alis_msgerr

# Step 1: Initialize module-level metadata
ModulName = "alis_sqlplus"
ModulVersion = "V1.1.3"
Modul_Name = ModulName
Modul_Version = ModulVersion

def starteSQLSkript(p_Eintragsnr: str, p_Skript: str, *args) -> int:
    """
    Validates and executes a SQL script.
    Uses the BigQuery client library as the primary execution path,
    with a fallback to Oracle SQL*Plus if configured or if BigQuery is unavailable.
    
    :param p_Eintragsnr: Fehlereintragsnummer (error log ID)
    :param p_Skript: Path to the SQL script to be run
    :param args: Dynamic arguments to pass to the SQL script
    :return: Exit status of the validation or SQL execution
    """
    # Step 3: Validate mandatory arguments are not null or empty
    if not p_Eintragsnr or not p_Skript:
        f_alis_msgerr.DWMSG_MeldeFehler(
            p_Eintragsnr, 
            "E", 
            "196", 
            f"{Modul_Name} {Modul_Version} starteSQLSkript"
        )
        return 196

    # Step 4: Validate that the target script file exists and is readable
    if not os.path.isfile(p_Skript) or not os.access(p_Skript, os.R_OK):
        f_alis_msgerr.DWMSG_MeldeFehler(
            p_Eintragsnr, 
            "E", 
            "201", 
            p_Skript
        )
        return 201

    # Step 5: Log execution details (exact print literals preserved)
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_Skript}")
    print(f"Skript-Parameter: {' '.join(args)}")

    # Step 6: Determine execution platform
    use_legacy = os.environ.get("USE_LEGACY_SQLPLUS", "FALSE").upper() == "TRUE"

    if not use_legacy:
        try:
            from google.cloud import bigquery
            
            # Retrieve global environment variables
            gcp_project = os.environ.get("GCP_PROJECT")
            bq_location = os.environ.get("BQ_LOCATION")
            
            client = bigquery.Client(project=gcp_project, location=bq_location)
            
            with open(p_Skript, "r", encoding="utf-8") as f:
                sql_content = f.read()
            
            # Replace positional parameters &1, &2, etc.
            for i, arg in enumerate(args, start=1):
                sql_content = sql_content.replace(f"&{i}", arg)
                
            query_job = client.query(sql_content)
            query_job.result()
            return 0
        except Exception as e:
            print(f"BigQuery execution failed or client not available: {e}. Falling back to SQL*Plus.", file=sys.stderr)

    # Step 7: Fallback to SQL*Plus execution
    dw_orauser = os.environ.get("DW_ORAUSER", "")
    cmd = ["sqlplus", dw_orauser, f"@{p_Skript}"] + list(args)
    try:
        result = subprocess.run(
            cmd,
            stdin=subprocess.DEVNULL,
            check=False
        )
        return result.returncode
    except Exception as e:
        print(f"System Error running SQL*Plus: {e}", file=sys.stderr)
        return -1