#!/usr/bin/env python3
import os
import sys
import argparse
from pathlib import Path
from google.cloud import bigquery

# Add current folder to sys.path to resolve adjacent module imports
sys.path.append(str(Path(__file__).parent.resolve()))

try:
    from f_alis_msgerr import DWMSG_MeldeFehler
except ImportError:
    # Fallback function if f_alis_msgerr is not immediately present in the environment
    def DWMSG_MeldeFehler(p_Eintragsnr, type_, code, msg):
        print(f"[DWMSG_MeldeFehler Fallback] Eintragsnr: {p_Eintragsnr}, Type: {type_}, Code: {code}, Message: {msg}", file=sys.stderr)

ModulName = "alis_sqlplus"
ModulVersion = "V1.1.3"

def starteSQLSkript(p_Eintragsnr, p_Skript, *args):
    # Step 3: Parameter Validation
    # Check if either of the mandatory parameters is empty
    if not p_Eintragsnr or not p_Skript:
        modul_name_var = os.environ.get("Modul_Name", ModulName)
        DWMSG_MeldeFehler(p_Eintragsnr, "E", "196", f"{modul_name_var} {ModulVersion} starteSQLSkript")
        return 196

    # Step 4: File Readability Check
    # Equivalent of [ ! -r $p_Skript ]
    script_path = Path(p_Skript)
    if not script_path.exists() or not os.access(script_path, os.R_OK):
        DWMSG_MeldeFehler(p_Eintragsnr, "E", "201", str(script_path))
        return 201

    # Step 5: Log parameter configuration to stdout (Exact original German print statements preserved)
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_Skript}")
    print(f"Skript-Parameter: {' '.join(args)}")

    # Step 6: Invoke BigQuery process with parameter substitution
    try:
        # Read query content
        query_text = script_path.read_text(encoding="utf-8")
        
        # Perform positional substitution (&1, &2, etc. and &&1, &&2, etc.)
        for idx, arg in enumerate(args):
            param_num = idx + 1
            query_text = query_text.replace(f"&&{param_num}", str(arg))
            query_text = query_text.replace(f"&{param_num}", str(arg))
            
        gcp_project = os.environ.get("GCP_PROJECT")
        bq_location = os.environ.get("BQ_LOCATION")
        
        # Initialize Google Cloud BigQuery client
        client = bigquery.Client(project=gcp_project, location=bq_location)
        
        # Clean SQL*Plus specific control command statements before execution
        cleaned_lines = []
        for line in query_text.splitlines():
            trimmed = line.strip().upper()
            if trimmed.startswith(("WHENEVER ", "SET ", "COMMIT", "ROLLBACK", "EXIT")):
                continue
            cleaned_lines.append(line)
        cleaned_query_text = "\n".join(cleaned_lines)
        
        # Run query natively on BigQuery
        query_job = client.query(cleaned_query_text)
        query_job.result()  # Wait for job to complete
        errcode = 0
    except Exception as e:
        print(f"Error executing BigQuery query: {e}", file=sys.stderr)
        errcode = 1

    return errcode

def main():
    parser = argparse.ArgumentParser(description="Python helper utility to replace h_alis_sqlplus.ksh")
    parser.add_argument("p_Eintragsnr", nargs="?", default="", help="Fehlereintragsnummer (error log ID)")
    parser.add_argument("p_Skript", nargs="?", default="", help="Path to the SQL script to be executed")
    parser.add_argument("script_args", nargs="*", help="Arbitrary parameters for the SQL script")
    
    args = parser.parse_args()
    return starteSQLSkript(args.p_Eintragsnr, args.p_Skript, *args.script_args)

if __name__ == "__main__":
    sys.exit(main())