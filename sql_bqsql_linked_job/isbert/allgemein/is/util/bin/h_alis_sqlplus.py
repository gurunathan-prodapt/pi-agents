#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Zweck:
#    Hilfsroutinen fuer die Benutzung von SQL*Plus / BigQuery
#
# Erzeugt von : TJ
# Erzeugt am  : 18.02.98
# Portiert nach Python 3

import os
import sys
import argparse
import subprocess

# Step 1: Initialize module-level metadata
MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"


# Step 2: Define helper function to call external error reporter
def _dwmsg_melde_fehler(p_eintragsnr: str, msg_type: str, err_code: int, msg_text: str) -> None:
    """
    Helper to run the external error logger DWMSG_MeldeFehler.
    """
    # REVIEW-STRUCT: original launcher call preserved below — replace with the GCP-native equivalent once
    # the launcher's internal behaviour (logging, error propagation, credential injection) is confirmed.
    cmd = ["DWMSG_MeldeFehler", p_eintragsnr, msg_type, str(err_code), msg_text]
    try:
        subprocess.run(cmd, check=True)
    except FileNotFoundError:
        print("shutil.which warning: DWMSG_MeldeFehler not found in PATH.", file=sys.stderr)
        print(f"Logged Error {err_code} ({msg_type}) for entry {p_eintragsnr}: {msg_text}", file=sys.stderr)
    except subprocess.CalledProcessError as e:
        print(f"Error calling DWMSG_MeldeFehler: {e}", file=sys.stderr)


# Step 3: Define starte_sql_skript function
def starte_sql_skript(p_eintragsnr: str, p_skript: str, *args: str) -> int:
    """
    Python equivalent of 'starteSQLSkript' function.
    Validates script path, logs details, and executes on BigQuery or SQL*Plus.
    """
    
    # Step 4: Validate inputs
    if not p_eintragsnr or not p_skript:
        # Note: Resolves the original typo where Modul_Name was used instead of ModulName
        error_msg = f"{MODUL_NAME} {MODUL_VERSION} starteSQLSkript"
        _dwmsg_melde_fehler(p_eintragsnr, "E", 196, error_msg)
        return 196
        
    # Step 5: Check file existence and readability
    if not os.path.isfile(p_skript) or not os.access(p_skript, os.R_OK):
        _dwmsg_melde_fehler(p_eintragsnr, "E", 201, p_skript)
        return 201

    # Step 6: Log execution metadata (OUTPUT/PRINT LITERAL RULE: Exact original German literals preserved)
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_skript}")
    print(f"Skript-Parameter: {' '.join(args)}")

    # Check if we should use BigQuery (default for migrated target platform) or legacy SQL*Plus
    use_bq = os.environ.get("USE_BIGQUERY", "True").lower() in ("true", "1", "yes")
    dw_orauser = os.environ.get("DW_ORAUSER")

    if use_bq and not dw_orauser:
        # Execute via BigQuery client
        try:
            from google.cloud import bigquery
            
            project = os.environ.get("GCP_PROJECT")
            location = os.environ.get("BQ_LOCATION")
            client = bigquery.Client(project=project, location=location)
            
            with open(p_skript, 'r', encoding='utf-8') as f:
                query_text = f.read()
            
            # Simple SQL*Plus style parameter substitution (&1, &2, etc. with args)
            # Oracle SQL*Plus scripts use &1, &2, etc. as positional parameters.
            for idx, arg_val in enumerate(args, start=1):
                query_text = query_text.replace(f"&{idx}", arg_val)
                query_text = query_text.replace(f"&&{idx}", arg_val)

            # Clean up SQL*Plus directives which are syntax errors in BigQuery
            cleaned_lines = []
            for line in query_text.splitlines():
                trimmed = line.strip().upper()
                if any(trimmed.startswith(cmd) for cmd in ["SET ", "WHENEVER ", "EXIT", "COLUMN ", "DEFINE ", "UNDEFINE ", "SPOOL ", "PROMPT "]):
                    continue
                cleaned_lines.append(line)
            query_text = "\n".join(cleaned_lines)

            # Run query on BigQuery
            query_job = client.query(query_text)
            query_job.result()  # Wait for execution to complete
            errcode = 0
        except Exception as e: 
            print(f"BigQuery execution failed: {e}", file=sys.stderr)
            errcode = 1
    else:
        # Fallback to legacy SQL*Plus via subprocess
        if not dw_orauser:
            print("Error: Neither BigQuery execution is enabled nor DW_ORAUSER is set.", file=sys.stderr)
            return 1
        
        sqlplus_cmd = ["sqlplus", dw_orauser, f"@{p_skript}"] + list(args)
        try:
            # Stdin is redirected to subprocess.DEVNULL to prevent hangs (equivalent to </dev/null)
            result = subprocess.run(sqlplus_cmd, stdin=subprocess.DEVNULL, check=False)
            errcode = result.returncode
        except FileNotFoundError:
            print("Error: sqlplus executable not found in PATH.", file=sys.stderr)
            return 127

    return errcode


def main() -> int:
    parser = argparse.ArgumentParser(description="Run SQL*Plus/BigQuery script via helper utility")
    parser.add_argument("p_eintragsnr", help="Error entry number")
    parser.add_argument("p_skript", help="SQL script file path")
    parser.add_argument("args", nargs="*", help="Additional arguments for SQL script")
    
    parsed_args = parser.parse_args()
    
    return starte_sql_skript(parsed_args.p_eintragsnr, parsed_args.p_skript, *parsed_args.args)


if __name__ == "__main__":
    sys.exit(main())