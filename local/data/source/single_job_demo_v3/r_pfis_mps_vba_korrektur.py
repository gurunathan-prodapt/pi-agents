#!/usr/bin/env python3

# Step 1: Import required standard modules
import os
import sys
import argparse
import subprocess
from datetime import datetime

# Step 2: Establish program metadata
PROG_NAME = "Korrektur VBA-IDs"
PROG_VERSION = "6.5.0"
JOB_KENNUNG = "PFIS_MPS_VBA_KORR"

def usage():
    aufruf = os.path.basename(sys.argv[0])
    print(f"""
Programm: {PROG_NAME}
Version: {PROG_VERSION}
Aufruf: {aufruf}  [-v] [-h]
Parameter:
  -v     verbose, gibt im Anschluss oder bei Fehlern direkt die Log-Datei aus
  -h     zeigt diese Seite an

Beschreibung:
   Korrektur nicht ermittelbarer VBA-IDs der MPS-Nutzungsdaten
""")

def main():
    # Sourced environment files comments:
    # REVIEW-STRUCT: environment file $HOME/.dw_init not supplied — variables it sets are unknown; do not guess their names or values
    # REVIEW-STRUCT: environment file f_alis_msgerr.ksh not supplied — variables it sets are unknown; do not guess their names or values
    # REVIEW-STRUCT: environment file h_alis_parameter.ksh not supplied — variables it sets are unknown; do not guess their names or values
    # REVIEW-STRUCT: environment file h_alis_date.ksh not supplied — variables it sets are unknown; do not guess their names or values
    # REVIEW-STRUCT: environment file h_alis_sqlplus.ksh not supplied — variables it sets are unknown; do not guess their names or values

    # Step 3: Parse command-line parameters
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-v', action='store_true', dest='verbose', default=False)
    parser.add_argument('-h', action='store_true', dest='help', default=False)
    
    args, unknown = parser.parse_known_args()
    
    if args.help:
        usage()
        sys.exit(0)
        
    p_verbose = 1 if args.verbose else 0
    
    # Step 4: Define path to SQL script
    # REVIEW-STRUCT: environment file defining DW_DIR_ROOT was not supplied. Defaulting to empty string.
    dw_dir_root = os.environ.get("DW_DIR_ROOT", "")
    korr_skript = os.path.join(dw_dir_root, "pruef/is/sql/d_pfis_mps_vba_korrektur.sql")
    
    # Step 5: Resolve job sequence/tracking ID
    # REVIEW-STRUCT: launcher DWMSG_ErmittleNr invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
    # REVIEW-STRUCT: legacy Oracle status-logging package [DWMSG] replaced with native logging — confirm target logging destination (Cloud Logging / BigQuery table) before deploying
    # REVIEW-STRUCT: launcher DWMSG_ErmittleNr is an external binary/utility whose source code is unsupplied.
    dw_eintrags_nr = os.environ.get("DW_EintragsNr")
    if not dw_eintrags_nr:
        try:
            res = subprocess.run(["DWMSG_ErmittleNr"], capture_output=True, text=True, check=True)
            dw_eintrags_nr = res.stdout.strip()
        except (subprocess.CalledProcessError, FileNotFoundError):
            dw_eintrags_nr = datetime.now().strftime("%Y%m%d%H%M%S")
        
    # Step 6: Determine log file location
    # REVIEW-STRUCT: launcher DWMSG_Logdateiname invoked — internal behaviour not available in this extraction
    # REVIEW-STRUCT: launcher DWMSG_Logdateiname is an external utility.
    log_datei = os.environ.get("LogDatei")
    if not log_datei:
        try:
            res = subprocess.run(["DWMSG_Logdateiname", JOB_KENNUNG, dw_eintrags_nr], capture_output=True, text=True, check=True)
            log_datei = res.stdout.strip()
        except (subprocess.CalledProcessError, FileNotFoundError):
            log_datei = f"{JOB_KENNUNG}_{dw_eintrags_nr}.log"
        
    # Step 7: Write runtime banners to log file and stdout (implements tee fix)
    log_dir = os.path.dirname(log_datei)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)

    try:
        with open(log_datei, "w") as f_log:
            f_log.write("--------------------------- Job ------------------------------------\n")
            f_log.write(f"Jobkennung :  {JOB_KENNUNG}\n")
            f_log.write(f"Job-Nr     :  {dw_eintrags_nr}\n")
            f_log.write(f"Logdatei   :  {log_datei}\n")
            f_log.write("--------------------------------------------------------------------\n")
        print("--------------------------- Job ------------------------------------")
        print(f"Jobkennung :  {JOB_KENNUNG}")
        print(f"Job-Nr     :  {dw_eintrags_nr}")
        print(f"Logdatei   :  {log_datei}")
        print("--------------------------------------------------------------------")
    except Exception as e:
        print(f"Error writing to log file {log_datei}: {e}", file=sys.stderr)
        sys.exit(1)
        
    # Step 8: Register execution start in tracking system
    # REVIEW-STRUCT: launcher DWMSG_ErzeugeEintrag invoked — internal behaviour not available in this extraction
    # REVIEW-STRUCT: launcher DWMSG_ErzeugeEintrag is an external utility.
    try:
        subprocess.run(["DWMSG_ErzeugeEintrag", dw_eintrags_nr, JOB_KENNUNG, sys.argv[0], log_datei], check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass

    # Setup execution state tracking for cleanup traps
    execution_success = False
    return_code = 0

    try:
        # Step 9: Map execution bridge variables
        p_eintrags_nr = dw_eintrags_nr
        p_sql_skript = korr_skript
        
        print("---------- Ausgabe Parameter --------------")
        print(f"Eintragnr.          : {p_eintrags_nr}")
        print("-------------------------------------------")
        
        try:
            with open(log_datei, "a") as f_log:
                f_log.write("---------- Ausgabe Parameter --------------\n")
                f_log.write(f"Eintragnr.          : {p_eintrags_nr}\n")
                f_log.write("-------------------------------------------\n")
        except Exception:
            pass
        
        # Step 10: Check if physical SQL file exists on server filesystem path
        check_path = "/vobs/dw_source/isdwh/pruef/is/sql/d_pfis_mps_vba_korrektur.sql"
        if os.path.exists(check_path):
            status_msg = "Gefunden"
        else:
            status_msg = "nicht gefunden"
            
        print(status_msg)
        try:
            with open(log_datei, "a") as f_log:
                f_log.write(f"{status_msg}\n")
        except Exception:
            pass
            
        # Step 11: Execute the SQL script
        # Target platform is confirmed as BIGQUERY.
        # We execute the SQL script using the google-cloud-bigquery client.
        print(f"Executing SQL script: {p_sql_skript}")
        try:
            from google.cloud import bigquery
            
            # Read SQL file
            with open(p_sql_skript, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # Replace legacy SQL*Plus positional parameters
            sql_content = sql_content.replace("&1", str(p_eintrags_nr))
            sql_content = sql_content.replace("&2", str(p_sql_skript))
            sql_content = sql_content.replace("&3", str(p_eintrags_nr))
            
            gcp_project = os.environ.get("GCP_PROJECT")
            client = bigquery.Client(project=gcp_project)
            
            # Bind p_eintrags_nr to @p_eintrags_nr as expected by the migrated SQL script (implements query job parameter fix)
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("p_eintrags_nr", "STRING", p_eintrags_nr),
                ]
            )
            
            query_job = client.query(sql_content, job_config=job_config)
            query_job.result()  # Wait for query to complete
            
            return_code = 0
            msg = "BigQuery SQL execution completed successfully."
            print(msg)
            with open(log_datei, "a") as f_log:
                f_log.write(f"{datetime.now().isoformat()} - INFO - {msg}\n")
                
        except Exception as bq_err:
            # Fallback to starteSQLSkript if BigQuery client fails or is not available
            fallback_msg = f"BigQuery execution failed or client not available: {bq_err}. Attempting fallback to starteSQLSkript..."
            print(fallback_msg, file=sys.stderr)
            with open(log_datei, "a") as f_log:
                f_log.write(f"{datetime.now().isoformat()} - WARNING - {fallback_msg}\n")
                
            try:
                with open(log_datei, "a") as f_log:
                    res_sql = subprocess.run(
                        ["starteSQLSkript", p_eintrags_nr, p_sql_skript, p_eintrags_nr],
                        stdout=f_log,
                        stderr=subprocess.STDOUT,
                        check=True
                    )
                    return_code = res_sql.returncode
            except subprocess.CalledProcessError as e:
                return_code = e.returncode
            except FileNotFoundError:
                return_code = 127
                with open(log_datei, "a") as f_log:
                    f_log.write("starteSQLSkript command not found.\n")

        # Step 12: Handle SQL execution failure
        if return_code != 0:
            try:
                with open(log_datei, "a") as f_log:
                    f_log.write("Fehler im Kernskript aufgetreten!\n")
            except Exception:
                pass
            print("Fehler im Kernskript aufgetreten!", file=sys.stderr)
            sys.exit(return_code)
            
        execution_success = True
        
    except Exception as exc:
        # Step 13: Exception handling block (analogous to KSH traps on error)
        # Print the exact original error messages
        print("!FEHLER gemeldet!", file=sys.stderr)
        print("!OSFEHLER gemeldet!", file=sys.stderr)
        
        err_msg = f"Exception occurred: {str(exc)}"
        print(err_msg, file=sys.stderr)
        try:
            with open(log_datei, "a") as f_log:
                f_log.write("!FEHLER gemeldet!\n")
                f_log.write("!OSFEHLER gemeldet!\n")
                f_log.write(f"{datetime.now().isoformat()} - ERROR - {err_msg}\n")
                
            # Run legacy error handling if available
            subprocess.run(["DWMSG_Fehlerbehandlung", dw_eintrags_nr], check=True)
        except Exception:
            pass
        
        if p_verbose == 1:
            if os.path.exists(log_datei):
                try:
                    with open(log_datei, "r") as f_log:
                        print(f_log.read())
                except Exception as e:
                    print(f"Error reading log file: {e}", file=sys.stderr)
        sys.exit(1)
        
    finally:
        # Step 14: Finalize execution tracking and log verbose traces
        if execution_success:
            # REVIEW-STRUCT: launcher DWMSG_SetzeStatusOK invoked — internal behaviour not available in this extraction
            # REVIEW-STRUCT: launcher DWMSG_SetzeStatusOK is an external tracking wrapper.
            try:
                subprocess.run(["DWMSG_SetzeStatusOK", dw_eintrags_nr], check=True)
            except (subprocess.CalledProcessError, FileNotFoundError):
                pass
                
            try:
                with open(log_datei, "a") as f_log:
                    f_log.write("Abarbeitung ohne erkennbare Fehler beendet\n")
                print("Abarbeitung ohne erkennbare Fehler beendet")
            except Exception:
                pass
                
        if p_verbose == 1:
            print("-- Logdatei --")
            if os.path.exists(log_datei):
                try:
                    with open(log_datei, "r") as f_log:
                        print(f_log.read())
                except Exception as e:
                    print(f"Error reading log file: {e}", file=sys.stderr)
            print("-- Logdatei Ende --")
            
        if execution_success:
            sys.exit(0)
        else:
            sys.exit(return_code if return_code != 0 else 1)

if __name__ == "__main__":
    sys.exit(main())