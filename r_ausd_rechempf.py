#!/usr/bin/env python3
import sys
import os
import argparse
import datetime
from google.cloud import bigquery

def usage():
    prog_name = "Initial Befuellung Vertrags-Cache FOS"
    prog_version = "V1.0.1"
    print(f"""    Programm: {prog_name}
    Version:  {prog_version}
    Aufruf:   sys.argv[0] Parameter
    Parameter:
	-h     zeigt diese Seite an
	-s     Stichtag DDMMYYYY
	-l     Wiederanlaufwert
               wird dieser Wert gesetzt, so werden nur Vertraege zu
               DWH_VERTRAG_ID > Wiederanlaufwert in die FOS-Tabelle
               geschrieben (die Eintraege bzgl. Werten >= diesem
               Wert werden geloescht)

    Beschreibung:
        Dieser Job erzeugt einen Stichtags-Abzug der Vertrags-Cache
	im DWH und stellt sie Forderungsscoring zur Verfuegung.
	Zu beachten ist hierbei, dass eine bereits bereitgestellte
	Tabelle dann geloescht wird, wenn keine aktive Vertragscache
	existiert, die noch nicht abgeholt worden ist.
	Eine solche Abholung muss vom FOS-Loader entsprechend markiert
	worden sein.
	Es werden jeweils Records selektiert, fuer die
               Gueltig_von <= Stichtag < Gueltig_bis AND
	       LADEDATUM   < Stichtag
	gilt.
	Falls der Stichtag nicht gesetzt wird, dann wird das
        MINIMUM aus aktuellem Systemdatum und maximalem Ladedatum
                (Quelltabelle)
        herangezogen.""")

def main():
    try:
        # Step 1: Parse arguments using argparse to preserve original parameter contract
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument("-h", "--help", action="store_true")
        parser.add_argument("-s", dest="p_stichtag", default=None)
        parser.add_argument("-l", dest="p_wiederanlaufWert", type=int, default=0)
        
        args, unknown = parser.parse_known_args()
        
        if args.help:
            usage()
            sys.exit(0)
            
        # Step 2: Determine dates and apply defaults
        v_sysdate = datetime.date.today().strftime('%d%m%Y')
        
        p_stichtag = args.p_stichtag
        if not p_stichtag:
            p_stichtag = v_sysdate
            
        p_wiederanlauf_wert = args.p_wiederanlaufWert
        
        # Step 3: Parameter verification (equivalent to pruefeParameterGesetzt)
        if not p_stichtag:
            print("ERROR: Required parameter 'Stichtag' is missing.", file=sys.stderr)
            usage()
            sys.exit(193) # ErrNr 193: Notwendiges Argument fehlt

        # Step 4: Setup logging framework variables and files
        job_kennung = "BERT_P_RECH_EMPF"
        
        dw_eintrags_nr = os.environ.get("DW_EINTRAGS_NR", "0")
        
        # LogDatei path determination
        log_datei_dir = os.environ.get("BERT_LOG_DIR", "/tmp")
        log_datei = os.path.join(log_datei_dir, f"{job_kennung}_{dw_eintrags_nr}.log")
        
        def log_and_print(message, to_file=True):
            print(message)
            if to_file:
                try:
                    with open(log_datei, "a") as lf:
                        lf.write(message + "\n")
                except IOError as e:
                    print(f"Warning: Could not write to log file {log_datei}: {e}", file=sys.stderr)

        log_and_print(" ----------------- Job -----------------------", to_file=False)
        log_and_print(f" Job-Nr    : '{dw_eintrags_nr}'", to_file=False)
        log_and_print(f" JobKennung: '{job_kennung}'", to_file=False)
        log_and_print(f" Logdatei  : '{log_datei}'", to_file=False)
        log_and_print(f" Stichtag  : '{p_stichtag}'", to_file=False)
        log_and_print(" ---------------------------------------------", to_file=False)

        # Step 5: Resolve environment variables and prepare execution paths
        bert_dir_root = os.environ.get("BERT_DIR_ROOT")
        if not bert_dir_root:
            raise SystemExit("BERT_DIR_ROOT must be set by the calling Airflow task")
            
        dw_dir_utl = os.environ.get("DW_DIR_UTL")
        if not dw_dir_utl:
            raise SystemExit("DW_DIR_UTL must be set by the calling Airflow task")
            
        name_sql_skript = os.path.join(bert_dir_root, "aufbereitung/sql/d_ausd_rechempf.sql")
        
        pid = os.getpid()
        tmp_file_path = os.path.join(dw_dir_utl, f"bert_k_ausd_rechempf_{pid}.tmp")
        
        # Step 6: Load SQL script and rewrite positional parameters for BigQuery
        try:
            with open(name_sql_skript, "r") as f:
                sql_text = f.read()
        except FileNotFoundError as e:
            print(f"ERROR: SQL script not found at {name_sql_skript}: {e}", file=sys.stderr)
            sys.exit(1)
            
        # Clean SQL*Plus syntax and map positional variables to named BQ parameters
        # &1 -> @eintrags_nr
        # &2 -> @job_kennung
        # &3 -> @stichtag
        # &5 -> @wiederanlauf_wert
        sql_text = sql_text.replace("&1", "@eintrags_nr")
        sql_text = sql_text.replace("&2", "@job_kennung")
        sql_text = sql_text.replace("&3", "@stichtag")
        sql_text = sql_text.replace("&5", "@wiederanlauf_wert")

        # Step 7: Execute query using native google-cloud-bigquery client
        gcp_project = os.environ.get("GCP_PROJECT")
        bq_location = os.environ.get("BQ_LOCATION")
        client = bigquery.Client(project=gcp_project, location=bq_location)
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("eintrags_nr", "STRING", dw_eintrags_nr),
                bigquery.ScalarQueryParameter("job_kennung", "STRING", job_kennung),
                bigquery.ScalarQueryParameter("stichtag", "STRING", p_stichtag),
                bigquery.ScalarQueryParameter("wiederanlauf_wert", "INT64", p_wiederanlauf_wert),
            ]
        )
        
        query_job = client.query(sql_text, job_config=job_config)
        # Wait for the query to complete
        query_job.result()
        
        # Get count of modified rows
        v_records = 0
        if query_job.num_dml_affected_rows is not None:
            v_records = query_job.num_dml_affected_rows
        elif query_job.total_rows is not None:
            v_records = query_job.total_rows
            
        # Step 8: Write count to temporary file and read it back to match legacy metrics workflow
        try:
            with open(tmp_file_path, "w") as tf:
                tf.write(str(v_records))
        except IOError as e:
            print(f"Warning: Could not write record count to {tmp_file_path}: {e}", file=sys.stderr)
            
        v_records_read = "0"
        try:
            if os.path.exists(tmp_file_path):
                with open(tmp_file_path, "r") as tf:
                    v_records_read = tf.read().strip()
                os.remove(tmp_file_path)
            else:
                v_records_read = str(v_records)
        except Exception as e:
            print(f"Warning: Error cleaning up temporary file {tmp_file_path}: {e}", file=sys.stderr)
            v_records_read = str(v_records)
            
        # Step 9: Final status reporting
        log_and_print("Die Abarbeitung wurde ohne erkennbare Fehler beendet")
        sys.exit(0)
        
    except KeyboardInterrupt:
        # Replaces trap on INT STOP CONT
        print("OSError: Abbruch", file=sys.stderr)
        sys.exit(1)
    except SystemExit as e:
        if isinstance(e.code, int):
            sys.exit(e.code)
        else:
            sys.exit(1 if e.code else 0)
    except Exception as e:
        # Replaces trap on ERR
        print("AppError: Abbruch", file=sys.stderr)
        print(f"Error details: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    sys.exit(main())