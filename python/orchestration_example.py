# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
# Target Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh

"""
This Python script serves as an example orchestration layer for the migrated BigQuery
error handling and logging procedures. It demonstrates how to interact with the
BigQuery stored procedures that replaced the f_alis_msgerr.ksh functionality.

Replace `my_project`, `my_dataset` with your actual BigQuery project and dataset IDs.
This script requires the `google-cloud-bigquery` library to be installed.
"""

from google.cloud import bigquery
import datetime
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class BigQueryErrorLogger:
    def __init__(self, project_id: str, dataset_id: str):
        self.client = bigquery.Client(project=project_id)
        self.dataset_ref = self.client.dataset(dataset_id, project=project_id)
        self.project_id = project_id
        self.dataset_id = dataset_id
        logging.info(f"Initialized BigQueryErrorLogger for project '{project_id}', dataset '{dataset_id}'")

    def _execute_procedure(self, procedure_name: str, params: list = None):
        qualified_procedure_name = f"`{self.project_id}.{self.dataset_id}.{procedure_name}`"
        
        call_params = []
        if params:
            for p in params:
                if p is None:
                    call_params.append("NULL")
                else:
                    call_params.append(f"'{p}'")
        
        query = f"CALL {qualified_procedure_name}({', '.join(call_params)})"

        logging.info(f"Executing BigQuery procedure: {query}")
        query_job = self.client.query(query)
        query_job.result()  # Waits for the job to complete
        logging.info(f"Procedure '{procedure_name}' executed successfully.")
        return None

    def dwmsg_ermittle_nr(self) -> str:
        """Generates a unique entry number."""
        qualified_procedure_name = f"`{self.project_id}.{self.dataset_id}.dwmsg_ermittle_nr`"
        query = f"""
        DECLARE entry_nr STRING;
        CALL {qualified_procedure_name}(entry_nr);
        SELECT entry_nr;
        """
        logging.info(f"Executing BigQuery procedure for unique ID: {query}")
        query_job = self.client.query(query)
        result = [row[0] for row in query_job.result()]
        if not result:
            raise RuntimeError("Failed to generate entry number.")
        entry_nr = result[0]
        logging.info(f"Generated entry number: {entry_nr}")
        return entry_nr

    def dwmsg_erzeuge_eintrag(self, eintrags_nr: str, job_kennung: str, programm_name: str, log_datei: str, typ: str):
        """Creates a new message/log entry."""
        self._execute_procedure(
            "dwmsg_erzeuge_eintrag",
            [eintrags_nr, job_kennung, programm_name, log_datei, typ]
        )

    def dwmsg_setze_status_ok(self, eintrags_nr: str):
        """Sets the status of an entry to 'OK'."""
        self._execute_procedure("dwmsg_setze_status_ok", [eintrags_nr])

    def dwmsg_setze_status_abbruch(self, eintrags_nr: str):
        """Sets the status of an entry to 'ABORTED'."""
        self._execute_procedure("dwmsg_setze_status_abbruch", [eintrags_nr])

    def dwmsg_melde_fehler(self, eintrags_nr: str, fehler_nr: str, fehler_text: str, zusatz_info_1: str = None, zusatz_info_2: str = None, variable_name: str = None):
        """Records error details."""
        self._execute_procedure(
            "dwmsg_melde_fehler",
            [eintrags_nr, fehler_nr, fehler_text, zusatz_info_1, zusatz_info_2, variable_name]
        )

    def dwmsg_logdateiname(self, job_kennung: str, programm_name: str) -> str:
        """Constructs a standardized log filename string."""
        qualified_procedure_name = f"`{self.project_id}.{self.dataset_id}.dwmsg_logdateiname`"
        query = f"""
        DECLARE log_file STRING;
        CALL {qualified_procedure_name}('{job_kennung}', '{programm_name}', log_file);
        SELECT log_file;
        """
        logging.info(f"Executing BigQuery procedure for log filename: {query}")
        query_job = self.client.query(query)
        result = [row[0] for row in query_job.result()]
        if not result:
            raise RuntimeError("Failed to generate log filename.")
        log_filename = result[0]
        logging.info(f"Generated log filename: {log_filename}")
        return log_filename

    def dwmsg_setze_stichtag_info(self, eintrags_nr: str, stichtag_value: str, stichtag_format: str, info_text: str):
        """Updates zusatz_infos with stichtag information."""
        self._execute_procedure(
            "dwmsg_setze_stichtag_info",
            [eintrags_nr, stichtag_value, stichtag_format, info_text]
        )

    def dwmsg_append_timing_infos(self, eintrags_nr: str, timing_info_key: str, timing_info_value: str):
        """Appends timing information to zusatz_infos."""
        self._execute_procedure(
            "dwmsg_append_timing_infos",
            [eintrags_nr, timing_info_key, timing_info_value]
        )
            
    def dwmsg_fehlerbehandlung(self, eintrags_nr: str, job_kennung: str, programm_name: str, error_code: str, error_message: str):
        """Handles generic error processing."""
        # This procedure internally calls other procedures and signals an error.
        # The Python client will catch the SIGNAL SQLSTATE as an exception.
        try:
            self._execute_procedure(
                "dwmsg_fehlerbehandlung",
                [eintrags_nr, job_kennung, programm_name, error_code, error_message]
            )
        except Exception as e:
            logging.error(f"DWMSG_Fehlerbehandlung completed with BigQuery error: {e}")
            # Re-raise or handle as appropriate for the orchestration layer
            raise

if __name__ == "__main__":
    # --- Configuration ---
    # !!! IMPORTANT: Replace these with your actual BigQuery project and dataset IDs !!!
    PROJECT_ID = "my_project" # e.g., "your-gcp-project-id"
    DATASET_ID = "my_dataset" # e.g., "etl_logs"
    # ---------------------

    logger = BigQueryErrorLogger(PROJECT_ID, DATASET_ID)

    # --- Example Usage ---
    job_id = "EXAMPLE_JOB_001"
    program_name = "example_script.py"

    entry_nr = None
    try:
        # 1. Generate a unique entry number
        entry_nr = logger.dwmsg_ermittle_nr()

        # 2. Create a new entry
        log_file = logger.dwmsg_logdateiname(job_id, program_name) # First get a log filename
        logger.dwmsg_erzeuge_eintrag(
            eintrags_nr=entry_nr,
            job_kennung=job_id,
            programm_name=program_name,
            log_datei=log_file,
            typ="INFO"
        )
        logging.info(f"Created entry {entry_nr} for job {job_id}.")

        # 3. Update with Stichtag info
        current_date_str = datetime.datetime.now().strftime('%Y-%m-%d')
        logger.dwmsg_setze_stichtag_info(entry_nr, current_date_str, '%Y-%m-%d', 'Processing for current date.')

        # 4. Simulate some work and record timing
        start_time = time.time()
        time.sleep(0.5) # Simulate work
        end_time = time.time()
        duration_ms = int((end_time - start_time) * 1000)
        logger.dwmsg_append_timing_infos(entry_nr, 'processing_duration_ms', str(duration_ms))

        # 5. Simulate success
        logger.dwmsg_setze_status_ok(entry_nr)
        logging.info(f"Job {job_id} with entry {entry_nr} completed successfully.")

    except Exception as e:
        logging.error(f"An unexpected error occurred during job {job_id}: {e}")
        if entry_nr:
            # 6. Call error handling procedure if an error occurred
            try:
                logger.dwmsg_fehlerbehandlung(entry_nr, job_id, program_name, "PYTHON_ERROR_001", str(e))
            except Exception as fe:
                logging.error(f"Failed to call dwmsg_fehlerbehandlung: {fe}")
        else:
            logging.error("No entry_nr available to log error in BigQuery.")

    logging.info("Orchestration example finished.")