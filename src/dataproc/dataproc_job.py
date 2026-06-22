#
# PySpark/Python script for DW.BERT_AUSD_BP_TA_RN_VERTRAG
# Legacy source: r_ausd_bp_ta_rn_vertrag.ksh, k_ausd_bp_ta_rn_vertrag.ksh,
#                gestern.ksh, h_alis_date.ksh, f_alis_msgerr.ksh, h_alis_parameter.ksh
# Target platform: Google Cloud Dataproc / BigQuery
#

import argparse
import datetime
import logging
import os
import sys

from google.cloud import bigquery

# Configure logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BertJob:
    """
    Orchestrates the BERT contract phone number aggregation.
    Replaces logic from r_ausd_bp_ta_rn_vertrag.ksh and k_ausd_bp_ta_rn_vertrag.ksh.
    """
    def __init__(self, project_id, dataset_id, sql_file_path, stichtag_str=None, wiederanlaufwert=0):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.sql_file_path = sql_file_path
        self.wiederanlaufwert = wiederanlaufwert

        self.stichtag = self._parse_stichtag(stichtag_str)
        self.client = bigquery.Client(project=self.project_id)
        logger.info(f"Initialized BertJob: Project={self.project_id}, Dataset={self.dataset_id}")
        logger.info(f"Stichtag (Reference Date): {self.stichtag.strftime('%Y-%m-%d')}")
        logger.info(f"Wiederanlaufwert (Restart Value): {self.wiederanlaufwert}")

    def _parse_stichtag(self, stichtag_str):
        """
        Parses the stichtag parameter. If not provided, defaults to today's date.
        Replicates logic from r_ausd_bp_ta_rn_vertrag.ksh
        (p_stichtag defaults to v_sysdate if not set).
        Expected format: DDMMYYYY.
        """
        if stichtag_str:
            try:
                # Assuming input format is DDMMYYYY
                stichtag = datetime.datetime.strptime(stichtag_str, '%d%m%Y').date()
            except ValueError:
                logger.error(f"Invalid Stichtag format: {stichtag_str}. Expected DDMMYYYY.")
                raise
        else:
            # Default to today's date if not provided
            stichtag = datetime.date.today()
            logger.info(f"Stichtag not provided, defaulting to today: {stichtag.strftime('%d%m%Y')}")
        return stichtag

    def _get_last_timecreated_from_dwtk_meldungen(self):
        """
        Replicates the v_datum calculation from d_ausd_bp_ta_rn_vertrag.sql.
        This function is for informational/logging purposes as v_datum is not used in the core SQL.
        """
        query = f"""
            SELECT
                IFNULL(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101') AS s_datum
            FROM
                `{self.project_id}.{self.dataset_id}.DWTK_MELDUNGEN` AS m
            WHERE
                m.job_kennung = 'BERT_DROP_TEMP_TABLE'
        """
        try:
            query_job = self.client.query(query)
            results = query_job.result()
            for row in results:
                s_datum = row.s_datum
                logger.info(f"Calculated v_datum from DWTK_MELDUNGEN: {s_datum}")
                return s_datum
        except Exception as e:
            logger.warning(f"Could not retrieve v_datum from DWTK_MELDUNGEN: {e}")
            return '19000101' # Default value from legacy script

    def _execute_bigquery_sql(self, sql_content):
        """Executes a BigQuery SQL query."""
        logger.info(f"Executing BigQuery SQL...")
        logger.debug(f"SQL Content: {sql_content}")
        try:
            query_job = self.client.query(sql_content)
            query_job.result()  # Waits for the query to finish
            logger.info("BigQuery SQL execution successful.")
        except Exception as e:
            logger.error(f"BigQuery SQL execution failed: {e}")
            raise

    def run(self):
        """
        Main execution logic for the job.
        Replicates the orchestration from k_ausd_bp_ta_rn_vertrag.ksh.
        """
        try:
            logger.info("Starting job DW.BERT_AUSD_BP_TA_RN_VERTRAG...")

            # Simulate v_datum calculation for logging (as per legacy)
            self._get_last_timecreated_from_dwtk_meldungen()

            # Read the BigQuery SQL transformation script
            if not os.path.exists(self.sql_file_path):
                raise FileNotFoundError(f"SQL file not found at: {self.sql_file_path}")

            with open(self.sql_file_path, 'r') as f:
                bigquery_sql = f.read()

            # Execute the BigQuery SQL
            self._execute_bigquery_sql(bigquery_sql)

            logger.info("Job DW.BERT_AUSD_BP_TA_RN_VERTRAG completed successfully.")

        except Exception as e:
            logger.error(f"Job DW.BERT_AUSD_BP_TA_RN_VERTRAG failed: {e}", exc_info=True)
            sys.exit(1) # Exit with a non-zero code to indicate failure

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="BERT Contract Phone Number Aggregation Job")
    parser.add_argument('--project_id', required=True, help="Google Cloud Project ID")
    parser.add_argument('--dataset_id', required=True, help="BigQuery Dataset ID (e.g., isbert_schema)")
    parser.add_argument('--sql_file', required=True, help="Path to the BigQuery SQL transformation file")
    parser.add_argument('--stichtag', help="Reference date in DDMMYYYY format (optional, defaults to today)")
    parser.add_argument('--wiederanlaufwert', type=int, default=0, help="Restart value (optional, defaults to 0)")

    args = parser.parse_args()

    job = BertJob(
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        sql_file_path=args.sql_file,
        stichtag_str=args.stichtag,
        wiederanlaufwert=args.wiederanlaufwert
    )
    job.run()