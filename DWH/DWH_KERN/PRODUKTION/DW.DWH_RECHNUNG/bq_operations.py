"""Module to operate table validations and output flat-file deliveries."""

import logging
from google.cloud import bigquery
from dags.modules import logger as custom_logger

logger = logging.getLogger(__name__)


class BigQueryEgressHelper:
    """Manages internal BigQuery checks and outputs files natively to Storage buckets."""

    def __init__(self, project_id: str):
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id

    def check_records_count(self, dataset_id: str, table_id: str, stichtag: str) -> int:
        """Runs validation queries determining execution row metrics counts."""
        count_query = f"""
            SELECT COUNT(1) as total_rows 
            FROM `{self.project_id}.{dataset_id}.{table_id}`
            WHERE rechnungs_datum = DATE('{stichtag}')
        """
        query_job = self.client.query(count_query)
        results = query_job.result()
        row_count = next(results).total_rows
        return int(row_count)

    def extract_table_to_gcs(
        self,
        dataset_id: str,
        table_id: str,
        gcs_bucket: str,
        filename_prefix: str,
        delimiter: str = "|",
    ) -> None:
        """Extracts target Table assets to Cloud Storage as delimiter-separated files."""
        destination_uri = f"gs://{gcs_bucket}/{filename_prefix}"
        
        dataset_ref = bigquery.DatasetReference(self.project_id, dataset_id)
        table_ref = dataset_ref.table(table_id)

        job_config = bigquery.ExtractJobConfig()
        job_config.field_delimiter = delimiter
        job_config.print_header = False  # Matches downstream spool-formatting dependencies

        extract_job = self.client.extract_table(
            table_ref, destination_uri, job_config=job_config
        )
        extract_job.result()  # Blocks till extraction confirmation
        logger.info(f"Successfully extracted table data to GCS destination: {destination_uri}")