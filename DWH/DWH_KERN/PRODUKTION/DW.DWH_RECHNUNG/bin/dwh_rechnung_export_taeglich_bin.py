"""
Module: dwh_rechnung_export_taeglich_bin.py
Description: Auxiliary helper functions for validating exports, processing row 
             counts, and consolidating sharded Cloud Storage files.
"""

import logging
from google.cloud import storage

logger = logging.getLogger(__name__)

class GcsExportHelper:
    """Consolidates and validates GCS exports."""

    def __init__(self, project_id: str):
        self.storage_client = storage.Client(project=project_id)

    def validate_and_consolidate_shards(
        self, 
        bucket_name: str, 
        source_prefix: str, 
        destination_blob_name: str,
        expected_delimiter: str = "|"
    ) -> int:
        """
        Validates row existence, merges sharded files outputted by BigQuery 
        into a single consolidated output file, and deletes the shards.
        
        :param bucket_name: GCS Bucket Name
        :param source_prefix: Folder/prefix representing the sharded files (e.g. 'exports/shards_')
        :param destination_blob_name: Consolidated output file path (e.g. 'ausgang/rechnung_export_20260101.dat')
        :param expected_delimiter: Delimiter to count lines and validate format
        :return: Total logical line count (excluding headers if applicable)
        """
        bucket = self.storage_client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=source_prefix))
        
        if not blobs:
            logger.warning(f"No source export shards found for prefix: gs://{bucket_name}/{source_prefix}")
            return 0
        
        # Sort blobs lexicographically to maintain shard ordering
        blobs.sort(key=lambda x: x.name)
        
        logger.info(f"Found {len(blobs)} shards to consolidate from prefix {source_prefix}...")
        
        # Compose operation constraints: GCS compose supports up to 32 components per call
        chunk_size = 32
        temp_composed_blobs = []
        
        try:
            # If there's only 1 file, simply rename/copy it to the destination
            if len(blobs) == 1:
                source_blob = blobs[0]
                bucket.copy_blob(source_blob, bucket, destination_blob_name)
                logger.info(f"Single shard copy successful: gs://{bucket_name}/{destination_blob_name}")
            else:
                # Batch compose shards into intermediate/final files
                for i in range(0, len(blobs), chunk_size):
                    chunk = blobs[i:i + chunk_size]
                    temp_blob_name = f"{destination_blob_name}.part{i//chunk_size}"
                    temp_blob = bucket.blob(temp_blob_name)
                    temp_blob.compose(chunk)
                    temp_composed_blobs.append(temp_blob)
                
                # Final execution compose
                final_blob = bucket.blob(destination_blob_name)
                final_blob.compose(temp_composed_blobs)
                logger.info(f"Consolidation successful: gs://{bucket_name}/{destination_blob_name}")
                
                # Clean up intermediate part files
                for temp_part in temp_composed_blobs:
                    temp_part.delete()
            
            # Count logical records within the consolidated file
            final_blob = bucket.get_blob(destination_blob_name)
            content = final_blob.download_as_text()
            line_count = len([line for line in content.splitlines() if line.strip()])
            
            # Log warnings if dataset is empty (mimicking legacy KSH validation)
            if line_count == 0:
                logger.warning(f"[WARNING] Export file gs://{bucket_name}/{destination_blob_name} contains 0 rows!")
            else:
                logger.info(f"[SUCCESS] Export completed. Verified line count: {line_count}")

            # Clean up the original sharded source files
            for blob in blobs:
                blob.delete()
            logger.info("Successfully cleaned up original sharded source files.")
            
            return line_count

        except Exception as e:
            logger.error(f"Error occurred during GCS shard consolidation: {str(e)}")
            raise e