# -*- coding: utf-8 -*-
"""Reusable Cloud Storage operational utilities."""

import logging
from google.cloud import storage

logger = logging.getLogger(__name__)

class GCSHandler:
    """Helper class to abstract Cloud Storage operations."""

    def __init__(self, project_id: str):
        self.project_id = project_id
        self._client = None

    @property
    def client(self) -> storage.Client:
        """Lazy-loaded Cloud Storage client."""
        if self._client is None:
            self._client = storage.Client(project=self.project_id)
        return self._client

    def upload_string_as_blob(self, bucket_name: str, destination_blob_name: str, content: str, content_type: str = "text/plain") -> str:
        """Uploads a standard python string directly to a GCS destination."""
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_string(content, content_type=content_type)
            uri = f"gs://{bucket_name}/{destination_blob_name}"
            logger.info(f"Successfully uploaded file to GCS: {uri}")
            return uri
        except Exception as e:
            logger.error(f"Failed to upload content to GCS blob '{destination_blob_name}': {str(e)}")
            raise e