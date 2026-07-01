import sys
import gzip
import datetime
from google.cloud import storage

def append_trailer_and_gzip_gcs(
    project_id, 
    source_bucket_name, 
    dest_bucket_name, 
    source_blob_name, 
    dest_blob_name, 
    report_type, 
    from_date, 
    separator="|"
):
    """
    Reads a CSV extract from GCS, counts rows, appends custom EXIS structured trailer,
    and uploads a gzipped version directly back to GCS.
    
    This function uses separate source_bucket_name (e.g. temp bucket) and 
    dest_bucket_name (e.g. store bucket) to prevent 404 GCS errors.
    """
    storage_client = storage.Client(project=project_id)
    
    # Read GCS file from source bucket
    source_bucket = storage_client.bucket(source_bucket_name)
    source_blob = source_bucket.blob(source_blob_name)
    data_content = source_blob.download_as_text()
    lines = data_content.splitlines()
    
    # Exclude empty ending rows if present
    if lines and not lines[-1].strip():
        lines.pop()
        
    row_count = len(lines)
    sysdate_str = datetime.datetime.now().strftime("%Y%m%d")
    
    # Map raw filename for footer representation
    destination_file = dest_blob_name.split('/')[-1]
    
    # Build legacy exact footer: X|filename|from_date|count|report_type|sysdate
    trailer_record = f"X{separator}{destination_file}{separator}{from_date}{separator}{row_count}{separator}{report_type}{separator}{sysdate_str}"
    lines.append(trailer_record)
    
    # Reassemble with separator line ending
    final_output = "\n".join(lines) + "\n"
    
    # Gzip output in memory
    compressed_data = gzip.compress(final_output.encode('utf-8'))
    
    # Write finished compressed file back to target bucket destination
    dest_bucket = storage_client.bucket(dest_bucket_name)
    dest_blob = dest_bucket.blob(dest_blob_name)
    dest_blob.upload_from_string(compressed_data, content_type='application/gzip')
    print(f"Successfully processed {row_count} rows. Compressed upload completed: gs://{dest_bucket_name}/{dest_blob_name}")

if __name__ == "__main__":
    if len(sys.argv) < 8:
        print("Usage: python add_trailer_and_compress.py <project_id> <source_bucket_name> <dest_bucket_name> <src_blob> <dest_blob> <report_type> <from_date>")
        sys.exit(1)
        
    append_trailer_and_gzip_gcs(
        project_id=sys.argv[1],
        source_bucket_name=sys.argv[2],
        dest_bucket_name=sys.argv[3],
        source_blob_name=sys.argv[4],
        dest_blob_name=sys.argv[5],
        report_type=sys.argv[6],
        from_date=sys.argv[7]
    )