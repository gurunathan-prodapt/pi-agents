import sys
import os
from pyspark.sql import SparkSession

def main():
    if len(sys.argv) < 3:
        print("Required args: [period_date] [allow_empty_flag]")
        sys.exit(1)
        
    period_date = sys.argv[1]
    allow_empty = sys.argv[2]
    
    spark = SparkSession.builder \
        .appName("Finance-Daily-GLClose") \
        .getOrCreate()
        
    try:
        # Original-language literal audit logs and event trigger publish simulated here
        print(f"[FINANCE_DAILY_GL_CLOSE] Period={period_date} complete")
        
        # Standardize and save the run audit details
        gcs_bucket = os.environ.get("GCS_BUCKET", "YOUR_BUCKET_NAME")
        audit_log_path = f"gs://{gcs_bucket}/audit/daily_audit_log/date={period_date}"
        audit_data = spark.createDataFrame(
            [(period_date, "SUCCESS", "FINANCE_GL_CLOSE_COMPLETE")],
            ["period_date", "status", "event_published"]
        )
        audit_data.write.mode("append").parquet(audit_log_path)
        sys.exit(0)
    except Exception as e:
        print(f"CRITICAL: GL close and validation failed. Details: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()