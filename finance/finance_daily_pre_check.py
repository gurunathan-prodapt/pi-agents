import sys
import os
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Finance-Daily-PreCheck") \
        .getOrCreate()
        
    # Note: JDBC target connection elements should be retrieved via GCP Secret Manager or Spark configurations.
    jdbc_url = "jdbc:oracle:thin:@//oracle-host.company.local:1521/FIN_ORA_SID"
    db_user = os.environ.get("FIN_ORA_USER", "FIN_ORA_USER")
    db_pass = os.environ.get("FIN_ORA_PASS", "FIN_ORA_PASSWORD")
    
    try: 
        test_df = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "(SELECT 'DB_OK' FROM DUAL) test_conn") \
            .option("user", db_user) \
            .option("password", db_pass) \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .load()
            
        if test_df.count() == 1:
            print("STATUS CHECK: SUCCESS. GL Database connectivity is functional.")
            sys.exit(0)
    except Exception as e:
        print(f"CRITICAL ERROR: GL database system is offline. Details: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()