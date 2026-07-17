#!/usr/bin/env python
from pyspark.sql import SparkSession

def main():
    # Initialize the Spark Session on Dataproc
    spark = SparkSession.builder \
        .appName("dw_dwh_dummy_absd_plato_tarife") \
        .getOrCreate()
        
    # VERBATIM preservation of legacy SCRIPT log output
    print("Doing nothinig")
    
    spark.stop()

if __name__ == "__main__":
    main()