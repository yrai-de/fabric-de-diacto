# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

folder_csv_links = [
    "https://diacto-my.sharepoint.com/:u:/p/nidhi_nair/ER45dYi3L5JKifuJDRmHbkABjm-s0d2nrj_pYB_y_n4-Pg?e=GU9mFd&downlod=1",
    
]

all_dfs = []

for url in folder_csv_links:
    try:
        pdf = pd.read_csv(url)
        sdf = spark.createDataFrame(pdf)
        all_dfs.append(sdf)
        print(f"Loaded: {url}")
    except Exception as e:
        print(f"Failed: {url} - {e}")

if all_dfs:
    combined = all_dfs[0]
    for df in all_dfs[1:]:
        combined = combined.unionByName(df, allowMissingColumns=True)
    
    display(combined)
    combined.write.mode("overwrite").saveAsTable("combined_csv_data")
    print("All files written to Lakehouse table.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
