# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fc4134eb-7b14-41ed-9ca8-15245a7a9776",
# META       "default_lakehouse_name": "sales_details",
# META       "default_lakehouse_workspace_id": "740120ee-81a1-42e0-909b-aeb533ec57b3",
# META       "known_lakehouses": [
# META         {
# META           "id": "fc4134eb-7b14-41ed-9ca8-15245a7a9776"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col

final_tables = ["AllSales", "Returns", "SalesPerson"]

all_tables = [t.name for t in spark.catalog.listTables()]

tables_to_delete = [t for t in all_tables if t not in final_tables]

for table in tables_to_delete:
    spark.sql(f"DROP TABLE IF EXISTS {table}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

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
