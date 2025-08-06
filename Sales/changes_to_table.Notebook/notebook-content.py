# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e2af9891-1525-4030-adda-61a676f46f85",
# META       "default_lakehouse_name": "Sales",
# META       "default_lakehouse_workspace_id": "740120ee-81a1-42e0-909b-aeb533ec57b3",
# META       "known_lakehouses": [
# META         {
# META           "id": "e2af9891-1525-4030-adda-61a676f46f85"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df_sales = spark.read.table("All_Sales") 

from pyspark.sql.functions import when, col

df_sales_updated = df_sales.withColumn(
    "returned",
    when(col("returned").isNull(), "No").otherwise(col("returned"))
)

df_sales_updated.write.mode("overwrite").format("delta").saveAsTable("All_Sales")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Import necessary functions
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, IntegerType, DateType

df = spark.read.table("All_Sales")

df_cleaned = df \
    .withColumn("Sales", col("Sales").cast(DoubleType())) \
    .withColumn("Profit", col("Profit").cast(DoubleType())) \
    .withColumn("Quantity", col("Quantity").cast(IntegerType())) \
    .withColumn("RowID", col("RowID").cast(IntegerType())) \
    .withColumn("OrderDate", col("OrderDate").cast(DateType())) \
    .withColumn("ShipDate", col("ShipDate").cast(DateType())) \
    .withColumn("PostalCode", col("PostalCode").cast(IntegerType())) 


df_cleaned.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable("All_Sales")



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
