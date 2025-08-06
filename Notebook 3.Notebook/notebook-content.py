# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6a613de1-41d7-4336-8ed7-92138f36b79d",
# META       "default_lakehouse_name": "ML_LakeHouse_RP_Test",
# META       "default_lakehouse_workspace_id": "740120ee-81a1-42e0-909b-aeb533ec57b3",
# META       "known_lakehouses": [
# META         {
# META           "id": "6a613de1-41d7-4336-8ed7-92138f36b79d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import pandas as pd

perdict = pd.read_csv("/lakehouse/default/Files/predict_card.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import mlflow
from synapse.ml.predict import MLFlowTransformer

model = MLFlowTransformer(
    inputCols=["Time","V1","V2","V3","V4","V5","V6","V7","V8","V9","V10","V11","V12","V13","V14","V15","V16","V17","V18","V19","V20","V21","V22","V23","V24","V25","V26","V27","V28","Amount"], # Your input columns here
    outputCol="predictions", # Your new column name here
    modelName="FraudDetector", # Your model name here
    modelVersion=1 # Your model version here
)
df = model.transform(predict)

df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
