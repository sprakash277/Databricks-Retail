# Databricks notebook source
import dlt
import json
from dlt import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# current_path = "/Workspace" + dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
current_path = "/Workspace/Users/sumit.prakash@databricks.com/dlt-metaprogramming-cdc/"
json_file_path = current_path+ "full_mapping_test.json"
with open(json_file_path, "r") as file:
    metadata = json.load(file)

scd_type = 1
# Obtain properties from metadata json
storage_path = metadata["storage_path"]
tables = metadata["table_list"]

# COMMAND ----------

# Create the Raw Table in the Bronze Layer
def createRawTable(table, path):
    @dlt.table(
        name= table ,
        comment="New " + table + " data incrementally ingested from cloud object storage landing zone"
    )
    @dlt.expect("Initial data quality check", "_rescued_data IS NULL")
    def raw_table():
        # Read raw data from cloud object storage landing zone
        df = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .load(path)
        )
        df = df.withColumn("operation_date", unix_timestamp(current_timestamp()))
        return df

    
def generate_scd_tables(table, keys, seq_col, scd_type):
  dlt.create_streaming_live_table("{}_scd{}".format(table, scd_type))
  dlt.apply_changes(
      target = "{}_scd{}".format(table, scd_type),
      source = table, 
      keys = keys, 
      sequence_by = col(seq_col),
      stored_as_scd_type = scd_type
    )



# COMMAND ----------

for table in tables:
  table_name = table["table_name"]
  scd_key = table["scd_key"]
  sequence_col = table["sequence_by_col"][0]
  print (table_name,scd_key,sequence_col)

  # Create the Raw Table in the Bronze Layer
  print (storage_path + table_name)
  raw_table_func = createRawTable(table_name, storage_path + table_name)

  # Create the Cleaned Table in the Silver Layer
  cleaned_table_func = generate_scd_tables(table_name, scd_key, sequence_col, scd_type)
