# Databricks notebook source
import dlt

@dlt.table
def bronze():
    df = spark.readStream\
            .format("cloudFiles")\
            .option("cloudFiles.format", "csv")\
            .option("delimiter", "|")\
            .load("/Volumes/workspace/damg7370/material_data")
    return df

# COMMAND ----------

@dlt.table
@dlt.expect_or_drop("unit_cost_nn", "unit_cost IS NOT NULL")
def silver():
    df = dlt.read("bronze")
    
    #type changes
    df = df.withColumn("unit_cost", df.unit_cost.cast("double"))
    df = df.withColumn("last_updated", df.last_updated.cast("date"))
    df = df.withColumn("lead_time_days", df.lead_time_days.cast("int"))
    df = df.withColumn("safety_stock", df.safety_stock.cast("int"))
    df = df.withColumn("reorder_level", df.reorder_level.cast("int"))

    return df
    
