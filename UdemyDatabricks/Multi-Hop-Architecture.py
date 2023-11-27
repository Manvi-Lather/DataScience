# Databricks notebook source
# MAGIC %run ./Includes/Copy-Datasets

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")

display(files)

# COMMAND ----------

(spark.readStream
       .format("cloudFiles")
       .option("cloudFiles.format","parquet")
       .option("cloudFiles.schemaLocation","dbfs:/mnt/demo/checkpoints/orders_raw")
       .load(f"{dataset_bookstore}/orders-raw")
       .createOrReplaceTempView("orders_raw_temp"))  #registering temp view to do data transformation in spark sql

# live stream is not started here . We have to do write or display for that 

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC create or replace temporary view orders_tmp as (
# MAGIC   select *, current_timestamp() arrival_time, input_file_name() source_file
# MAGIC   from orders_raw_temp
# MAGIC )
# MAGIC
# MAGIC
# MAGIC -- we will enrich our raw data with additional metadata describing the source file and the time it was ingested 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from orders_tmp

# COMMAND ----------

# now we are going to pass this enriched data back to pySpark API to process an incremental write to a Delta Lake 


(spark.table("orders_tmp")
      .writeStream
      .format("delta")
      .option("checkpointLocation",'dbfs:/mnt/demo/checkpoints/orders_bronze')
      .outputMode("append")
      .table("orders_bronze"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from orders_bronze

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from orders_bronze

# COMMAND ----------

# MAGIC %md Silver table 

# COMMAND ----------

(spark.read
      .format('json')
      .load(f"{dataset_bookstore}/customers-json")
      .createOrReplaceTempView("customers_lookup"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from customers_lookup

# COMMAND ----------

# to work with bronze data in silver layer 

(spark.readStream
      .table('orders_bronze')
      .createOrReplaceTempView('orders_bronze_tmp'))

# COMMAND ----------


