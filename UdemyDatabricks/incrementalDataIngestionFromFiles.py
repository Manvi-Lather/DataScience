# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./Includes/Copy-Datasets

# COMMAND ----------

print(dataset_bookstore)

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")

display(files)


# data source directory 

# COMMAND ----------


