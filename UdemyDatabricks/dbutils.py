# Databricks notebook source
dbutils.help()

# databricks utility 

# COMMAND ----------

dbutils.fs.ls("dbfs:/")

# to list out the content of the root directory 



# COMMAND ----------

dbutils.fs.ls("/databricks-datasets")

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets/adult")

# COMMAND ----------


# fs -- > file system 
 magic command %fs allows to interact with file system utility directly 

# COMMAND ----------

# MAGIC %fs 
# MAGIC
# MAGIC ls /databricks-datasets/adult/

# COMMAND ----------


