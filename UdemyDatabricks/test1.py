# Databricks notebook source
# MAGIC %md
# MAGIC You were asked to create a table that can store the below data, orderTime is a timestamp but the finance team when they query this data normally prefer the orderTime in date format, you would like to create a calculated column that can convert the orderTime column timestamp datatype to date and store it, fill in the blank to complete the DDL.
# MAGIC
# MAGIC
# MAGIC
# MAGIC order id | order time            | units |
# MAGIC 1        |01-01-2022 09:10:24 AM | 100    |

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or replace TABLE orders (
# MAGIC     orderId INT, 
# MAGIC     orderTime TIMESTAMP,
# MAGIC     orderdate DATE GENERATED ALWAYS AS (CAST(orderTime AS DATE)),
# MAGIC     units INT
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FUNCTION udf_convert_f(temp DOUBLE, measure STRING)
# MAGIC RETURNS DOUBLE 
# MAGIC RETURN CASE WHEN measure = 'F' THEN (temp * 9/5) + 32
# MAGIC             ELSE (temp - 32) * 5/9
# MAGIC         END;
# MAGIC

# COMMAND ----------

udf_convert_f(30.9,'F')

# COMMAND ----------

x = "hello"

#if condition returns True, then nothing happens:
assert x == "hello" , 'NOT RIGHT'

assert x == "manvi" , 'NOT RIGHT'




# COMMAND ----------


