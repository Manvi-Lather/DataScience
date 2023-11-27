-- Databricks notebook source
CREATE TABLE orders (
    orderId int,
    orderTime timestamp,
    orderdate date GENERATED ALWAYS AS(cast(orderTime as DATE)),
    units int)

-- COMMAND ----------

CREATE TABLE transactions USING DELTA LOCATION "DBFS:/transactions"

-- COMMAND ----------

INSERT OVERWRITE customer_sales
SELECT * FROM customers c
INNER JOIN sales_monthly s on s.customer_id = c.customer_id

-- COMMAND ----------

CREATE FUNCTION udf_convert(temp DOUBLE, measure string)
returns DOUBLE 
return case when measure == 'F' then (temp * 9/5) + 32
      else(temp-33) * 5/9
      end 

-- COMMAND ----------

udf_convert( 43,'F')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC row_count = spark.sql("select count(*) from orders").collect()[0][0]

-- COMMAND ----------

row_count[0]

-- COMMAND ----------

assert row_count = 10, "row count did not match"

-- COMMAND ----------


