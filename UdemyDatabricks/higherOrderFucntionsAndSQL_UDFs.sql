-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ./Includes/Copy-Datasets

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

describe orders

-- COMMAND ----------

--books column dtype is complex 
--it's an array of struct type 

-- COMMAND ----------

-- MAGIC %md higher order function allows you to work directly with hierarchical data like arrays and map type objects 

-- COMMAND ----------

select * from orders limit 2

-- COMMAND ----------

-- most common function is filter function : 
--filter (expr, func)

--expr --> an array expression 

--func --> a lambda function



select 
order_id, 
books, 
FILTER(books, i -> i.quantity >=2) as multiple_copies
from orders




-- COMMAND ----------

SELECT filter(array(1, 2, 3), x -> x % 2 == 1);

-- COMMAND ----------


