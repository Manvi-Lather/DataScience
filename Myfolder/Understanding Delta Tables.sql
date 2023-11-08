-- Databricks notebook source
create table delta
(id int, name string, salary double); 

-- COMMAND ----------

insert into delta 
values
(1, "manvi",6000),
(2,"shakti",5500)

-- COMMAND ----------

select * from delta

-- COMMAND ----------

describe detail delta

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/user/hive/warehouse/delta"

-- COMMAND ----------

update delta
set salary = salary + 100
where name like 'm%'

-- COMMAND ----------

select * from delta

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/user/hive/warehouse/delta"

-- COMMAND ----------

describe detail delta

-- COMMAND ----------

describe history delta

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/user/hive/warehouse/delta/_delta_log/"

-- COMMAND ----------

-- MAGIC %fs head 'dbfs:/user/hive/warehouse/delta/_delta_log/00000000000000000000.json'
-- MAGIC
-- MAGIC "operation":"CREATE TABLE"

-- COMMAND ----------

-- MAGIC %fs head 'dbfs:/user/hive/warehouse/delta/_delta_log/00000000000000000001.json'
-- MAGIC
-- MAGIC --"operation":"WRITE"

-- COMMAND ----------

-- MAGIC %fs head 'dbfs:/user/hive/warehouse/delta/_delta_log/00000000000000000002.json'
-- MAGIC
-- MAGIC
-- MAGIC --"operation":"UPDATE"

-- COMMAND ----------

select * 
from delta version as of 1; 

-- COMMAND ----------

select * from delta@v1

-- COMMAND ----------

delete from delta

-- COMMAND ----------

select * from delta

-- COMMAND ----------

restore table delta to version as of 2; 

-- COMMAND ----------

select * from delta

-- COMMAND ----------

describe history delta


-- COMMAND ----------

describe detail delta

-- COMMAND ----------

optimize delta
Zorder by id; 

-- COMMAND ----------

describe detail delta

-- COMMAND ----------

describe history delta

-- COMMAND ----------

vacuum delta  

-- COMMAND ----------

set spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

vacuum delta retain 0 hours

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/delta'

-- COMMAND ----------

select * FROM delta@v1

-- COMMAND ----------

drop table delta

-- COMMAND ----------

select * from delta

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/delta'

-- COMMAND ----------


