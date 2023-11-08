-- Databricks notebook source
create table managed_default
(width int, length int,height int);


insert into managed_default
values(3, 2, 1)

-- this table is stored in hive meta datastore default folder 


--this table is known as manager table.
--created under the database directory.
--dropping the table, delete the underlying data files.

-- COMMAND ----------

select * from managed_default

-- COMMAND ----------

describe detail managed_default


--location of the file 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC one delta table/transaction table and one parquet file 

-- COMMAND ----------

-- MAGIC %fs  ls 'dbfs:/user/hive/warehouse/managed_default'
-- MAGIC
-- MAGIC

-- COMMAND ----------

describe extended managed_default; 

-- COMMAND ----------

drop table managed_default

-- COMMAND ----------

-- MAGIC %fs  ls 'dbfs:/user/hive/warehouse/managed_default'

-- COMMAND ----------



-- COMMAND ----------

--lets create a external table --> created outside the database directory 
--dropping this table will not delete the underlying data file

create table external_default
(width int, length int, height int)
location 'dbfs:/mnt/demo/external_default'; 


insert into external_default
values(3, 2, 1)

-- COMMAND ----------

describe extended external_default; 

-- COMMAND ----------

drop table external_default

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC dropping this table will not delete the underlying data file
-- MAGIC

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_default'
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC both tables are removed from the hive meta datastore however if we check table directory , we see the log and parquet file exists for external file 

-- COMMAND ----------

create schema new_default; 

-- COMMAND ----------

describe database extended new_default; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC notice .db extension of db 

-- COMMAND ----------

use new_default; 

-- I want to create tables in new_default db 

create table managed_new_default
(width int, length int, height int); 

insert into managed_new_default
values(1, 2, 4); 



--------------------------------------------

create table external_new_default
(width int, length int, height int)
location 'dbfs:/mnt/demo/external_new_default'; 

insert into external_new_default 
values(3, 4,5); 



-- COMMAND ----------

describe extended managed_new_default



-- COMMAND ----------

describe extended external_new_default

-- COMMAND ----------

drop table external_new_default;

drop table managed_new_default;

-- COMMAND ----------

--create db outside hive directory 

create schema custom 
location 'dbfs:/Shared/Schemas/custom.db'


--we can see from data explorer that db is created in the hive metastore , however, if we run the describe database extended cmd , we can see that it is created in custom location 

-- COMMAND ----------

describe database extended custom


--Location --> dbfs:/Shared/Schemas/custom.db

-- COMMAND ----------

use custom; 

-- I want to create tables in new_default db 

create table managed_custom
(width int, length int, height int); 

insert into managed_custom
values(1, 2, 4); 



--------------------------------------------

create table external_custom
(width int, length int, height int)
location 'dbfs:/mnt/demo/external_'; 

insert into external_custom 
values(3, 4,5); 



-- COMMAND ----------


describe extended managed_custom;

--dbfs:/Shared/Schemas/custom.db/managed_custom



-- COMMAND ----------

describe extended external_custom;


--dbfs:/mnt/demo/external_

--created outside db directory

-- COMMAND ----------


