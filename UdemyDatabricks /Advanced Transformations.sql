-- Databricks notebook source
-- MAGIC %run "/Repos/sunitadhankhar@my.unt.edu/DataScience/Myfolder/Includes/Copy-Datasets"
-- MAGIC

-- COMMAND ----------

select * from customers

-- COMMAND ----------

--profile column has complex data structure. 

describe customers

--dtype of profile column is string 

-- COMMAND ----------

select customer_id, profile:first_name, profile:address:country
from customers


--

-- COMMAND ----------

select from_json(profile) as profile_struct
from customers;

-- we were expecting errror 

-- COMMAND ----------

select profile
from customers 
limit 1

-- COMMAND ----------

create or replace temp view parsed_customers as 
  select customer_id,from_json(profile,schema_of_json('{"first_name":"Susana","last_name":"Gonnely","gender":"Female","address":{"street":"760 Express Court","city":"Obrenovac","country":"Serbia"}}')) as profile_struct
  from customers; 


select * from parsed_customers;

-- COMMAND ----------

describe parsed_customers

-- COMMAND ----------

select customer_id , profile_struct.first_name, profile_struct.address.country

from parsed_customers

-- COMMAND ----------


