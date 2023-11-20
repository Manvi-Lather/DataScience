-- Databricks notebook source
-- MAGIC %run "/Repos/sunitadhankhar@my.unt.edu/DataScience/Myfolder/Includes/Copy-Datasets"
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC copy-dataset notebook download and copy the dataset to our databricks file system 

-- COMMAND ----------

create table orders as 

select * from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

select * from orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC -- When writing to tables, we could be interested by completely overwriting the data in the table.
-- MAGIC --There are multiple benefits to overwriting tables instead of deleting and recreating tables.
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC --For example, the old version of the table still exists and can easily retrieve all data using time travel. 
-- MAGIC
-- MAGIC --in addition, overwriting a table is much faster because it does not need to list the directory recursively or delete any file 
-- MAGIC
-- MAGIC
-- MAGIC --it is atomic operation . Concurrent queries can still read the table while you are overwriting it. 
-- MAGIC
-- MAGIC --ACID 
-- MAGIC
-- MAGIC

-- COMMAND ----------

--First method to overwrite the table 

--1. Create or Replace 

create or replace table orders as 
select * from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

Describe history orders

-- COMMAND ----------

--2. Insert overwrite 

insert overwrite orders
select * from parquet.`${dataset.bookstore}/orders`

--Diff from Create or Replace 
--DIff
--it can only overwrite existing table 

--it can overwrite only the new records that match the current table schema, which means it is safer technique for overwriting an existing 
--table without the risk of modifying the table schema 

-- COMMAND ----------

Describe history orders

-- COMMAND ----------

insert overwrite orders
select *, current_timestamp()
from parquet.`{dataset.bookstore}/orders`

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC as expected we got error 
-- MAGIC
-- MAGIC -- we can add new column to the table while running Insert overwrite command

-- COMMAND ----------

Describe history orders

-- COMMAND ----------

-- MAGIC %md Appending records to the table 
-- MAGIC
-- MAGIC

-- COMMAND ----------

insert into orders
select * from parquet.`${dataset.bookstore}/orders-new`

-- COMMAND ----------

select count(*) from orders

-- COMMAND ----------

-- Limitation : Insert into statement does not have any built in gaurantees to prevent inserting duplicates. this means if we run the same query again, it can lead to dunplicate records 

-- COMMAND ----------

-- To resolve this issue ,we can use Merge operation 

Create or replace temp view customers_updates as 
select * from json.`${dataset.bookstore}/customers-json-new`;


Merge into customers c
using customers_updates u 
on c.customer_id = u.customer_id 
  when matched and c.email is null and u.email is not null 
    then 
      update set email = u.email, updated = u.updated
  when not matched 
    then 
      insert * 

-- COMMAND ----------

-- we have to insert new details to the table and input data is in csv format 
--so first we will create a temp view 


create or replace temp view books_updates
  (book_id string, title string, author string, category string, price double)
using csv 
options(
  path ="${dataset.bookstore}/books-csv-new",
  header='true',
  delimiter=';'
);

select * from books_updates


-- we want to insert only category = computer science 

-- COMMAND ----------

merge into books b
using books_updates u 
on b.book_id = u.book_id and b.title = u.title
when not matched and u.category = 'Computer Science' then 
insert * 


--Insert records only where key is not matching and cat = 'CS'

-- COMMAND ----------

--Mani advantage of merge operation is to avoid duplicates 

-- COMMAND ----------


