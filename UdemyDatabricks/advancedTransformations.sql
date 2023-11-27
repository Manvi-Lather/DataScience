-- Databricks notebook source
-- MAGIC %run ./Includes/Copy-Datasets
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
  select customer_id,
  from_json(profile,schema_of_json('{"first_name":"Susana","last_name":"Gonnely","gender":"Female","address":{"street":"760 Express Court","city":"Obrenovac","country":"Serbia"}}')) as profile_struct
  from customers; 


select * from parsed_customers;

--struct is a native spark type with nested attributes. This can be done with the from_json function. This function needs schema of the data 

-- COMMAND ----------

describe parsed_customers

-- COMMAND ----------

select customer_id , profile_struct.first_name, profile_struct.address.country

from parsed_customers

-- COMMAND ----------

create or replace temp view customer_final as

  select customer_id, profile_struct.*
  from parsed_customers;

select * from customer_final

-- COMMAND ----------

select order_id, customer_id,books
from orders

-- COMMAND ----------

--explode functions: 

-- dtype of books column is struct: 

--explode function allows us to put each element of an array on its own row . 


select order_id, customer_id, explode(books) as book 
from orders; 


--each element in the book column has its own row and we have repeated order_id and customer_id 
--before

--one row
--------------------------------------------------------------------------
-- array
-- 0: {"book_id": "B07", "quantity": 1, "subtotal": 33}
-- 1: {"book_id": "B06", "quantity": 1, "subtotal": 22}
--------------------------------------------------------------------------

--after
-- 2 rows 

-- 0: {"book_id": "B07", "quantity": 1, "subtotal": 33}
---------------------------------------------------------------------
-- 1: {"book_id": "B06", "quantity": 1, "subtotal": 22}




-- COMMAND ----------

--collect_set aggregation function --- it allows us to collect unique values for a field, including fields within arrays. 

-- COMMAND ----------

select order_id, customer_id,books
from orders

-- COMMAND ----------

select customer_id, 
collect_set(order_id) as order_set, 
collect_set(books.book_id) as books_set
from orders
group by customer_id 


-- notice that for customer_id = C00001, we have all the order set and book_set in one row . 

--Books_set column has array in array 

-- COMMAND ----------

select order_id, customer_id,books
from orders
where customer_id = 'C00001'

-- COMMAND ----------

--Flatten the array 

--keep only distinct values . customer_id = C00004 has duplicate values in book_set column 


select customer_id, 
  collect_set(books.book_id) as before_flatten, 
  array_distinct(flatten(collect_set(books.book_id))) as after_flatten

from orders
group by customer_id 

-- COMMAND ----------

-- join operation 


create or replace view orders_enriched as
select * 
from (
  select *, explode(books) as book
  from orders) o
inner join books b 
on o.book.book_id = b.book_id;


-- COMMAND ----------

select * from orders_enriched

-- COMMAND ----------

select * from orders limit 1;

select
  customer_id, 
  book.book_id as book_id, 
  book.quantity as quantity
  from orders_enriched limit 1; 

  



-- COMMAND ----------

--spark sqk also support pivot clause, which is used to change dara perspective 

-- COMMAND ----------

CREATE OR REPLACE TABLE transactions AS

SELECT * FROM (
  SELECT
    customer_id,
    book.book_id AS book_id,
    book.quantity AS quantity
  FROM orders_enriched
) PIVOT (
  sum(quantity) FOR book_id in (
    'B01', 'B02', 'B03', 'B04', 'B05', 'B06',
    'B07', 'B08', 'B09', 'B10', 'B11', 'B12'
  )
);

SELECT * FROM transactions

-- COMMAND ----------


