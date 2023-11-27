-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC %md
-- MAGIC --querying files directly 
-- MAGIC
-- MAGIC --select * from file_format.'/path/to/file'
-- MAGIC
-- MAGIC --file_format: 
-- MAGIC
-- MAGIC
-- MAGIC --works well with self describing format that have well defined schema like JSON and Parquet. 
-- MAGIC
-- MAGIC --it does not work well with non self decribing formats like csv 
-- MAGIC
-- MAGIC --for path we can give : 1. single file , multiple file and complete directory(all of the files should have same format and schema)
-- MAGIC
-- MAGIC --example : select * from json.'/path/file_name.json'
-- MAGIC
-- MAGIC --Raw Data: 
-- MAGIC
-- MAGIC     -- extract text files as raw strings 
-- MAGIC       --text based files(json,csv,tsv and txt formats)
-- MAGIC       --select * from text.'/path/to/file'
-- MAGIC
-- MAGIC     --extract files as raw bytes.
-- MAGIC       --images or unstructured data
-- MAGIC       --select * from binaryFile.'/path/to/file'
-- MAGIC
-- MAGIC --CTAS(create table as select statement) ; Registering tables from files
-- MAGIC
-- MAGIC   --Create table table_name
-- MAGIC   --as select * from file_format.'/path/to/file'
-- MAGIC
-- MAGIC   --Automatically infer schema information from query results 
-- MAGIC     --do not support manual schema declaration. 
-- MAGIC     --useful for external data ingestion with well-defined schema(parquet, tables) 
-- MAGIC     --data is moving during table creation
-- MAGIC
-- MAGIC   --do not support file options -- significance limitation when trying to ingest data from csv files 
-- MAGIC
-- MAGIC
-- MAGIC   --solution for csv files
-- MAGIC
-- MAGIC --Registering tables on external data sources
-- MAGIC
-- MAGIC     --create table table_name 
-- MAGIC     --(col_1, col_2)
-- MAGIC     --USING data_source
-- MAGIC     --OPTIONS (key1 = val1, key2 = val2)
-- MAGIC     --LOCATION = path
-- MAGIC
-- MAGIC
-- MAGIC   --External tables
-- MAGIC   --these files are kept in its original format, which means no Delta tables
-- MAGIC   --these tables are just a refrence to the files 
-- MAGIC   --no data moving during table creation . We are just pointing to files stored in an external location 
-- MAGIC
-- MAGIC   --Example: creating a table using csv external source . it is not a delta table . we are pointing to csv file exits in external location. we are sepecifying option like there is header and delimiter is ; . 
-- MAGIC
-- MAGIC --csv table 
-- MAGIC   create table table_name
-- MAGIC   (col_name col_type)
-- MAGIC   using csv 
-- MAGIC   options(header = 'true',
-- MAGIC   delimiter= ';')
-- MAGIC   location = path
-- MAGIC
-- MAGIC --sql server table 
-- MAGIC   Create table table_name
-- MAGIC   (col_name col_type )
-- MAGIC   uisng JDBC
-- MAGIC   options(url= 'jdbc:sqlite://hostname:port',
-- MAGIC           dbtable = 'database.table',
-- MAGIC           user = 'username',
-- MAGIC           password = 'pwd') 
-- MAGIC
-- MAGIC
-- MAGIC --limitations : 
-- MAGIC
-- MAGIC     -- it's no delta table 
-- MAGIC     --we can not expect the performance guarantees associated with delta lake and lakehouse 
-- MAGIC
-- MAGIC     --having a huge database tables, this can also cause perfromance issue 
-- MAGIC
-- MAGIC --solution 
-- MAGIC -- create a temp view refering to external data source 
-- MAGIC
-- MAGIC   cerate a temp view temp_view_name(col_name1 col_type1,...)
-- MAGIC     using data_source 
-- MAGIC     options(key1 = val1, key2= val2,....)
-- MAGIC --query the view 
-- MAGIC     create table table_name 
-- MAGIC     as select * from temp_view_name 
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In this notebook we will practice, how to extract data directly from files using spark sql on Databricks 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying JSON 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC copy-dataset notebook download and copy the dataset to our databricks file system 

-- COMMAND ----------

-- MAGIC %run ./Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Querying json files

-- COMMAND ----------

-- to query single json file 
-- notice backticks are used around the path

SELECT * FROM json.`${dataset.bookstore}/customers-json/export_001.json`

-- COMMAND ----------

SELECT * FROM json.`${dataset.bookstore}/customers-json/export_*.json`

--using wildcard

-- COMMAND ----------

SELECT * FROM json.`${dataset.bookstore}/customers-json`

--querying directory. assuming all the files have same format and schema 

-- COMMAND ----------

SELECT count(*) FROM json.`${dataset.bookstore}/customers-json`



-- COMMAND ----------

 SELECT *,
    input_file_name() source_file 
  FROM json.`${dataset.bookstore}/customers-json`;


  -- input_file_name function (built in spark sql command that records the source data file for each record)
  --help full in troubleshooting

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying text Format

-- COMMAND ----------

SELECT * FROM text.`${dataset.bookstore}/customers-json`

--query any text based file 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Querying binaryFile Format

-- COMMAND ----------

SELECT * FROM binaryFile.`${dataset.bookstore}/customers-json`

--used to extract the metadata of the file 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Querying CSV 

-- COMMAND ----------

SELECT * FROM csv.`${dataset.bookstore}/books-csv`

-- COMMAND ----------

-- MAGIC %md the data is not well parsed

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC create table with using operation

-- COMMAND ----------

CREATE TABLE books_csv
  (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  header = "true",
  delimiter = ";"
)
LOCATION "${dataset.bookstore}/books-csv"


-- this will create a external table 
-- that means no data will move during data creation 
-- this table is just pointing to the data in external location 

-- COMMAND ----------

SELECT * FROM books_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Limitations of Non-Delta Tables

-- COMMAND ----------

DESCRIBE EXTENDED books_csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)
-- MAGIC
-- MAGIC
-- MAGIC # check how many book csv files are in directory

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.read
-- MAGIC         .table("books_csv")
-- MAGIC       .write
-- MAGIC         .mode("append")
-- MAGIC         .format("csv")
-- MAGIC         .option('header', 'true')
-- MAGIC         .option('delimiter', ';')
-- MAGIC         .save(f"{dataset_bookstore}/books-csv"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

SELECT COUNT(*) FROM books_csv

-- COMMAND ----------

REFRESH TABLE books_csv

-- to manually refresh the cache 

-- COMMAND ----------

SELECT COUNT(*) FROM books_csv

-- for large dataset , it can take significant amount of time 


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CTAS Statements

-- COMMAND ----------

-- Create delta table 

CREATE TABLE customers AS
SELECT * FROM json.`${dataset.bookstore}/customers-json`;

DESCRIBE EXTENDED customers;

-- COMMAND ----------

CREATE TABLE books_unparsed AS
SELECT * FROM csv.`${dataset.bookstore}/books-csv`;

SELECT * FROM books_unparsed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC CTAS statements automatically infer schema information from a query and do not support manual schema declaration 
-- MAGIC
-- MAGIC
-- MAGIC This means that CTAS statements are useful for external data ingestion from sources with well-defined schema such as parquet files and tables.
-- MAGIC
-- MAGIC In addition, CTAS statements do not support specifying additional file options which presents significant limitations when trying to ingest data from CSV files 
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC to correct this we need first to use a refrence to the files that allows us to specify options 
-- MAGIC
-- MAGIC
-- MAGIC and this is what we are doing here by creating this temporary view that allows us to specify file options.
-- MAGIC

-- COMMAND ----------

CREATE TEMP VIEW books_tmp_vw
   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "${dataset.bookstore}/books-csv/export_*.csv",
  header = "true",
  delimiter = ";"
);

-- we will use this temp view as the source for our CTAS statment to successfully register the delta tables 

CREATE TABLE books AS
  SELECT * FROM books_tmp_vw;
  
SELECT * FROM books

-- COMMAND ----------

--Check meta data of the table 
DESCRIBE EXTENDED books

-- COMMAND ----------


