# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./Includes/Copy-Datasets

# COMMAND ----------

(spark.readStream
.table('books')
.createOrReplaceTempView("books_streaming_tmp_vm") #registering temporary view against stream source . it is a streaming temporary view that allows to apply most #transformation in sql the same way as we would with stattic data
)

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from books_streaming_tmp_vm

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC -- applying aggregation on streaming view 
# MAGIC
# MAGIC select author, 
# MAGIC count(book_id) as total_books
# MAGIC from books_streaming_tmp_vm
# MAGIC group by author
# MAGIC
# MAGIC
# MAGIC -- as we are querying a streaming temporary view this becomes a streaming query that executes infinitely, rather than completing after retrieving a single set of results 

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC --sorting is not supported when working on streaming data 
# MAGIC
# MAGIC
# MAGIC --- using order by will give us error 
# MAGIC
# MAGIC --error : Sorting is not supported on streaming DataFrames/Datasets, unless it is on aggregated DataFrame/Dataset in Complete output mode;
# MAGIC
# MAGIC select * 
# MAGIC from books_streaming_tmp_vm
# MAGIC order by author

# COMMAND ----------

# solution ---> we can use advanced methods like windowing and watermarking to achieve such operations

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC -- in order to persist incremental results, we need first to pass out logic back to pyspark dataframe api 
# MAGIC --creating another temp view 
# MAGIC --This new view is also a streaming temp view as we are creating this temp view from the result of a query against a streaming temp view 
# MAGIC
# MAGIC create or replace temp view author_count_tmp_vw as 
# MAGIC (
# MAGIC select author, count(book_id) as total_books
# MAGIC from books_streaming_tmp_vm  --streaming view 
# MAGIC group by author
# MAGIC
# MAGIC )

# COMMAND ----------


(spark.table('author_count_tmp_vw') 
    .writeStream
    .trigger(processingTime = '4 seconds')
    .outputMode('complete')
    .option('checkpointLocation',"dbfs:/mnt/demo/author_counts_checkpoint")
    .table('author_counts')
    )


# we can use spark.table() to load data from a streaming temporary view to a DataFrame


# spark always load streaming views as a streaming DataFrames, and static views as a Static DataFrame 


# incremental processing must be defined from the very begining with Read Logic to support later an incremental writing. 


################step 1 
# (spark.readStream
# .table('books')
# .createOrReplaceTempView("books_streaming_tmp_vm") #registering temporary view against stream source . it is a streaming temporary view that allows to apply most #transformation in sql the same way as we would with stattic data
# )


###############step 2  


# -- in order to persist incremental results, we need first to pass out logic back to pyspark dataframe api 
# --creating another temp view 
# --This new view is also a streaming temp view as we are creating this temp view from the result of a query against a streaming temp view 

# create or replace temp view author_count_tmp_vw as 
# (
# select author, count(book_id) as total_books
# from books_streaming_tmp_vm  --streaming view 
# group by author

# )

######################## step 3


#  use spark.table() to write data from a streaming temporary view to a DataFrame


# (spark.table('author_count_tmp_vw') 
#     .writeStream
#     .trigger(processingTime = '4 seconds') 
#     .outputMode('complete')
#     .option('checkpointLocation',"dbfs:/mnt/demo/author_counts_checkpoint")   -- to 
#     .table('author_counts')
#     )



#output mode : complete and append 

# for aggregation streaming queries, we must always use "complete" mode to overwrite the table with the new calculation


# checkpoint location to help tracking the progress of the streaming processing



# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from author_counts

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC --add new data to the books table 
# MAGIC
# MAGIC
# MAGIC insert into books
# MAGIC values('B19','introduction to modeling','manvi','computer science',28),
# MAGIC       ('b20','robot modeling and control','shakti','computer science',30);

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from author_counts where author = 'manvi'
# MAGIC
# MAGIC
# MAGIC --new data has arrived 

# COMMAND ----------

(spark.table('author_count_tmp_vw') 
    .writeStream
    .trigger(availableNow = True)  
    .outputMode('complete')
    .option('checkpointLocation',"dbfs:/mnt/demo/author_counts_checkpoint")
    .table('author_counts')
    .awaitTermination()
    )



# notice .trigger(availableNow = True)  --> the query will process all new available data and stop on ots own after execution
# .awaitTermination() --> method to block the execution of any cell in this notebook until the incremental batch's write has succeeded


#teh query will process all the available data and then stop on its own 


# COMMAND ----------


