# Databricks notebook source

#  check dbutils notebook first 


#  reading a csv file and creating a df 

adult_census_data = spark.read.csv("dbfs:/databricks-datasets/adult/adult.data",header= True)

type(adult_census_data)

# COMMAND ----------

# we can access the underline RDD of a dataframe using rdd function

# checking the underline rdd of the df 
adult_census_rdd = adult_census_data.rdd 

type(adult_census_rdd)

# COMMAND ----------

#  display the content of the rdd 

adult_census_rdd.collect()
#  .collect is action 


# COMMAND ----------

# MAGIC %md
# MAGIC Lazy evaluation : Transformations are lazy in spark 
# MAGIC
# MAGIC means that spark will wait untill the very last moment to execute the graph of computation instructions
# MAGIC
# MAGIC in spark , instead of modifying the data immediately when we express some operation, we build up a plan of transformations that we would like to apply to our source data 
# MAGIC
# MAGIC

# COMMAND ----------

# count 

adult_census_rdd.count()

# COMMAND ----------

# check the first row of rdd

adult_census_rdd.first()

# COMMAND ----------

#  perform operation on very record in the rdd : --> use map fucntion

adult_census_rdd.map(lambda row: (row[1],row[3],row[5]))  

# row[1],row[3],row[5] -- > means col 1, col 3, col 5
# map operation is a transformation operation


# transformation in spark are not materialised till we request for result 


# COMMAND ----------

adult_census_rdd.map(lambda row: (row[1],row[3],row[5]))\
                .collect()


#  use collect action to materilise the transformation

# COMMAND ----------

# We can use column names as well to fecth the data 


adult_census_rdd.map(lambda row: (row[' State-gov'],row[' Adm-clerical'],row[' <=50K']))\
                .collect()

# COMMAND ----------

# using filter operation 


adult_census_rdd_filtered = adult_census_rdd.filter(lambda row: row[' <=50K'] == ' <=50K')

# COMMAND ----------

adult_census_rdd_filtered.collect()

# COMMAND ----------

# another dataset 


dbutils.fs.ls("dbfs:/")

# COMMAND ----------

dbutils.fs.ls('dbfs:/databricks-datasets/')

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/databricks-datasets/bikeSharing/'))

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/databricks-datasets/bikeSharing/data-001/'))

# COMMAND ----------

bike_sharing_data = spark.read.format('csv')\
                     .option('inferSchema', 'True')\
                     .option('header', 'True')\
                     .option('sep', ',')\
                     .load('/databricks-datasets/bikeSharing/data-001/day.csv')


# COMMAND ----------

# 1. check the underline rdd 
underline_rdd = bike_sharing_data.rdd
type(underline_rdd)


#.collect action to see the rdd


underline_rdd.collect()

# COMMAND ----------

# check the first row of  rdd

underline_rdd.first()

# COMMAND ----------

#view the result of the df 

bike_sharing_data.show(10)


# COMMAND ----------

# check few columns data only 

biks_share = bike_sharing_data.select('weekday','temp')  #transformation
biks_share.show(2) #action 

# COMMAND ----------

#filter on df 

bike_sharing_data.filter(bike_sharing_data['cnt']> 1000).show()

# COMMAND ----------


