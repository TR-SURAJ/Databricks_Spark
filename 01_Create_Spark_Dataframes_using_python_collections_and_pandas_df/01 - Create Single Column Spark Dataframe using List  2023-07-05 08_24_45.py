# Databricks notebook source
age_list = [1,2,3,4,5]

# COMMAND ----------

type(age_list)

# COMMAND ----------

spark

# COMMAND ----------

help(spark.createDataFrame)

# COMMAND ----------

spark.createDataFrame(age_list)

# COMMAND ----------

spark.createDataFrame(age_list,'int')

# COMMAND ----------

from pyspark.sql.types import IntegerType,StringType

# COMMAND ----------

spark.createDataFrame(age_list, IntegerType())

# COMMAND ----------

names = ['abc','xyz','jfj']

# COMMAND ----------

spark.createDataFrame(names,'string')

# COMMAND ----------

spark.createDataFrame(names, StringType())

# COMMAND ----------


