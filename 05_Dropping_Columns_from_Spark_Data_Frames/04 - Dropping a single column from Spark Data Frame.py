# Databricks notebook source
# MAGIC %run "/Users/surajthallapalli@outlook.com/05 Dropping Columns from Spark Data Frames/02 - Creating Spark Data Frame for Dropping Columns"

# COMMAND ----------

userdf.printSchema()

# COMMAND ----------

userdf.drop('last_updated_ts').printSchema()

# COMMAND ----------

userdf.drop(userdf['last_updated_ts']).printSchema()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

userdf.drop(col('last_updated_ts')).printSchema()

# COMMAND ----------

# If we have column name which does not exist, the column will be ignored
userdf.drop(col('abc')).printSchema()

# COMMAND ----------


