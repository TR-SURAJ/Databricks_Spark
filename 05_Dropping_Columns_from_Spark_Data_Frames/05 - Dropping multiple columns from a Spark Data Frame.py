# Databricks notebook source
# MAGIC %run "/Users/surajthallapalli@outlook.com/05 Dropping Columns from Spark Data Frames/02 - Creating Spark Data Frame for Dropping Columns"

# COMMAND ----------

userdf.drop('first_name','last_name').printSchema()

# COMMAND ----------

userdf.drop('first_name','last_name').show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

userdf.drop(col('first_name')).show()

# COMMAND ----------

userdf.drop(col('first_name'), col('id')).show() ## ???????

# COMMAND ----------

help(userdf.drop)

# COMMAND ----------


