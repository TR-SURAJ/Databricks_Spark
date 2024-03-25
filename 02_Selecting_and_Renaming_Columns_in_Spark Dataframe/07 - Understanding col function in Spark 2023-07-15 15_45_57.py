# Databricks notebook source
# MAGIC %run "/Users/surajthallapalli@outlook.com/02 Selecting and Renaming Columns in Spark Dataframe/Creating Spark Dataframe 2023-07-11 20:32:19"

# COMMAND ----------

users_df.select('id','first_name','last_name').show()

# COMMAND ----------

cols = ['id','first_name','last_name']
users_df.select(*cols).show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

help(col)

# COMMAND ----------

user_id = col('id')
users_df.select(user_id).show()

# COMMAND ----------

from pyspark.sql.functions import date_format

# COMMAND ----------

users_df.select(
    col('id'),
    date_format('customer_from','yyyyMMdd')
).show()

# COMMAND ----------

users_df.select(
    col('id'),
    date_format('customer_from','yyyyMMdd').cast('int').alias('customer_from')
).show()

# COMMAND ----------

cols = [col('id'),date_format('customer_from','yyyyMMdd').cast('int').alias('customer_from')]
users_df.select(*cols).show()

# COMMAND ----------


