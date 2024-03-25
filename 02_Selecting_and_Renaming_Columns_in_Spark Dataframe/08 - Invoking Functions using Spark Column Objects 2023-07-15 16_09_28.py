# Databricks notebook source
# MAGIC %run "/Users/surajthallapalli@outlook.com/02 Selecting and Renaming Columns in Spark Dataframe/Creating Spark Dataframe 2023-07-11 20:32:19"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC * Concatenate `first_name` and `last_name` to generate `full_name`

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

help(concat)

# COMMAND ----------

full_name_col = concat(col('first_name'), lit(','), col('last_name'))

# COMMAND ----------

full_name_col

# COMMAND ----------

full_name_alias = full_name_col.alias('full_name')

# COMMAND ----------

full_name_alias

# COMMAND ----------

users_df.select('id', full_name_alias).show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC * Convert data type of customer_from date to numeric type

# COMMAND ----------

from pyspark.sql.functions import date_format 

# COMMAND ----------

date_format('customer_from','yyyymmdd').cast('int').alias('customer_from')

# COMMAND ----------

customer_from_alias = date_format('customer_from','yyyymmdd').cast('int').alias('customer_from')

# COMMAND ----------

users_df.select('id',customer_from_alias).show()

# COMMAND ----------


