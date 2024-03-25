# Databricks notebook source
users_list = [[1,"bob"], [2,"max"], [3,"pix"], [4,"rix"]]

# COMMAND ----------

type(users_list)

# COMMAND ----------

type(users_list[2])

# COMMAND ----------

spark.createDataFrame(users_list)

# COMMAND ----------

spark.createDataFrame(users_list,'id int, name string')

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

users_rows = [Row(*user) for user in users_list]

# COMMAND ----------

users_rows

# COMMAND ----------

spark.createDataFrame(users_rows,'user_id int, user_first_name string')

# COMMAND ----------


