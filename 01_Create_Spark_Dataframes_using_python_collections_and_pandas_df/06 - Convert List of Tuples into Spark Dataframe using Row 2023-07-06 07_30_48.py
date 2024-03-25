# Databricks notebook source
users_list = [(1, 'Scott'),(2, 'Bob'),(3, 'Max'),(4, 'Rin')]

# COMMAND ----------

type(users_list[1])

# COMMAND ----------

spark.createDataFrame(users_list)

# COMMAND ----------

spark.createDataFrame(users_list,'id int, name string')

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

user_rows = [Row(*user) for user in users_list]

# COMMAND ----------

user_rows

# COMMAND ----------


