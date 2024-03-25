# Databricks notebook source
users_list = [
    {'user_id': 1, 'user_first_name': 'Scott'},
    {'user_id': 2, 'user_first_name': 'bob'},
    {'user_id': 3, 'user_first_name': 'rick'},
    {'user_id': 4, 'user_first_name': 'bonnie'}
]

# COMMAND ----------

spark.createDataFrame(users_list)

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

help(Row)

# COMMAND ----------

users_rows = [Row(*user.values()) for user in users_list]

# COMMAND ----------

users_rows

# COMMAND ----------

df1 = spark.createDataFrame(users_rows,'id int, name string')

# COMMAND ----------

df1.show()

# COMMAND ----------

user_rows_2 = [Row(**user)  for user in users_list]

# COMMAND ----------

user_rows_2

# COMMAND ----------

df2 = spark.createDataFrame(user_rows_2)

# COMMAND ----------

df2.show()

# COMMAND ----------


