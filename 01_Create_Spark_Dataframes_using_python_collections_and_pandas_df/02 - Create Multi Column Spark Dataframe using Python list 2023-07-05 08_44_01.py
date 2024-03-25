# Databricks notebook source
age_list = [(21,),(23,),(34,),(32,)]

# COMMAND ----------

type(age_list)

# COMMAND ----------

type(age_list[2])

# COMMAND ----------

spark.createDataFrame(age_list)

# COMMAND ----------

spark.createDataFrame(age_list,'age int')

# COMMAND ----------

spark.createDataFrame(age_list,'age string')

# COMMAND ----------

users_list = [(1,"bob"),(2,"max"),(3,"lov"),(4,"fin")]

# COMMAND ----------

spark.createDataFrame(users_list)

# COMMAND ----------

spark.createDataFrame(users_list,'id int,name string')

# COMMAND ----------


