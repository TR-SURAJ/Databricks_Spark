# Databricks notebook source
users_list = [(1,"bob"),(2,"fin"),(3,"jax"),(4,"rob")]
df = spark.createDataFrame(users_list,'user_id int,user_name string')

# COMMAND ----------

df.show()

# COMMAND ----------

df.collect()

# COMMAND ----------

type(df.collect())

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

help(Row)

# COMMAND ----------

row = Row(name="Alice", age=11)

row

# COMMAND ----------

row.name

# COMMAND ----------

row.age

# COMMAND ----------

row2 = Row("Alice", 11)
row2

# COMMAND ----------


