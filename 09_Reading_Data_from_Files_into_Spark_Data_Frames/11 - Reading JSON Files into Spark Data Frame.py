# Databricks notebook source
help(spark.read.json)

# COMMAND ----------

# Schema will be inferred by default
df = spark.read.json('/public/retail_db_json/orders')

# COMMAND ----------

df.show()

# COMMAND ----------

df = spark.read.format('json').load('/public/retail_db_json/orders')

# COMMAND ----------

df.show()

# COMMAND ----------

df.inputFiles()

# COMMAND ----------

df.dtypes

# COMMAND ----------

df.show()

# COMMAND ----------


