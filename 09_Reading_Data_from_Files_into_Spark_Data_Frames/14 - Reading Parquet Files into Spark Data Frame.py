# Databricks notebook source
help(spark.read.parquet)

# COMMAND ----------

import getpass
username = getpass.getuser()

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/retail_db_parquet/orders')

# COMMAND ----------

df = spark.read.parquet(f'/user/{username}/retail_db_parquet/orders')

# COMMAND ----------

df.inputFiles()

# COMMAND ----------

df.dtypes

# COMMAND ----------

df.show()

# COMMAND ----------

df = spark.read.format('parquet').load(f'/user/{username}/retail_db_parquet/orders')

# COMMAND ----------

df.inputFiles()

# COMMAND ----------


