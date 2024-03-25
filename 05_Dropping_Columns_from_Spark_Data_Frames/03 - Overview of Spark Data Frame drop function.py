# Databricks notebook source
# MAGIC %run "/Users/surajthallapalli@outlook.com/05 Dropping Columns from Spark Data Frames/02 - Creating Spark Data Frame for Dropping Columns"

# COMMAND ----------

userdf.show(truncate=False)

# COMMAND ----------

userdf.select('*').show(truncate=False)

# COMMAND ----------

help(userdf.drop)

# COMMAND ----------


