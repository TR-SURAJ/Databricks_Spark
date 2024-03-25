# Databricks notebook source
# MAGIC %run "/Users/surajthallapalli@outlook.com/08 Joining Spark Data Frames/02 - Setup Datasets to perform joins"

# COMMAND ----------

help(courses_df.alias)

# COMMAND ----------

type(courses_df.alias('c'))

# COMMAND ----------

courses_df.alias('c').select('c.course_id').show()

# COMMAND ----------


