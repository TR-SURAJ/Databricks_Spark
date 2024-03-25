# Databricks notebook source
# MAGIC %run "/Users/surajthallapalli@outlook.com/06 Sorting Data in Spark Data Frames/02 - Creating Spark Data Frame for Sorting the Data"

# COMMAND ----------

userdf.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

userdf.sort('first_name').show()

# COMMAND ----------

userdf.sort(userdf.first_name).show()

# COMMAND ----------

userdf.sort(userdf['first_name']).show(truncate=False)

# COMMAND ----------

userdf.sort(col('first_name')).show()

# COMMAND ----------

userdf.sort('customer_from').show()

# COMMAND ----------

userdf.sort(size('courses')).show()

# COMMAND ----------


