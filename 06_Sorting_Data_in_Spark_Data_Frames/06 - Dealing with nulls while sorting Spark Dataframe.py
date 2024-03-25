# Databricks notebook source
# MAGIC %run "/Users/surajthallapalli@outlook.com/06 Sorting Data in Spark Data Frames/02 - Creating Spark Data Frame for Sorting the Data"

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

userdf.show()

# COMMAND ----------

userdf. \
    select('id','customer_from').\
    orderBy('customer_from').\
    show()

# COMMAND ----------

userdf. \
    select('id','customer_from').\
    orderBy(userdf['customer_from'].asc_nulls_last()).\
    show()

# COMMAND ----------

userdf. \
    select('id','customer_from').\
    orderBy(col('customer_from').asc_nulls_last()).\
    show()

# COMMAND ----------

userdf. \
    select('id','customer_from').\
    orderBy(userdf['customer_from'].desc()).\
    show()

# COMMAND ----------

userdf. \
    select('id','customer_from').\
    orderBy(col('customer_from').desc_nulls_first()).\
    show()

# COMMAND ----------


