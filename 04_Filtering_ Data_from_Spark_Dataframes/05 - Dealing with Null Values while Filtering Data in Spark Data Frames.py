# Databricks notebook source
# MAGIC %run "/Users/surajthallapalli@outlook.com/04 Filtering Data from Spark Dataframes/02 - Creating Spark Data Frame for filtering"

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

userdf. \
    select('id', 'customer_from'). \
    show()

# COMMAND ----------

userdf. \
    select('id', 'customer_from'). \
    filter( col('customer_from') != None) .\
    show()

# COMMAND ----------

userdf. \
    select('id', 'customer_from'). \
    filter( col('customer_from').isNotNull() ) .\
    show()

# COMMAND ----------

userdf. \
    select('id', 'customer_from'). \
    filter('customer_from IS NOT NULL'). \
    show()

# COMMAND ----------

userdf. \
    select('id', 'customer_from'). \
    filter( col('customer_from').isNull() ) .\
    show()

# COMMAND ----------

userdf. \
    select('id', 'customer_from'). \
    filter('customer_from IS NULL'). \
    show()

# COMMAND ----------


