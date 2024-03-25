# Databricks notebook source
# MAGIC %run "/Users/surajthallapalli@outlook.com/04 Filtering Data from Spark Dataframes/02 - Creating Spark Data Frame for filtering"

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

userdf. \
    select('id', 'gender', 'amount_paid'). \
    filter( (col('is_customer') == True) | (isnan(col('amount_paid')) == True ) ). \
    show()

# COMMAND ----------

userdf. \
    select('id', 'gender', 'amount_paid'). \
    filter(' is_customer == True OR isnan(amount_paid) == True  '). \
    show()

# COMMAND ----------

userdf. \
    select('id', 'gender', 'amount_paid'). \
    filter(' is_customer == true OR isnan(amount_paid) == true  '). \
    show()

# COMMAND ----------

userdf. \
    select('id', 'gender', 'amount_paid'). \
    filter(' is_customer == "true" OR isnan(amount_paid) == "true"  '). \
    show()

# COMMAND ----------

userdf. \
    select('id', 'is_customer', 'last_updated_ts'). \
    filter( (col('is_customer') == False) | (col('last_updated_ts') < '2021-03-01' ) ). \
    show()

# COMMAND ----------


