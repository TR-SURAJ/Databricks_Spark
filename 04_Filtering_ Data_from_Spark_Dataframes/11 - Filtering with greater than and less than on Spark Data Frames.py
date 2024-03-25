# Databricks notebook source
# MAGIC %run "/Users/surajthallapalli@outlook.com/04 Filtering Data from Spark Dataframes/02 - Creating Spark Data Frame for filtering"

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

userdf. \
    select('id', 'amount_paid'). \
    show()

# COMMAND ----------

userdf. \
    select('id', 'amount_paid'). \
    filter( col('amount_paid') > 600 ). \
    show()

# COMMAND ----------

userdf. \
    select('id', 'amount_paid'). \
    filter( (col('amount_paid') > 600) & (isnan(col('amount_paid')) == False)). \
    show()

# COMMAND ----------

userdf. \
    select('id', 'amount_paid'). \
    filter(' amount_paid > 600 AND isnan(amount_paid) = false'). \
    show()

# COMMAND ----------

userdf. \
    select('id', 'amount_paid'). \
    filter( (col('amount_paid') > 600) & (isnan(col('amount_paid')) == True)). \
    show()

# COMMAND ----------

userdf. \
    select('id', 'customer_from'). \
    show()


# COMMAND ----------

userdf. \
    select('id', 'customer_from'). \
    printSchema()


# COMMAND ----------

userdf. \
    select('id', 'customer_from'). \
    filter( col('customer_from') > '2021-01-15' ). \
    show()


# COMMAND ----------

userdf. \
    select('id', 'customer_from'). \
    filter('customer_from > "2021-01-15"'). \
    show()


# COMMAND ----------


