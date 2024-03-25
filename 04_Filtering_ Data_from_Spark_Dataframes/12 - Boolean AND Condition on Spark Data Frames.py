# Databricks notebook source
# MAGIC %run "/Users/surajthallapalli@outlook.com/04 Filtering Data from Spark Dataframes/02 - Creating Spark Data Frame for filtering"

# COMMAND ----------

userdf. \
    select('id', 'gender', 'is_customer'). \
    show()

# COMMAND ----------

userdf. \
    select('id', 'gender', 'is_customer'). \
    printSchema()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

userdf. \
    select('id', 'gender', 'is_customer'). \
    filter(col('gender') == 'male').\
    show()

# COMMAND ----------

userdf. \
    select('id', 'gender', 'is_customer'). \
    filter(col('gender') = 'male').\
    show()

# COMMAND ----------

userdf. \
    select('id', 'gender', 'is_customer'). \
    filter(col('gender') == 'male' & col('is_customer') == True).\
    show()

# COMMAND ----------

userdf. \
    select('id', 'gender', 'is_customer'). \
    filter( (col('gender') == 'male') & col('is_customer') == True).\
    show()

# COMMAND ----------

userdf. \
    select('id', 'gender', 'is_customer'). \
    filter(" gender = 'male' AND is_customer = true ").\
    show()

# COMMAND ----------

userdf. \
    select('id', 'gender', 'is_customer'). \
    filter('gender = "male" AND is_customer = true').\
    show()

# COMMAND ----------

userdf. \
    select('id', 'gender', 'is_customer'). \
    filter('gender = "male" AND is_customer = True').\
    show()

# COMMAND ----------

userdf. \
    select('id', 'gender', 'is_customer'). \
    filter('gender = "male" AND is_customer = "true"').\
    show()

# COMMAND ----------

userdf. \
    select('id', 'gender', 'customer_from'). \
    filter( (col('customer_from') > '2021-01-15') & (col('customer_from') < '2023-01-12')).\
    show()

# COMMAND ----------

userdf. \
    select('id', 'gender', 'customer_from'). \
    filter("customer_from > '2021-01-15' AND customer_from < '2023-01-12'"). \
    show()

# COMMAND ----------


