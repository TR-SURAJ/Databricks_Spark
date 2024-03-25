# Databricks notebook source
# MAGIC %run "/Users/surajthallapalli@outlook.com/04 Filtering Data from Spark Dataframes/02 - Creating Spark Data Frame for filtering"

# COMMAND ----------

userdf. \
    select('id', 'email', 'last_updated_ts'). \
    show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

c = col('last_updated_ts')

# COMMAND ----------

c.between

# COMMAND ----------

help(c.between)

# COMMAND ----------

userdf. \
    select('id', 'email', 'last_updated_ts'). \
    filter( col('last_updated_ts').between('2018-02-15', '2023-02-15') ). \
    show()

# COMMAND ----------

userdf. \
    select('id', 'email', 'last_updated_ts'). \
    filter( col('last_updated_ts').between('2018-02-15 00:00:00', '2023-02-15 23:59:59') ). \
    show()

# COMMAND ----------

userdf. \
    select('id', 'email', 'last_updated_ts'). \
    filter(" last_updated_ts BETWEEN '2018-02-15 00:00:00' AND '2023-02-15 23:59:59' "). \
    show()

# COMMAND ----------

userdf. \
    select('id', 'amount_paid'). \
    show()

# COMMAND ----------

userdf. \
    select('id', 'amount_paid'). \
    filter( col('amount_paid').between(850, 900) ). \
    show()

# COMMAND ----------

userdf. \
    select('id', 'amount_paid'). \
    filter(" amount_paid BETWEEN 500 AND 1000 "). \
    show()

# COMMAND ----------

userdf. \
    select('id', 'amount_paid'). \
    filter('amount_paid BETWEEN "500" AND "1000"'). \
    show()

# COMMAND ----------


