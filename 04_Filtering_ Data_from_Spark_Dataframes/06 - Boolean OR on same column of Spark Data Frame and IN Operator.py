# Databricks notebook source
# MAGIC %run "/Users/surajthallapalli@outlook.com/04 Filtering Data from Spark Dataframes/02 - Creating Spark Data Frame for filtering"

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

userdf. \
    select('id', 'customer_from'). \
    filter( (col('customer_from').isNull()) | (col('phone_numbers').isNull()) ).\
    show()

# COMMAND ----------

userdf. \
    filter(" customer_from IS NULL OR  phone_numbers IS NULL").\
    show()

# COMMAND ----------

# this will fail as conditions are not enclosed in circular brackets
userdf. \
    select('id', 'customer_from'). \
    filter( col('customer_from') == '' | col('phone_numbers').isNull() ).\
    show()

# COMMAND ----------

userdf. \
    select('id', 'customer_from'). \
    filter( (col('customer_from').isNull()) | col('phone_numbers').isNull() ).\
    show()

# COMMAND ----------

userdf. \
    filter(" customer_from = '' OR  phone_numbers IS NULL").\
    show()

# COMMAND ----------

userdf. \
    select('id', 'current_city'). \
    filter( (col('current_city') == 'Dallas') | (col('current_city') == 'Illions') ).\
    show()

# COMMAND ----------

userdf. \
    select('id', 'current_city'). \
    filter(" current_city = 'Dallas' OR current_city = 'Illions' ").\
    show()

# COMMAND ----------

userdf. \
    select('id', 'current_city'). \
    filter(col('current_city').isin('Dallas','Illions')).\
    show()

# COMMAND ----------

userdf. \
    select('id', 'current_city'). \
    filter("current_city IN ('Dallas', 'Illions') "). \
    show()


# COMMAND ----------

userdf. \
    select('id', 'customer_from'). \
    filter(col('customer_from').isin('2021-01-15','2022-05-15', 'NULL')).\
    show()

# COMMAND ----------

userdf. \
    select('id', 'customer_from'). \
    filter("customer_from IN ('2021-01-15','2022-05-15', NULL) "). \
    show()


# COMMAND ----------

userdf. \
    select('id', 'customer_from'). \
    filter("customer_from IN ('2021-01-15','2022-05-15') OR customer_from IS NULL"). \
    show()


# COMMAND ----------


