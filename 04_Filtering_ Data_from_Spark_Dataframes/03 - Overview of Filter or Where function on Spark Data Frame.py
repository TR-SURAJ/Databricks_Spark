# Databricks notebook source
# MAGIC %run "/Users/surajthallapalli@outlook.com/04 Filtering Data from Spark Dataframes/02 - Creating Spark Data Frame for filtering"

# COMMAND ----------

help(userdf.filter)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC - filter and where are synonyms

# COMMAND ----------

userdf.filter(col('id') == 1).show(truncate = False)

# COMMAND ----------

userdf.where(col('id') == 1).show(truncate = False)

# COMMAND ----------

userdf.filter(userdf['id'] == 1).show(truncate = False)

# COMMAND ----------

userdf.where(userdf['id'] == 1).show(truncate = False)

# COMMAND ----------

userdf.filter('id = 1').show(truncate = False)

# COMMAND ----------

userdf.where('id = 1').show(truncate = False)

# COMMAND ----------

userdf.createOrReplaceTempView('users')

# COMMAND ----------

spark.sql("""
        SELECT *
        FROM users
        WHERE id = 1
 """). \
     show()

# COMMAND ----------

userdf. \
    select('id', 'current_city'). \
    show()

# COMMAND ----------

userdf. \
    select('id', 'current_city'). \
    filter(col('current_city') != 'Dallas'). \
    show()

# COMMAND ----------

userdf. \
    select('id', 'current_city'). \
    filter( (col('current_city') != 'Dallas') | (col('current_city').isNull()) ). \
    show()

# COMMAND ----------

userdf. \
    select('id', 'current_city'). \
    filter("current_city == 'Dallas'"). \
    show()

# COMMAND ----------

userdf. \
    select('id', 'current_city'). \
    filter(col('current_city') != ''). \
    show()

# COMMAND ----------

userdf. \
    select('id', 'current_city'). \
    filter("current_city != '' "). \
    show()

# COMMAND ----------


