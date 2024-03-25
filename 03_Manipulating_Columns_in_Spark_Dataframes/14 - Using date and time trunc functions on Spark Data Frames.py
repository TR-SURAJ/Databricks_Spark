# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

datetimes = [
    ("2014-02-28", "2014-02-28 10:00:00.123"),
    ("2016-03-29", "2016-03-29 11:23:00.234"),
    ("2018-04-20", "2018-04-20 12:34:00.543"),
    ("2019-05-12", "2019-05-12 13:21:00.567")
]

# COMMAND ----------

datedf = spark.createDataFrame(datetimes, schema = 'date STRING, time STRING')

# COMMAND ----------

datedf.show(truncate=False)

# COMMAND ----------

help(trunc)

# COMMAND ----------

help(date_trunc)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC -  Get beginning month date using date field and beginning year date using time field

# COMMAND ----------

datedf. \
    withColumn("date_trunc_by_month", trunc("date","MM")). \
    withColumn("time_trunc_by_year", trunc("time", "yy")). \
    show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC - Get beginning hour time using date and time field

# COMMAND ----------

help(date_trunc)

# COMMAND ----------

datedf. \
    withColumn("date_trunc_by_month", date_trunc('MM', "date")). \
    withColumn("time_trunc_by_year", date_trunc('yy', "time")). \
    show(truncate = False)

# COMMAND ----------

datedf. \
    withColumn("date_dt", date_trunc("HOUR", "date")). \
    withColumn("time_dt", date_trunc("HOUR", "time")). \
    withColumn("time_dt1", date_trunc("dd", "time")). \
    show(truncate=False)

# COMMAND ----------


