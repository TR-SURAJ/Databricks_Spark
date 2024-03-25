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

help(date_add)

# COMMAND ----------

help(date_sub)

# COMMAND ----------

datedf.\
    withColumn("date_add_date", date_add("date", 10)). \
    withColumn("date_add_time", date_add("time", 10)). \
    withColumn("date_sub_date", date_sub("date",10)). \
    withColumn("date_sub_time", date_sub("time", 10)). \
    show(truncate=False)

# COMMAND ----------

help(date_diff)

# COMMAND ----------

datedf.show(truncate=False)

# COMMAND ----------

datedf. \
    withColumn("datediff_date", date_diff(current_date(), "date")). \
    withColumn("datediff_time", date_diff(current_timestamp(), "time")). \
    show(truncate = False)

# COMMAND ----------

help(months_between)

# COMMAND ----------

help(add_months)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC most of date functions return date inspite of using it on timetsamp

# COMMAND ----------

datedf. \
    withColumn("months_between_date", round(months_between(current_date(), "date"),2)). \
    withColumn("months_between_time", round(months_between(current_timestamp(), "time"),2)). \
    withColumn("add_months_date", add_months("date", 3)). \
    withColumn("add_months_time", add_months("time",3)). \
    show(truncate = False)

# COMMAND ----------


