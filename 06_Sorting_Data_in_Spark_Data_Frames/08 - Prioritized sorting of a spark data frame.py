# Databricks notebook source
# MAGIC %run "/Users/surajthallapalli@outlook.com/06 Sorting Data in Spark Data Frames/02 - Creating Spark Data Frame for Sorting the Data"

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

userdf.show()

# COMMAND ----------

help(when)

# COMMAND ----------

course_level = when(col('suitable_for') == 'Beginner',0).otherwise(when(col('suitable_for') == 'Intermediate',1).otherwise(2))

# COMMAND ----------

userdf.\
    orderBy(course_level,col('number_of_ratings').desc()). \
    show()

# COMMAND ----------


