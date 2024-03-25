# Databricks notebook source
# MAGIC %run "/Users/surajthallapalli@outlook.com/06 Sorting Data in Spark Data Frames/02 - Creating Spark Data Frame for Sorting the Data"

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

userdf.sort('first_name', ascending = False).show()

# COMMAND ----------

userdf.sort(userdf['first_name'], ascending = False).show()

# COMMAND ----------

userdf.sort(userdf.first_name, ascending = False).show()

# COMMAND ----------

userdf.sort(userdf['first_name'].desc()).show()

# COMMAND ----------

userdf.sort(desc('first_name')).show()

# COMMAND ----------

userdf.sort(userdf['customer_from'].desc()).show()

# COMMAND ----------

userdf.\
    select('id','courses'). \
    withColumn('no_of_courses', size('courses')). \
    sort('no_of_courses', ascending = False). \
    show()

# COMMAND ----------

userdf.\
    select('id','courses'). \
    withColumn('no_of_courses', size('courses')). \
    sort(desc('no_of_courses')). \
    show()

# COMMAND ----------

userdf.\
    select('id','courses'). \
    withColumn('no_of_courses', size('courses')). \
    sort(col('no_of_courses').desc()). \
    show()

# COMMAND ----------


