# Databricks notebook source
# MAGIC %run "/Users/surajthallapalli@outlook.com/06 Sorting Data in Spark Data Frames/02 - Creating Spark Data Frame for Sorting the Data"

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

userdf.show()

# COMMAND ----------

userdf. \
    sort('first_name','last_name').\
    show()

# COMMAND ----------

userdf. \
    sort(userdf['first_name'],userdf['last_name']).\
    show()

# COMMAND ----------

userdf. \
    sort(userdf['first_name'],userdf['amount_paid'].desc()).\
    show()

# COMMAND ----------

userdf. \
    sort(userdf['first_name'],desc(userdf['amount_paid'])).\
    show()

# COMMAND ----------

userdf. \
    sort('first_name',desc('amount_paid')).\
    show()

# COMMAND ----------

userdf. \
    sort(['first_name','amount_paid'], ascending = [1,0]).\
    show()

# COMMAND ----------


