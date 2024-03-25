# Databricks notebook source
# MAGIC %run "/Users/surajthallapalli@outlook.com/05 Dropping Columns from Spark Data Frames/02 - Creating Spark Data Frame for Dropping Columns"

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

pii = ['first_name','last_name','email','phone_numbers']

# COMMAND ----------

# This will fail. We need to convert list to varying arguments to get it working
userdf_nopii = userdf.drop(pii)

# COMMAND ----------

userdf_nopii = userdf.drop(*pii)

# COMMAND ----------

userdf_nopii.show()

# COMMAND ----------

type(pii)

# COMMAND ----------

#If any col which doesnt exist in dataframe it will be ignored on drop
pii = ['first_name','last_name','email','phone_numbers','abc']

# COMMAND ----------

userdf.drop(*pii).show()

# COMMAND ----------


