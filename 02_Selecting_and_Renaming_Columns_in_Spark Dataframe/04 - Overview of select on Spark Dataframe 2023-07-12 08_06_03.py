# Databricks notebook source
# MAGIC %run "/Users/surajthallapalli@outlook.com/02 Selecting and Renaming Columns in Spark Dataframe/Creating Spark Dataframe 2023-07-11 20:32:19"

# COMMAND ----------

users_df.show()

# COMMAND ----------

help(users_df.select)

# COMMAND ----------

users_df.select('*').show()

# COMMAND ----------

users_df.select('id','email').show()

# COMMAND ----------

users_df.select(['id','first_name','last_name']).show()

# COMMAND ----------

# Defining alias to datafrmae

users_df.alias('u').select('u.*').show()

# COMMAND ----------

users_df.alias('u').select('u.id','u.first_name','u.last_name').show()

# COMMAND ----------

from pyspark.sql.functions import col,concat,lit

# COMMAND ----------

users_df.select(col('id'),'first_name','last_name').show()

# COMMAND ----------

users_df.select(
    col('id'),
    'first_name',
    'last_name',
    concat(col('first_name'),lit(','),col('last_name')).alias('full_name')
).show()

# COMMAND ----------

users_df.select(
    'id',
    'first_name',
    'last_name',
    concat('first_name',lit(','),'last_name').alias('full_name')
).show()

# COMMAND ----------


