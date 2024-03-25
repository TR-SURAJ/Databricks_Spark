# Databricks notebook source
# MAGIC %run "/Users/surajthallapalli@outlook.com/02 Selecting and Renaming Columns in Spark Dataframe/Creating Spark Dataframe 2023-07-11 20:32:19"

# COMMAND ----------

help(users_df.selectExpr)

# COMMAND ----------

users_df.selectExpr('*').show()

# COMMAND ----------

users_df.alias('u').selectExpr('u.*').show()

# COMMAND ----------

users_df.selectExpr('id','first_name','last_name').show()

# COMMAND ----------

from pyspark.sql.functions import col,concat,lit

# COMMAND ----------

users_df.select(
    col('id'),
    'first_name',
    'last_name',
    concat(col('first_name'),lit(','),col('last_name')).alias('full_name')
).show()

# COMMAND ----------

users_df.selectExpr('id','first_name','last_name', "concat('first_name', ',' , last_name) AS full_name").show()

# COMMAND ----------

users_df.createOrReplaceTempView('users')

# COMMAND ----------

spark.sql("""
          SELECT id, first_name, last_name,
          concat(first_name, ',',last_name) AS full_name    
          FROM users      
 """).show()

# COMMAND ----------


