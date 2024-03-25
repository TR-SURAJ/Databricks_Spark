# Databricks notebook source
# MAGIC %run "/Users/surajthallapalli@outlook.com/02 Selecting and Renaming Columns in Spark Dataframe/Creating Spark Dataframe 2023-07-11 20:32:19"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC * Rename id to user_id
# MAGIC * Rename first_name to user_first_name
# MAGIC * Rename last_name to user_last_name
# MAGIC * Also add new column by name user_full_name which is derived by concatenationg first_name and last_name with , in between

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

users_df.select(
    col('id').alias('user_id'),
    col('first_name').alias('user_first_name'),
    col('last_name').alias('user_last_name'),
    concat('first_name',lit(','),'last_name').alias('full_name_method_1'),
    concat(col('first_name'), lit(','), col('last_name')).alias('full_name_method_2')
).show()

# COMMAND ----------

users_df.select(
    users_df['id'].alias('user_id'),
    users_df['first_name'].alias('user_first_name'),
    users_df['last_name'].alias('user_last_name'),
    concat('first_name',lit(','),'last_name').alias('full_name_method_1'),
    concat(users_df['first_name'], lit(','), users_df['last_name']).alias('full_name_method_2')
).show()

# COMMAND ----------

users_df.select(
    users_df['id'].alias('user_id'),
    users_df['first_name'].alias('user_first_name'),
    users_df['last_name'].alias('user_last_name'),
    concat('first_name',lit(','),'last_name').alias('full_name_method_1'),
    concat(users_df['first_name'], lit(','), users_df['last_name']).alias('full_name_method_2')
).\
withColumn('user_full_name', concat(users_df['first_name'], lit(','), users_df['last_name'])).show()

# COMMAND ----------

users_df.select(
    users_df['id'].alias('user_id'),
    users_df['first_name'].alias('user_first_name'),
    users_df['last_name'].alias('user_last_name'),
    concat('first_name',lit(','),'last_name').alias('full_name_method_1'),
    concat(users_df['first_name'], lit(','), users_df['last_name']).alias('full_name_method_2')
).\
withColumn('user_full_name', concat(users_df['user_first_name'], lit(','), users_df['user_last_name'])).show()

# COMMAND ----------

users_df.select(
    users_df['id'].alias('user_id'),
    users_df['first_name'].alias('user_first_name'),
    users_df['last_name'].alias('user_last_name'),
    concat('first_name',lit(','),'last_name').alias('full_name_method_1'),
    concat(users_df['first_name'], lit(','), users_df['last_name']).alias('full_name_method_2')
).\
withColumn('user_full_name', concat(col('user_first_name'), lit(','), col('user_last_name'))).show()

# COMMAND ----------

users_df.\
withColumn('user_full_name', concat(col('first_name'), lit(','), col('last_name'))).\
select(
    users_df['id'].alias('user_id'),
    users_df['first_name'].alias('user_first_name'),
    users_df['last_name'].alias('user_last_name')    
).show()

# COMMAND ----------


