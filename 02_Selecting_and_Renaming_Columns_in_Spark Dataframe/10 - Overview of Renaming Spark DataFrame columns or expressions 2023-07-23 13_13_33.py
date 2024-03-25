# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC * We can rename column or expression using **alias** as part of select
# MAGIC * We can add or rename column or expression using **withColumn** on top of dataframe
# MAGIC * We can rename one column at a time using **withColumnRenamed** on top of Dataframe
# MAGIC * We typically use **withColumn** to perform row level transformations and then to provide a name to the result. If we provide the same name as existing column, then the column will be replaced with new one
# MAGIC * If we want to just rename the column then it is better to use **withColumnRenamed**
# MAGIC * If we want to apply any transformation, we need to either use **select** or **withColumn**
# MAGIC * We can rename bunch of columns using **toDF**.
# MAGIC

# COMMAND ----------

# MAGIC %run "/Users/surajthallapalli@outlook.com/02 Selecting and Renaming Columns in Spark Dataframe/Creating Spark Dataframe 2023-07-11 20:32:19"

# COMMAND ----------

users_df.select('id','first_name','last_name').show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

users_df.select(
    "id",
    "first_name",
    "last_name",
    concat("first_name", lit(","), "last_name").alias("full_name")
).show()

# COMMAND ----------

users_df.select("id","first_name","last_name").withColumn("full_name", concat("first_name",lit(","),"last_name") ).show()

# COMMAND ----------

users_df.select("id","first_name","last_name").withColumn("full_name", concat("first_name",lit(","),"last_name").alias("fn") ).show()

# COMMAND ----------

users_df.select("id","first_name","last_name").withColumn("first_name", concat("first_name",lit(","),"last_name").alias("fn") ).show()

# COMMAND ----------

users_df.select('id','first_name','last_name').withColumn('fn', col('first_name')).show()

# COMMAND ----------

users_df.select('id','first_name','last_name').withColumn('fn', users_df['first_name']).show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC * Find the number courses that each id has taken

# COMMAND ----------

users_df.select('id','courses').show()

# COMMAND ----------

users_df.select('id','courses').withColumn('course_count', size('courses')).show()

# COMMAND ----------


