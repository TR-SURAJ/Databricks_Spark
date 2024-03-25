# Databricks notebook source
# MAGIC %run "/Users/surajthallapalli@outlook.com/08 Joining Spark Data Frames/02 - Setup Datasets to perform joins"

# COMMAND ----------

help(courses_df.crossJoin)

# COMMAND ----------

users_df.\
    crossJoin(courses_df).\
    show()

# COMMAND ----------

users_df.\
    crossJoin(courses_df).\
    count()

# COMMAND ----------

# Even join without conditions result in cross join or cartesian product
users_df.\
    join(courses_df).\
    count()

# COMMAND ----------

users_df.\
    join(courses_df, how = 'cross').\
    count()

# COMMAND ----------


