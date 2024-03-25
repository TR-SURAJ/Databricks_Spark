# Databricks notebook source
# MAGIC %run "/Users/surajthallapalli@outlook.com/08 Joining Spark Data Frames/02 - Setup Datasets to perform joins"

# COMMAND ----------

course_enrolments_df.\
    join(users_df, users_df.user_id == course_enrolments_df.user_id, 'right').\
    show()

# COMMAND ----------

course_enrolments_df.\
    join(users_df, 'user_id', 'right').\
    show()

# COMMAND ----------


