# Databricks notebook source
# MAGIC %run "/Users/surajthallapalli@outlook.com/08 Joining Spark Data Frames/02 - Setup Datasets to perform joins"

# COMMAND ----------

users_df.\
    join(course_enrolments_df, users_df.user_id == course_enrolments_df.user_id, 'inner').\
    show()

# COMMAND ----------

users_df.\
    join(course_enrolments_df, users_df.user_id == course_enrolments_df.user_id, 'outer').\
    show()

# COMMAND ----------


