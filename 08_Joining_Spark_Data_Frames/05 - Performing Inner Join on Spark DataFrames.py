# Databricks notebook source
# MAGIC %run "/Users/surajthallapalli@outlook.com/08 Joining Spark Data Frames/02 - Setup Datasets to perform joins"

# COMMAND ----------

help(courses_df.join)

# COMMAND ----------

# MAGIC %md
# MAGIC - Get the user details who have enrolled for the ourses
# MAGIC   - Need to join **users_df** and **course_enrolments_df**
# MAGIC   - Here are the fields that needs to be displayed
# MAGIC     - All fields from users_df
# MAGIC     - course_id and course_enrolment_id from course_enrolments_df

# COMMAND ----------

users_df.\
    join(course_enrolments_df, users_df.user_id == course_enrolments_df.user_id).\
    show()

# COMMAND ----------

#as both data frames have user id using same name, we can pass column name as string as well. Advantage of this approach is the condition column is mentioned once in df unlike above approach
users_df.\
    join(course_enrolments_df, 'user_id').\
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC - To get all columns from users_df, course_id and course_enrolmane_id from course_enrolments_df

# COMMAND ----------

users_df.\
    join(course_enrolments_df, users_df.user_id == course_enrolments_df.user_id).\
    select(users_df['*'],course_enrolments_df['course_id'],course_enrolments_df['course_enrolmane_id']).\
    show()

# COMMAND ----------

# using alias
users_df.alias('u').\
    join(course_enrolments_df.alias('ce'), users_df.user_id == course_enrolments_df.user_id).\
    select('u.*','course_id','course_enrolmane_id').\
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - Get number of courses enrolled by each user. Fails as user_id is part of both the data frames

# COMMAND ----------

# using alias
users_df.alias('u').\
    join(course_enrolments_df.alias('ce'), users_df.user_id == course_enrolments_df.user_id).\
    groupBy('user_id').\
    count().\
    show()

# COMMAND ----------

users_df.alias('u').\
    join(course_enrolments_df.alias('ce'), users_df.user_id == course_enrolments_df.user_id).\
    groupBy(users_df['user_id']).\
    count().\
    show()

# COMMAND ----------

# using alias
users_df.alias('u').\
    join(course_enrolments_df.alias('ce'), users_df.user_id == course_enrolments_df.user_id).\
    groupBy('u.user_id').\
    count().\
    show()

# COMMAND ----------

users_df.\
    join(course_enrolments_df, 'user_id').\
    groupBy('user_id').\
    count().\
    show()

# COMMAND ----------


