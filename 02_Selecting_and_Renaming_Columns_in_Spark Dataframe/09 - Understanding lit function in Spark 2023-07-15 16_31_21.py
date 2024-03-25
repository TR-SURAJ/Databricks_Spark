# Databricks notebook source
# MAGIC %run "/Users/surajthallapalli@outlook.com/02 Selecting and Renaming Columns in Spark Dataframe/Creating Spark Dataframe 2023-07-11 20:32:19"

# COMMAND ----------

users_df.createOrReplaceTempView('users')

# COMMAND ----------

spark.sql("""  
        select id, (amount_paid + 25) as amount_paid
        from users  
""").show()

# COMMAND ----------

 users_df.selectExpr('id', '(amount_paid + 25) as amount_paid').show()

# COMMAND ----------

users_df.select('id','amount_paid' + 25).show()

# COMMAND ----------

users_df.select('id','amount_paid' + '25').show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

'amount_paid' + lit(25)

# COMMAND ----------

type(lit(25.0))

# COMMAND ----------

users_df.dtypes

# COMMAND ----------

# amount_paid is converted to string by implicitly using lit.
# spark returns null when we perform arithmetic operations on non-compatible types

users_df.select('id', 'amount_paid' + lit(25.0), lit(50)+lit(25)).show()

# COMMAND ----------

# This works
users_df.select('id', col('amount_paid') + lit(25.0)).show()

# COMMAND ----------


