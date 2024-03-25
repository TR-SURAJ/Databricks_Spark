# Databricks notebook source
# MAGIC %run "/Users/surajthallapalli@outlook.com/04 Filtering Data from Spark Dataframes/02 - Creating Spark Data Frame for filtering"

# COMMAND ----------

userdf.filter(col('is_customer') == "true").show()

# COMMAND ----------

userdf.filter(col('is_customer') == True).show()

# COMMAND ----------

userdf.filter(col('is_customer') == False).show()

# COMMAND ----------

userdf.filter('is_customer == true').show()

# COMMAND ----------

userdf.filter('is_customer = true').show()

# COMMAND ----------

userdf.createOrReplaceTempView('users')

# COMMAND ----------

spark.sql("""
        SELECT *
        FROM users
        WHERE is_customer = "true"
 """). \
     show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM users WHERE is_customer = "true"

# COMMAND ----------

userdf.filter(col('current_city') == 'Dallas' ).show()

# COMMAND ----------

userdf.filter("current_city == 'Dallas'").show()

# COMMAND ----------

userdf.filter("amount_paid == 1000.55").show()

# COMMAND ----------

userdf.filter('amount_paid == 1000.55').show()

# COMMAND ----------

userdf.filter(col('amount_paid') == 1000.55).show()

# COMMAND ----------

userdf.filter(col('amount_paid') == '1000.55').show()

# COMMAND ----------

userdf.show(truncate=False)

# COMMAND ----------

userdf.select('amount_paid', isnan('amount_paid')).show()

# COMMAND ----------

userdf.filter(isnan('amount_paid') == True).show()

# COMMAND ----------

userdf.filter(isnan('amount_paid') = True).show()

# COMMAND ----------

userdf.filter('isnan(amount_paid) = True').show()

# COMMAND ----------

userdf.filter('isnan(amount_paid) == True').show()

# COMMAND ----------


