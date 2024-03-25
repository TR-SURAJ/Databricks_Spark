# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC - We can pass the options using different ways while creating the Data Frame
# MAGIC   - Using key word arguments as part of APIs. We can use key word arguments as part of load as well as direct API
# MAGIC   - spark.read.option
# MAGIC   - spark.read.options
# MAGIC   - If key in the option is incorrect then the options will be ignored
# MAGIC
# MAGIC - Depending up on the API based on the file format the options as well as arguments vary

# COMMAND ----------

import getpass
username = getpass.getuser()

# COMMAND ----------

orders = spark.read.csv(f'/user/{username}/retail_db_pipe/orders')

# COMMAND ----------

orders.show(truncate=False)

# COMMAND ----------

orders = spark.read.csv(f'/user/{username}/retail_db_pipe/orders', sep = '|', header = None, inferSchema = True).\
         toDF('order_id','order_date','order_customer_id','order_status')

# COMMAND ----------

orders.show()

# COMMAND ----------

help(spark.read.format('csv').load)

# COMMAND ----------

help(spark.read.option)

# COMMAND ----------

orders = spark. \
        read.\
        option('sep','|').\
        option('header', None).\
        option('inferSchema', True). \
        csv(f'/user/{username}/retail_db_pipe/orders').\
        toDF('order_id','order_date','order_customer_id','order_status')

# COMMAND ----------

orders.show()

# COMMAND ----------

orders = spark.read.\
         options(sep = '|', header = None, inferSchema=True).\
         csv(f'/user/{username}/retail_db_pipe/orders').\
         toDF('order_id','order_date','order_customer_id','order_status')

# COMMAND ----------

orders.show()

# COMMAND ----------

options = {
    'sep': '|',
    'header': None,
    'inferSchema': True
}

# COMMAND ----------

orders = spark.\
        read.\
        options(**options).\
        csv(f'/user/{username}/retail_db_pipe/orders').\
        toDF('order_id','order_date','order_customer_id','order_status')

# COMMAND ----------

orders.show()

# COMMAND ----------


