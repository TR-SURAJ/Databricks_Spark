# Databricks notebook source
# MAGIC %fs ls /public/retail_db

# COMMAND ----------

schema = """ 
    order_id INT,
    order_date TIMESTAMP,
    order_customer_id INT,
    order_status STRING
"""

# COMMAND ----------

orders = spark.read.schema(schema).csv('/public/retail_db/orders')

# COMMAND ----------

orders.show()

# COMMAND ----------

orders.printSchema()

# COMMAND ----------

orders_json = spark.read.json('/public/retail_db_json/orders')

# COMMAND ----------

orders_json.show()

# COMMAND ----------

orders_json.printSchema()

# COMMAND ----------

import getpass
username = getpass.getuser()

# COMMAND ----------

input_dir = '/public/retail_db_json'
output_dir = f'/user/{username}/retail_db_parquet'

# COMMAND ----------

dbutils.fs.ls(input_dir)

# COMMAND ----------

for file_details in dbutils.fs.ls(input_dir):
    print(file_details.path)

# COMMAND ----------

for file_details in dbutils.fs.ls(input_dir):
    if not ('.git' in file_details.path or file_details.path.endswith('sql')):
        print(f'Converting data in {file_details.path} folder from json to parquet')
        data_set_dir = file_details.path.split('/')[-2]
        df = spark.read.json(file_details.path)
        df.coalesce(1).write.parquet(f'{output_dir}/{data_set_dir}', mode = 'overwrite')

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/retail_db_parquet')

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/retail_db_parquet/orders')

# COMMAND ----------

orders = spark.read.parquet(f'/user/{username}/retail_db_parquet/orders')

# COMMAND ----------

orders.show()

# COMMAND ----------

orders.printSchema()

# COMMAND ----------


