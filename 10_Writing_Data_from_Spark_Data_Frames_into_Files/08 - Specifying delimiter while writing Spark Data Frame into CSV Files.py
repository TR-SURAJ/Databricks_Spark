# Databricks notebook source
dbutils.fs.ls('/public/retail_db')

# COMMAND ----------

df = spark.read.csv('/public/retail_db/orders/')

# COMMAND ----------

df.show()

# COMMAND ----------

import getpass
username = getpass.getuser()

# COMMAND ----------

input_dir = '/public/retail_db'
output_dir = f'/user/{username}/retail_db_pipe'

# COMMAND ----------

df.coalesce(1).write.mode('overwrite').csv(f'{output_dir}/orders', sep = '|')

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/retail_db_pipe/orders')

# COMMAND ----------

spark.read.csv(f'/user/{username}/retail_db_pipe/orders').show(truncate=False)

# COMMAND ----------

spark.read.csv(f'/user/{username}/retail_db_pipe/orders', sep = '|').show(truncate=False)

# COMMAND ----------

dbutils.fs.ls(input_dir)

# COMMAND ----------

for file_details in dbutils.fs.ls(input_dir):
    print(f'Converting data in {file_details.path} folder from comma seperated to pipe delimiter')
    df = spark.read.csv(file_details.path)
    folder_name = file_details.path.split('/')[-2]
    df.coalesce(1).write.mode('overwrite').csv(f'{output_dir}/{folder_name}', sep = '|')

# COMMAND ----------

schema = """
    order_id INT,
    order_date TIMESTAMP,
    order_customer_id INT,
    order_status STRING
  """

# COMMAND ----------

orders = spark.read.schema(schema).csv(f'/user/{username}/retail_db_pipe/orders')

# COMMAND ----------

orders.show()

# COMMAND ----------

orders = spark.read.schema(schema).csv(f'/user/{username}/retail_db_pipe/orders', sep = '|')

# COMMAND ----------

orders.show()

# COMMAND ----------


