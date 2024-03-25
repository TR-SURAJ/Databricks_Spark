# Databricks notebook source
dbutils.fs.ls('/public/retail_db')

# COMMAND ----------

schema = """
    order_id INT,
    order_date TIMESTAMP,
    order_customer_id INT,
    order_status STRING
   """

# COMMAND ----------

orders = spark.read.csv('/public/retail_db/orders', schema=schema)

# COMMAND ----------

orders.show()

# COMMAND ----------

help(orders.write.csv)

# COMMAND ----------

help(orders.write.option)

# COMMAND ----------

import getpass
username = getpass.getuser()

input_dir = '/public/retail_db'
output_dir = f'/user/{username}/retail_db_pipe'

# COMMAND ----------

orders.\
    coalesce(1).\
    write.\
    mode('overwrite').\
    options(sep = '|', header = True, compression = 'gzip').\
    csv(f'{output_dir}/orders')

# COMMAND ----------

orders.\
    coalesce(1).\
    write.\
    mode('overwrite').\
    csv(f'{output_dir}/orders', sep = '|', header = True, compression = 'gzip')

# COMMAND ----------

dbutils.fs.ls(f'{output_dir}/orders')

# COMMAND ----------

spark.read.csv(f'{output_dir}/orders', sep = '|', header = True, inferSchema=True).show()

# COMMAND ----------

orders. \
    coalesce(1).\
    write. \
    mode('overwrite'). \
    option('compression', 'gzip').\
    option('header', True).\
    option('sep', '|').\
    csv(f'{output_dir}/orders')

# COMMAND ----------

dbutils.fs.ls(f'{output_dir}/orders')

# COMMAND ----------

spark.read.csv(f'{output_dir}/orders', sep = '|', header = True, inferSchema=True).show()

# COMMAND ----------

orders.\
    coalesce(1). \
    write.\
    csv(f'{output_dir}/orders', mode = 'overwrite', sep = '|', header = True, compression = 'gzip')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


