# Databricks notebook source
order_items = spark.read.json('/public/retail_db_json/order_items')

# COMMAND ----------

order_items.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC - Get revenue using order_item_subtotal for a given order_item_order_id (eg: for order_item_order_id 2)

# COMMAND ----------

# narrow transformation
order_items.filter('order_item_order_id = 2')

# COMMAND ----------

order_items.filter('order_item_order_id = 2').show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------


help(sum)

# COMMAND ----------

order_items.filter('order_item_order_id = 2').select(sum('order_item_subtotal')).show()

# COMMAND ----------

order_items.filter('order_item_order_id = 2').select(sum('order_item_subtotal').alias('order_revenue')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - Get number of items. total quantity as well as revenue for a given order item order id (ex: 2)
# MAGIC   - Number of items can be computed using count on order_item_quantity
# MAGIC   - Total quantity can be computed using sum on order_item_quantity
# MAGIC   - Total revenue can be computed using sum on order_item_subtotal

# COMMAND ----------

order_items. \
    filter('order_item_order_id = 2'). \
    select(
        count('order_item_quantity').alias('order_item_count'),
        count('order_item_quantity').alias('order_quanityt'),
        count('order_item_subtotal').alias('order_revenue')
    ). \
    show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


