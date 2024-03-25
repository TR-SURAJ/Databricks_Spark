# Databricks notebook source
order_items = spark.read.json('/public/retail_db_json/order_items')

# COMMAND ----------

order_items.show()

# COMMAND ----------

order_items_grouped = order_items.groupBy('order_item_order_id')

# COMMAND ----------

type(order_items_grouped)

# COMMAND ----------

order_items_grouped. \
    count(). \
    show()


# COMMAND ----------

order_items_grouped. \
    count(). \
    withColumnRenamed('count','order_count').\
    show()


# COMMAND ----------

# Get sum of all numeric fields

order_items_grouped. \
    sum(). \
    show()

# COMMAND ----------

orders = spark.read.json('/public/retail_db_json/orders')

# COMMAND ----------

orders.printSchema()

# COMMAND ----------

orders.\
    groupBy('order_date'). \
    sum(). \
    show()

# COMMAND ----------

order_items_grouped = order_items. \
    select('order_item_order_id','order_item_quantity','order_item_subtotal'). \
    groupBy('order_item_order_id')

# COMMAND ----------

# Gets sum on order_item_order_id as well. It is not reevant and better to discard aggregation on key fields such as order_item_order_id

order_items_grouped. \
    sum(). \
    show()

# COMMAND ----------

# Gets sum on order_item_order_id as well. It is not reevant and better to discard aggregation on key fields such as order_item_order_id

order_items_grouped. \
    sum('order_item_quantity','order_item_subtotal'). \
    show()

# COMMAND ----------

# assign proper col names for aggregated cols
order_items_grouped. \
    sum('order_item_quantity','order_item_subtotal'). \
    toDF('order_item_order_id','order_quantity','order_revenue').\
    show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# We can specify custom names to derived fields using toDF. withColumn can be used to apply functions such as round on aggregated results

# assign proper col names for aggregated cols
order_items_grouped. \
    sum('order_item_quantity','order_item_subtotal'). \
    toDF('order_item_order_id','order_quantity','order_revenue'). \
    withColumn('order_revenue', round('order_revenue', 2)).\
    show()

# COMMAND ----------


