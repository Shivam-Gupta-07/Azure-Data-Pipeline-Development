# Databricks notebook source
filename = dbutils.widgets.get('filename')
print(filename)

# COMMAND ----------

alreadyMounted = False
for x in dbutils.fs.mounts():
    if x.mountPoint == '/mnt/sales':
        alreadyMounted = True
        break
    else:
        alreadyMounted = False
print(alreadyMounted)

# COMMAND ----------

databricks_Secret_Scope = 'databricks_Secret_Scope'

# COMMAND ----------

storage_Access_Key = dbutils.secrets.get(scope=databricks_Secret_Scope, key="storage-Access-Key") 

# COMMAND ----------

if not alreadyMounted:
    dbutils.fs.mount(
        source="wasbs://sales@prodsalessa.blob.core.windows.net",
        mount_point="/mnt/sales",
        extra_configs={"fs.azure.account.key.prodsalessa.blob.core.windows.net": storage_Access_Key})
    print("Mounting done successfully")
else:
    print("Mounting already done")

# COMMAND ----------

# MAGIC %md
# MAGIC ###For Orders

# COMMAND ----------

orders_Df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f"/mnt/sales/landing/{filename}")

#orders_Df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/sales/landing")

# COMMAND ----------

display(orders_Df)

# COMMAND ----------

orders_Df.createOrReplaceTempView("orders")

# COMMAND ----------

error_Flag = False

order_Count = orders_Df.count()
print(order_Count)

distinct_Order_Count = orders_Df.select("order_id").distinct().count()
print(distinct_Order_Count)

# COMMAND ----------

if order_Count != distinct_Order_Count:
    error_Flag = True
if error_Flag:
    dbutils.fs.mv(f"/mnt/sales/landing/{filename}", f"/mnt/sales/discarded")
    #dbutils.fs.mv("/mnt/sales/landing/mDTSOo47TkCgo6wfIYMF_orders.csv","/mnt/sales/discarded")
    dbutils.notebook.exit('{"error_flag":"false","error_message":"order_id is not unique"}')

# COMMAND ----------

dbServer = 'prod-sales-server'
dbPort = '1433'
dbName = 'prod_sales_db'
dbUser = 'prod_sales'
dbPassword = 'sql-password'
databricksScope = 'databricks_Secret_Scope'

# COMMAND ----------

connection_Url = f"jdbc:sqlserver://{dbServer}.database.windows.net:{dbPort};database={dbName};user={dbUser};"

dbPassword = dbutils.secrets.get(scope=databricksScope, key=dbPassword)

connection_Properties = {"password": dbPassword,"driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"} 

# COMMAND ----------

valid_Order_Status_Df = spark.read.jdbc(url=connection_Url, table="valid_order_status", properties=connection_Properties)

# COMMAND ----------

display(valid_Order_Status_Df)

# COMMAND ----------

valid_Order_Status_Df.createOrReplaceTempView("valid_Order_Status")

# COMMAND ----------

invalid_status_df = spark.sql("SELECT * FROM orders WHERE order_status NOT IN (SELECT order_status FROM valid_Order_Status)")

# COMMAND ----------

display(invalid_status_df)

# COMMAND ----------

if invalid_status_df.count() > 0:
    error_Flag = True
if error_Flag:
    dbutils.fs.mv(f"/mnt/sales/landing/{filename}", f"/mnt/sales/discarded")
    dbutils.notebook.exit('{"error_flag":"false","error_message":"Invalid order status"}')
else:
    dbutils.fs.mv(f"/mnt/sales/landing/{filename}", f"/mnt/sales/staging") 
    #dbutils.fs.mv(f"/mnt/sales/landing/mDTSOo47TkCgo6wfIYMF_orders.csv","/mnt/sales/staging")    
   


# COMMAND ----------

staging_Orders_Df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f"/mnt/sales/staging/{filename}")

# COMMAND ----------

display(staging_Orders_Df)

# COMMAND ----------

staging_Orders_Df.createOrReplaceTempView("orders_staging")

# COMMAND ----------

# MAGIC %md
# MAGIC ###For Order_Items 

# COMMAND ----------

order_Items_Df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/sales/order_items/order_items.csv")

# COMMAND ----------

display(order_Items_Df )

# COMMAND ----------

order_Items_Df.createOrReplaceTempView("order_items")

# COMMAND ----------

# MAGIC %md
# MAGIC ##**Customers**

# COMMAND ----------

customers_Df = spark.read.jdbc(url=connection_Url, table="customers", properties=connection_Properties)

# COMMAND ----------

display(customers_Df)

# COMMAND ----------

customers_Df.createOrReplaceTempView("customers")

# COMMAND ----------

# MAGIC %md
# MAGIC how much orders are placed by each customer and how much amount is spent by each customer

# COMMAND ----------

result_df = spark.sql("SELECT c.customer_id,c.customer_fname,c.customer_lname,c.customer_street,c.customer_city,c.customer_state,c.customer_zipcode,count(order_id) as num_orders_placed,round(sum(order_item_subtotal),2) as total_amount from customers c join orders_staging o on o.customer_id=c.customer_id join order_items oi on o.order_id = oi.order_item_order_id group by c.customer_id,c.customer_fname,c.customer_lname,c.customer_street,c.customer_city,c.customer_state,c.customer_zipcode order by total_amount desc ")

# COMMAND ----------

display(result_df)

# COMMAND ----------

result_df.write.jdbc(url=connection_Url, table="sales_reporting", mode="overwrite", properties=connection_Properties)