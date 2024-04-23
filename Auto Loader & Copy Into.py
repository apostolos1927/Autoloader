# Databricks notebook source
rawDataSource = "/FileStore/data*"

# COMMAND ----------

(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("header", "true")
.option("cloudFiles.inferColumnTypes", "true")
.option("cloudFiles.schemaEvolutionMode","addNewColumns")
#.option("cloudFiles.schemaEvolutionMode", "rescue") 
.option("cloudFiles.schemaLocation", "/FileStore/schemalocexample")
.load(rawDataSource)
.writeStream
.format("delta")
#.trigger(once=True) #batch mode
.option("checkpointLocation", "/FileStore/checkpointexample")
.option("mergeSchema", "true")
.toTable("demo")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS my_demo;
# MAGIC
# MAGIC COPY INTO my_demo
# MAGIC FROM '/FileStore/data5.csv'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('header' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');                
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM my_demo
