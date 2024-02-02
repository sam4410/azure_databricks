# Databricks notebook source
print("Hello World")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Magic command for adding markdown text and mixing language within a notebook

# COMMAND ----------

# MAGIC %md
# MAGIC # This is a header
# MAGIC
# MAGIC ### This is a subheader

# COMMAND ----------

# MAGIC %scala
# MAGIC println("Hello World")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select current_date()

# COMMAND ----------

# MAGIC %r
# MAGIC myVar = c('Hello','my','friend!','how', 'are','you')
# MAGIC print(myVar[2])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Utilities Module and Filestore Utilities

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/tables/')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 'dbfs:/FileStore/tables'

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 'dbfs:/FileStore/tables/raw/'

# COMMAND ----------

