# Databricks notebook source
# DBTITLE 1,Mount Raw
try:
  dbutils.fs.mount(
    source = "wasbs://raw@dlscase2deveastus.blob.core.windows.net",
    mount_point = "/mnt/raw",
    extra_configs = {"fs.azure.account.key.dlscase2deveastus.blob.core.windows.net":dbutils.secrets.get(scope = "dbw-kv-scope", key = "storageaccount-key")})
except:
  pass

# COMMAND ----------

# DBTITLE 1,Mount Bronze
try:
  dbutils.fs.mount(
    source = "wasbs://bronze@dlscase2deveastus.blob.core.windows.net",
    mount_point = "/mnt/bronze",
    extra_configs = {"fs.azure.account.key.dlscase2deveastus.blob.core.windows.net":dbutils.secrets.get(scope = "dbw-kv-scope", key = "storageaccount-key")})
except:
  pass

# COMMAND ----------

# DBTITLE 1,Mount Bronze
try:
  dbutils.fs.mount(
    source = "wasbs://silver@dlscase2deveastus.blob.core.windows.net",
    mount_point = "/mnt/silver",
    extra_configs = {"fs.azure.account.key.dlscase2deveastus.blob.core.windows.net":dbutils.secrets.get(scope = "dbw-kv-scope", key = "storageaccount-key")})
except:
  pass
