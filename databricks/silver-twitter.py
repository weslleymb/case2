# Databricks notebook source
# DBTITLE 1,Bibliotecas
from pyspark.sql.functions import *
from pyspark.sql.window import *
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Variaveis
bronze_path_tweets = "dbfs:/mnt/bronze/tweets"

silver_path_tweets = "dbfs:/mnt/silver/tweets"

# COMMAND ----------

# DBTITLE 1,Funcoes UDF
# MAGIC %run "./funcoes"

# COMMAND ----------

# DBTITLE 1,Checagem arquivo origem
if arquivo_existe(bronze_path_tweets) == False:
  dbutils.notebook.exit('stop')
else:
  df_bronze_tweets = spark.read.format("delta").load(bronze_path_tweets)
  if (df_bronze_tweets.count()==0): 
    dbutils.notebook.exit('stop')

# COMMAND ----------

# DBTITLE 1,Dedup
particao_dedup = Window.partitionBy("id", "usuario", "mensagem", "data", "marca", "linha").orderBy(col("data_carga").cast("date").desc())

df_process_tweets_dedup = df_bronze_tweets\
  .withColumn("chave", concat(col("marca"), lit(" - "), col("linha")))\
  .withColumn("msg", regexp_replace(col("mensagem"), "[\n\r]", " "))\
  .withColumn(
    "dedup", 
    row_number()
    .over(particao_dedup)
  )\
  .filter("dedup = 1")\
  .select("chave", "id", "usuario", "msg", col("data").cast("date").alias("dt_tweet"), "data_carga")

#df_process_tweets_dedup.display()

# COMMAND ----------

# DBTITLE 1,Armazenamento
if arquivo_existe(silver_path_tweets) == False:
  df_process_tweets_dedup\
    .write\
    .format("delta")\
    .mode("overwrite")\
    .save(silver_path_tweets)
else:
  df_destino = DeltaTable.forPath(spark, silver_path_tweets)
  df_destino.alias("destino")\
    .merge(
      df_process_tweets_dedup.alias("origem"), """
        destino.chave = origem.chave 
        and destino.id = origem.id 
        and destino.usuario = origem.usuario
        and destino.msg = origem.msg
        and destino.dt_tweet = origem.dt_tweet
      """
    )\
    .whenNotMatchedInsert(values = {
      "chave": "origem.chave",
      "id": "origem.id",
      "usuario": "origem.usuario", 
      "msg": "origem.msg", 
      "dt_tweet": "origem.dt_tweet",
      "data_carga": "origem.data_carga"
    })\
    .execute()
