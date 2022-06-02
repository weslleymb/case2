# Databricks notebook source
# DBTITLE 1,Bibliotecas
import pyspark
spark = pyspark.sql.SparkSession.builder.appName("busca_twitter") \
    .config("spark.jars.packages", "com.microsoft.azure:synapseml_2.12:0.9.5") \
    .config("spark.jars.repositories", "https://mmlspark.azureedge.net/maven") \
    .getOrCreate()
import synapse.ml
import synapse.ml
from synapse.ml.cognitive import *
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Variaveis
silver_path_tweets = "dbfs:/mnt/silver/tweets"

service_key = dbutils.secrets.get(scope = "dbw-kv-scope", key = "key-cog")

gold_path_tweets = "dbfs:/mnt/goldcase2/tweets"

# COMMAND ----------

# DBTITLE 1,Funcoes UDF
# MAGIC %run "./funcoes"

# COMMAND ----------

# DBTITLE 1,Checagem arquivo origem
if arquivo_existe(silver_path_tweets) == False:
  dbutils.notebook.exit('stop')
else:
  df_silver_tweets = spark.read.format("delta").load(silver_path_tweets)
  if (df_silver_tweets.count()==0): 
    dbutils.notebook.exit('stop')

# COMMAND ----------

# DBTITLE 1,Novos Tweets
if arquivo_existe(gold_path_tweets):
  df_gold_tweets = spark.read.format("delta").load(gold_path_tweets)
  df_process_tweets = df_silver_tweets.join(df_gold_tweets, on=[df_silver_tweets.id == df_gold_tweets.id], how='leftanti')
else:
  df_process_tweets = df_silver_tweets

# COMMAND ----------

df_process_busca_tweets = df_process_tweets.withColumn("linguagem", lit("pt-BR"))

sentiment = (TextSentiment()
    .setSubscriptionKey(service_key)
    .setTextCol("msg")
    .setOutputCol("sentiment")
    .setErrorCol("error")
    .setLanguageCol("linguagem")
    .setUrl("https://eastus.api.cognitive.microsoft.com/text/analytics/v3.0/sentiment"))

df_result = sentiment.transform(df_process_busca_tweets).select("chave", "id", "usuario", "msg", col("sentiment")[0].getItem("sentiment").alias("sentiment"), "dt_tweet", "data_carga")

# COMMAND ----------

# DBTITLE 1,Armazenamento
if arquivo_existe(gold_path_tweets) == False:
  df_result\
    .write\
    .format("delta")\
    .mode("append")\
    .save(gold_path_tweets)
else:
  df_destino = DeltaTable.forPath(spark, gold_path_tweets)
  df_destino.alias("destino")\
    .merge(
      df_result.alias("origem"), """
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
      "sentiment": "origem.sentiment", 
      "dt_tweet": "origem.dt_tweet",
      "data_carga": "origem.data_carga"
    })\
    .execute()
