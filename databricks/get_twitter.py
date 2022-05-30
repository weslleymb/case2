# Databricks notebook source
# DBTITLE 1,Instalacao biblioteca Twitter
pip install tweepy

# COMMAND ----------

# DBTITLE 1,Importacao - UDF
# MAGIC %run "./funcoes"

# COMMAND ----------

# DBTITLE 1,Variaveis
dbutils.widgets.text("input", "","")
param = dbutils.widgets.get("input")

raw_path_storage = "dbfs:/mnt/raw/"

raw_path_recurso = raw_path_storage + param

consumer_key = dbutils.secrets.get(scope = "dbw-kv-scope", key = "twitter-api-key")

consumer_secret = dbutils.secrets.get(scope = "dbw-kv-scope", key = "twitter-api-secret")

access_token = dbutils.secrets.get(scope = "dbw-kv-scope", key = "twitter-access-token")

access_token_secret = dbutils.secrets.get(scope = "dbw-kv-scope", key = "twitter-access-token-secret")

bronze_tweets = "dbfs:/mnt/bronze/tweets"

# COMMAND ----------

# DBTITLE 1,Importacao bibliotecas
import tweepy
import pandas as pd
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Gera base de "linhas" para pesquisa 
df = spark.read.csv(raw_path_recurso, header='true')

df_pesquisa = df.select("LINHA").distinct()

pd_pesquisa = df_pesquisa.toPandas()

# COMMAND ----------

# DBTITLE 1,Extracao tweets
autorizar = tweepy.OAuthHandler(consumer_key, consumer_secret)

autorizar.set_access_token(access_token, access_token_secret)

api = tweepy.API(autorizar)

pd_tweets = pd.DataFrame(columns = ["id", "usuario", "mensagem", "data", "palavra_chave", "arquivo_origem"])

for indice, linha in pd_pesquisa.iterrows():
  
  query = 'Boticário ' + linha['LINHA']

  resultados = api.search_tweets(q=query, lang='pt-br', count=50)

  for tweet in resultados:
    
    pd_tweets = pd_tweets.append({'id':tweet.id, 'usuario':tweet.user.name, 'mensagem':tweet.text, 'data':tweet.created_at, 'palavra_chave':linha['LINHA'], 'arquivo_origem':param}, ignore_index=True)
    
df_tweets = spark.createDataFrame(pd_tweets)

# COMMAND ----------

#df_tweets.groupBy("palavra_chave", "arquivo_origem").agg(count("*")).display()

# COMMAND ----------

# DBTITLE 1,Armazenamento
if arquivo_existe(raw_path_recurso):
  df_tweets\
    .write\
    .format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .save(bronze_tweets)
else:
  df_tweets\
    .write\
    .format("delta")\
    .mode("append")\
    .option("overwriteSchema", "true")\
    .save(bronze_tweets)
