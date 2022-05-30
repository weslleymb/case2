# Databricks notebook source
pip install tweepy

# COMMAND ----------

dbutils.widgets.text("input", "","")
param = dbutils.widgets.get("input")

# COMMAND ----------

import tweepy

consumer_key = dbutils.secrets.get(scope = "dbw-kv-scope", key = "twitter-api-key")
consumer_secret = dbutils.secrets.get(scope = "dbw-kv-scope", key = "twitter-api-secret")
access_token = dbutils.secrets.get(scope = "dbw-kv-scope", key = "twitter-access-token")
access_token_secret = dbutils.secrets.get(scope = "dbw-kv-scope", key = "twitter-access-token-secret")

autorizar = tweepy.OAuthHandler(consumer_key, consumer_secret)
autorizar.set_access_token(access_token, access_token_secret)

api = tweepy.API(autorizar)

query = 'Boticário ' + param

resultados = api.search_tweets(q=query, lang='pt-br', count=50)

recentes = api.home_timeline(count=5)

# COMMAND ----------

import pandas as pd

df = pd.DataFrame(columns = ["id", "usuario", "mensagem", "data"])

for tweet in resultados:
    df = df.append({'id':tweet.id, 'usuario':tweet.user.name, 'mensagem':tweet.text, 'data':tweet.created_at}, ignore_index=True)

df

# COMMAND ----------


