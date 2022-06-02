# Databricks notebook source
#https://docs.microsoft.com/en-us/azure/synapse-analytics/machine-learning/tutorial-text-analytics-use-mmlspark
#https://microsoft.github.io/SynapseML/docs/next/getting_started/installation/

import pyspark
spark = pyspark.sql.SparkSession.builder.appName("busca_twitter") \
    .config("spark.jars.packages", "com.microsoft.azure:synapseml_2.12:0.9.5") \
    .config("spark.jars.repositories", "https://mmlspark.azureedge.net/maven") \
    .getOrCreate()
import synapse.ml

import synapse.ml
from synapse.ml.cognitive import *
from pyspark.sql.functions import col

#trocado .setLinkedService(linked_service_name)
#por .setSubscriptionKey(service_key)

#trocado cognitive_service_name = "<Your linked service for text analytics>"
#por...
service_key = "c2a2445a9d164941842da4b6dff1be12"

# Create a dataframe that's tied to it's column names
df = spark.createDataFrame([
  ("1", "I am so happy today, its sunny!", "en-US"),
  ("2", "I am frustrated by this rush hour traffic", "en-US"),
  ("3", "The cognitive services on spark aint bad", "en-US"),
], ["id", "text", "language"])

sentiment = (TextSentiment()
    .setSubscriptionKey(service_key)
    .setTextCol("text")
    .setOutputCol("sentiment")
    .setErrorCol("error")
    .setLanguageCol("language")
    .setUrl("https://eastus.api.cognitive.microsoft.com/text/analytics/v3.0/sentiment"))

# Show the results of your text query in a table format
#display(sentiment.transform(df).select("id", "text", col("sentiment")[0].getItem("sentiment").alias("sentiment")))

df_result = sentiment.transform(df).select("id", "text", col("sentiment")[0].getItem("sentiment").alias("sentiment"))

df_result.display()
