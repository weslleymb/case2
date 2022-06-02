# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

silver_path_vendas = "dbfs:/mnt/silver/vendas"

gold_path_tabela1 = "dbfs:/mnt/silver/tabela1_ano_mes"

gold_path_tabela2 = "dbfs:/mnt/silver/tabela2_marca_linha"

gold_path_tabela3 = "dbfs:/mnt/silver/tabela3_marca_ano_mes"

gold_path_tabela4 = "dbfs:/mnt/silver/tabela4_linha_ano_mes"

gold_path_marca_linha = "dbfs:/mnt/silver/marca_linha"

gold_path_vendas = "dbfs:/mnt/silver/vendas"

# COMMAND ----------


