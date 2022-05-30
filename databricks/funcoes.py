# Databricks notebook source
# DBTITLE 1,Check existencia recurso
def arquivo_existe(recurso):
  try:
    dbutils.fs.ls(recurso)
    return True
  except:
    return False
