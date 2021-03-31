# Databricks notebook source
from pyspark.sql.functions import rand, input_file_name, from_json, col
from pyspark.sql.types import *

from pyspark.ml.feature import StringIndexer, StandardScaler, VectorAssembler
from pyspark.ml import Pipeline

#ML import
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.mllib.evaluation import MulticlassMetrics
from mlflow.utils.file_utils import TempDir
import mlflow.spark
import mlflow
import seaborn as sn
import pandas as pd
import matplotlib.pyplot as plt

from time import sleep
import re


# COMMAND ----------

import re

userName = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
dbName = re.sub(r'\W+', '_', userName)
path = "/Users/{}/demo/turbine".format(userName)
dbutils.widgets.text("path", path, "path")
dbutils.widgets.text("dbName", dbName, "dbName")
print("using path {}".format(path))
spark.sql("""create database if not exists {}""".format(dbName))
spark.sql("""USE {}""".format(dbName))


# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS gold_ml
USING delta
LOCATION '/mnt/oetrta/volker/turbine/gold_ml/'
""")
