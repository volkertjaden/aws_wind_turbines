# Databricks notebook source
import re

userName = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
dbName = re.sub(r'\W+', '_', userName)
path = "/Users/{}/demo/turbine".format(userName)
dbutils.widgets.text("path", path, "path")
dbutils.widgets.text("dbName", dbName, "dbName")
print("using path {}".format(path))
spark.sql("""create database if not exists {}""".format(dbName))
spark.sql("""USE {}""".format(dbName))
