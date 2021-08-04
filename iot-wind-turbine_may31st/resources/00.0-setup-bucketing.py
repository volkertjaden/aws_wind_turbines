# Databricks notebook source
#dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"])

# COMMAND ----------

# MAGIC %run ./00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import re
# MAGIC 
# MAGIC def display_plan(sql :str, highlightExchange :bool = False, highlightPartitionFilters :bool = True) -> None:
# MAGIC   
# MAGIC   plan = str(spark.sql(f"explain extended {sql}").first()[0]).splitlines()
# MAGIC   
# MAGIC   skipping = True  # skip up to physical plan
# MAGIC   html_body = ''
# MAGIC   
# MAGIC   for line in plan:
# MAGIC     if line=='== Physical Plan ==':
# MAGIC       skipping=False
# MAGIC     if skipping: continue
# MAGIC       
# MAGIC     if html_body=='':
# MAGIC       html_body='<pre>'
# MAGIC     else:
# MAGIC       html_body += "<br>"
# MAGIC       
# MAGIC     if highlightExchange:
# MAGIC       line = re.sub(r"(\+\- (?:Broadcast)?Exchange)", r"<font color=red>\1</font>", line)
# MAGIC     if highlightPartitionFilters:
# MAGIC       line = re.sub(r" (PartitionFilters): ", r" <font color=red>\1</font>: ", line)
# MAGIC 
# MAGIC     # insert a new line every x characters to avoid horizontal scrolling
# MAGIC     # todo: change to textwrap.wrap(text, width, break_long_words=False)
# MAGIC     line = re.sub(r"(.{120})", r"\1<BR>", line, 0, re.DOTALL)
# MAGIC 
# MAGIC     html_body += line
# MAGIC   
# MAGIC   displayHTML(html_body + "</pre>")

# COMMAND ----------

reset_all = dbutils.widgets.get("reset_all_data") == "true"
tables = ["turbine_silver_bucket", "turbine_silver_partitioned", "turbine_status_gold_bucket"]
if reset_all:
  print("resetting data")
  for table in tables:
    spark.sql("""drop table if exists {}.{}""".format(dbName, table))
else:
  print("loaded without data reset")
