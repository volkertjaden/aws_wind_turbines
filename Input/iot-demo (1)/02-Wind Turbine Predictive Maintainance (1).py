# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Wind Turbine Predictive Maintenance
# MAGIC 
# MAGIC In this example, we demonstrate anomaly detection for the purposes of finding damaged wind turbines. A damaged, single, inactive wind turbine costs energy utility companies thousands of dollars per day in losses.
# MAGIC 
# MAGIC 
# MAGIC <img src="https://quentin-demo-resources.s3.eu-west-3.amazonaws.com/images/turbine/turbine_flow.png" />
# MAGIC 
# MAGIC 
# MAGIC <div style="float:right; margin: -10px 50px 0px 50px">
# MAGIC   <img src="https://s3.us-east-2.amazonaws.com/databricks-knowledge-repo-images/ML/wind_turbine/wind_small.png" width="400px" /><br/>
# MAGIC   *locations of the sensors*
# MAGIC </div>
# MAGIC Our dataset consists of vibration readings coming off sensors located in the gearboxes of wind turbines. 
# MAGIC 
# MAGIC We will use Gradient Boosted Tree Classification to predict which set of vibrations could be indicative of a failure.
# MAGIC 
# MAGIC One the model is trained, we'll use MFLow to track its performance and save it in the registry to deploy it in production
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC *Data Source Acknowledgement: This Data Source Provided By NREL*
# MAGIC 
# MAGIC *https://www.nrel.gov/docs/fy12osti/54530.pdf*

# COMMAND ----------

# MAGIC %run ./00-setup

# COMMAND ----------

dbutils.widgets.text("path", path, "path")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Use ML and MLFlow to detect damaged turbine
# MAGIC 
# MAGIC Our data is now ready. We'll now train a model to detect damaged turbines.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Data Exploration
# MAGIC What do the distributions of sensor readings look like for ourturbines? 
# MAGIC 
# MAGIC *Notice the much larger stdev in AN8, AN9 and AN10 for Damaged turbined.*

# COMMAND ----------

display(spark.read.format("delta").load("/mnt/quentin-demo-resources/turbine/status_gold"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC select distinct(id) from turbine_silver  where id > 499

# COMMAND ----------

display(turbine_damaged)

# COMMAND ----------

from pyspark.sql.functions import rand

turbine_healthy = spark.read.table("turbine_gold").filter("STATUS = 'healthy'").limit(100000)
turbine_damaged = spark.read.table("turbine_gold").filter("STATUS = 'damaged'").limit(100000)

dataset = turbine_damaged.union(turbine_healthy).orderBy(rand())
# Compare AN9 value for healthy/damaged; varies much more for damaged ones
dataset.createOrReplaceTempView("test")
display(dataset)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Model Creation: Workflows with Pyspark.ML Pipeline

# COMMAND ----------

# DBTITLE 1,Build Training and Test dataset
def prepare_data(products):
  le = LabelEncoder()
  le.fit(products["STATUS"])
  X_train, X_test, y_train, y_test = train_test_split(products["AN3", "AN4", "AN5", "AN6", "AN7", "AN8", "AN9", "AN10"], le.transform(products["STATUS"]), test_size=0.33, random_state=42)
  return X_train, X_test, y_train, y_test, le

X_train, X_test, y_train, y_test, le = prepare_data(dataset.limit(1000000))

# COMMAND ----------

from sklearn import ensemble

params = {'n_estimators': 1000, 'max_leaf_nodes': 4, 'max_depth': None, 'random_state': 42, 'min_samples_split': 5}

import mlflow.spark
import mlflow

with mlflow.start_run():
  #the source table will automatically be logged to mlflow
  mlflow.spark.autolog()
  
  X_train =   train = ["AN3", "AN4", "AN5", "AN6", "AN7", "AN8", "AN9", "AN10"]

  clf = ensemble.GradientBoostingClassifier(**params)
  clf.fit(X_train, y_train)
    
    
  gbt = GBTClassifier(labelCol="label", featuresCol="features").setMaxIter(5)
  grid = ParamGridBuilder().addGrid(gbt.maxDepth, [4, 5, 6]).build()

  ev = BinaryClassificationEvaluator()
 
  # 3-fold cross validation
  cv = CrossValidator(estimator=gbt, estimatorParamMaps=grid, evaluator=ev, numFolds=3)

  featureCols = ["AN3", "AN4", "AN5", "AN6", "AN7", "AN8", "AN9", "AN10"]
  stages = [VectorAssembler(inputCols=featureCols, outputCol="va"), StandardScaler(inputCol="va", outputCol="features"), StringIndexer(inputCol="STATUS", outputCol="label"), cv]
  pipeline = Pipeline(stages=stages)

  pipelineTrained = pipeline.fit(train)
  
  mlflow.spark.log_model(pipelineTrained, "turbine_gbt")
  mlflow.set_tag("model", "turbine_gbt")
  predictions = pipelineTrained.transform(test)
  # Prints AUROC
  AUROC = ev.evaluate(predictions)
  mlflow.log_metric("AUROC", AUROC)

# COMMAND ----------

# DBTITLE 1,Train our model using a GBT
with mlflow.start_run():
  #the source table will automatically be logged to mlflow
  #mlflow.spark.autolog()
  
  gbt = GBTClassifier(labelCol="label", featuresCol="features").setMaxIter(5)
  grid = ParamGridBuilder().addGrid(gbt.maxDepth, [3,4,5]).build()

  ev = BinaryClassificationEvaluator()
  cv = CrossValidator(estimator=gbt, estimatorParamMaps=grid, evaluator=ev, numFolds=2)

  featureCols = ["AN3", "AN4", "AN5", "AN6", "AN7", "AN8", "AN9", "AN10"]
  stages = [VectorAssembler(inputCols=featureCols, outputCol="va"), StandardScaler(inputCol="va", outputCol="features"), StringIndexer(inputCol="STATUS", outputCol="label"), cv]
  pipeline = Pipeline(stages=stages)

  pipelineTrained = pipeline.fit(train)
  
  mlflow.spark.log_model(pipelineTrained, "turbine_gbt")
  mlflow.set_tag("model", "turbine_gbt")
  predictions = pipelineTrained.transform(test)

  metrics = MulticlassMetrics(predictions.select(['prediction', 'label']).rdd)
  mlflow.log_metric("precision", metrics.precision(1.0))
  mlflow.log_metric("recall", metrics.recall(1.0))
  mlflow.log_metric("f1", metrics.fMeasure(1.0))
  AUROC = ev.evaluate(predictions)
  mlflow.log_metric("AUROC", AUROC)
  
  #Add confusion matrix to the model:
  with TempDir() as tmp_dir:
    labels = pipelineTrained.stages[2].labels
    sn.heatmap(pd.DataFrame(metrics.confusionMatrix().toArray()), annot=True, fmt='g', xticklabels=labels, yticklabels=labels)
    plt.suptitle("Turbine Damage Prediction. F1={:.2f}".format(metrics.fMeasure(1.0)), fontsize = 18)
    plt.xlabel("Predicted Labels")
    plt.ylabel("True Labels")
    plt.savefig(tmp_dir.path()+"/confusion_matrix.png")
    mlflow.log_artifact(tmp_dir.path()+"/confusion_matrix.png")

# COMMAND ----------

# MAGIC %md ## Saving our model to MLFLow registry

# COMMAND ----------

# DBTITLE 1,Save our new model to the registry as a new version
#get the best model having the best metrics.AUROC from the registry
best_models = mlflow.search_runs(filter_string='tags.model="turbine_gbt" and attributes.status = "FINISHED" and metrics.AUROC > 0', order_by=['metrics.AUROC DESC'], max_results=1)
model_uri = best_models.iloc[0].artifact_uri

model_registered = mlflow.register_model(best_models.iloc[0].artifact_uri+"/turbine_gbt", "turbine_gbt")
sleep(5)

# COMMAND ----------

# DBTITLE 1,Flag this version as production ready
client = mlflow.tracking.MlflowClient()
client.transition_model_version_stage(name = "turbine_gbt", version = model_registered.version, stage = "Production", archive_existing_versions=True) #NOTE: set archive_existing_versions=true with client version 1.10 (mlflow==1.10.0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detecting damaged turbine in a production pipeline

# COMMAND ----------

# DBTITLE 1,Load the model from our registry
model_from_registry = mlflow.spark.load_model('models:/turbine_gbt/production')

# COMMAND ----------

# DBTITLE 1,Compute predictions using our spark model:
prediction = model_from_registry.transform(dataset.limit(100))
display(prediction.select(*featureCols+['prediction']))

# COMMAND ----------

# MAGIC %md
# MAGIC ### We can now explore our prediction in a new dashboard
# MAGIC https://e2-demo-west.cloud.databricks.com/sql/dashboards/92d8ccfa-10bb-411c-b410-274b64b25520-turbine-demo-predictions?o=2556758628403379
