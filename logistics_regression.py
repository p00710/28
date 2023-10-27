import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkFiles
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.feature import VectorAssembler


spark = pyspark.sql.SparkSession.builder.appName("ex1").getOrCreate()
data = spark.read.csv("data.csv", header=True, inferSchema=True)
print("Data: ")
data.show()

# Rename the columns for better readability
columns = ['id', 'diagnosis'] + [f'feature_{i}' for i in range(1, 32)]
data = data.toDF(*columns)

# Map 'M' (malignant) to 1 and 'B' (benign) to 0
data = data.withColumn(
    "label", (data["diagnosis"] == "M").cast("integer")).drop("diagnosis")

feature_columns = [f'feature_{i}' for i in range(1, 25)]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

data = assembler.transform(data)

train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

# building logistic model
print("building logistic model: ")
logistic_regression = LogisticRegression(
    featuresCol="features", labelCol="label")
model = logistic_regression.fit(train_data)

coefficients = model.coefficients
intercept = model.intercept

print('Coefficients: ', coefficients)
print('Intercept: {:.3f}'.format(intercept))

predictions = model.transform(test_data)

# AUC-ROC
evaluator = BinaryClassificationEvaluator(
    rawPredictionCol="rawPrediction", labelCol="label")
auc = evaluator.evaluate(predictions)

# Accuracy, Precision, and Recall
multi_evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction")
accuracy = multi_evaluator.evaluate(
    predictions, {multi_evaluator.metricName: "accuracy"})
precision = multi_evaluator.evaluate(
    predictions, {multi_evaluator.metricName: "weightedPrecision"})
recall = multi_evaluator.evaluate(
    predictions, {multi_evaluator.metricName: "weightedRecall"})

print(f"AUC-ROC: {auc:.4f}")
print(f"Accuracy: {accuracy:.4f}")
print(f"Precision: {precision:.4f}")
print(f"Recall: {recall:.4f}")
