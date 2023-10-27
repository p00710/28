# naive bayes clasification
# loading the liberaries
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from sklearn.metrics import confusion_matrix
from sklearn.datasets import load_iris
import pandas as pd
# preparing the data
iris = load_iris()
df_iris = pd.DataFrame(iris.data, columns=iris.feature_names)
print(df_iris.head)
# target into series
df_iris['label'] = pd.Series(iris.target)
print(df_iris.head)
# define sql context and create the dataframe
sc = SparkContext().getOrCreate()
sqlcontext = SQLContext(sc)
data = sqlcontext.createDataFrame(df_iris)
print(data.printSchema())
# combine all the features of data and seperate label while using vectorassembler
features = iris.feature_names
va = VectorAssembler(inputCols=features, outputCol='features')
va_df = va.transform(data)
va_df.select('features', 'label')
va_df.show(3)
# split data into training and testing[90:10]
(train, test) = va_df.randomSplit([0.9, 0.1])

# prediction and accuracy
# we will define decision tree classifier by using naive bayes class and fit the model on train data
nb = NaiveBayes(smoothing=1.0, modelType='multinomial')
nb = nb.fit(train)
pred = nb.transform(test)
pred.show(3)
# predict the test data
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
acc = evaluator.evaluate(pred)
print("Prediction accuracy ", acc)
y_pred = pred.select("prediction").collect()
y_orig = pred.select("label").collect()

cn = confusion_matrix(y_orig, y_pred)
print(cn)
sc.stop()
