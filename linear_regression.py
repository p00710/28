from pyspark.ml.feature import VectorAssembler
import pyspark
from pyspark.ml.regression import LinearRegression
# based on data we need to evaluate intercept,coefficient, accuracy and errors
from pyspark.ml.evaluation import RegressionEvaluator


spark = pyspark.sql.SparkSession.builder.appName(
    "Linear Regression").getOrCreate()
data = spark.read.csv("data_price.csv", header=True, inferSchema=True)
print("---Print the CSV File----")
data.show()
print("----Structure of the data---")
data.printSchema()
# print("----Print the columns of the csv file in the form of list----")
# data.columns
# vectors(VectorAssembler)=joining two columns(independent var) into one and creating new feature
print("")
assembler = VectorAssembler(
    inputCols=["age", "Exper"], outputCol="Independent")
output = assembler.transform(data)
print("----Structure of the output variable---")
output.show()
# print("----Print the columns of the output var in the form of list----")
# output.columns


final_data = output.select('Independent', 'Salary')
print("----Print the independent and dependent columns----")
final_data.show()


#
train_data, test_data = final_data.randomSplit([0.7, 0.3])
regressor = LinearRegression(featuresCol="Independent", labelCol="Salary")
regressor = regressor.fit(train_data)
regressor.coefficients
regressor.intercept
print("---prediction of salary with actual and expected----")
pred_result = regressor.evaluate(test_data)
pred_result.predictions.show()
