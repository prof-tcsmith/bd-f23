from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, round
from pyspark.ml.feature import StringIndexer
from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import DecisionTreeClassifier, RandomForestClassifier, NaiveBayes
from pyspark.ml.clustering import KMeans
from pyspark.sql import Row
import pandas as pd
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("CreditCardDefault").getOrCreate()

# Load the file into an RDD and remove the header row
ccRaw = spark.read.text("/Users/ashishbansal/Downloads/credit-card-default-1000.csv")
ccRaw = ccRaw.filter(~ccRaw.value.contains("EDUCATION"))

# Remove lines that are not "CSV"
ccRaw = ccRaw.filter(~ccRaw.value.contains("aaaaaa"))

# Remove double quotes from records
ccRaw = ccRaw.withColumn("value", col("value").cast("string"))
ccRaw = ccRaw.withColumn("value", col("value").cast("double"))

# Normalize sex to 1 and 2
ccRaw = ccRaw.withColumn("SEX", (col("SEX") == "F").cast("int"))

# Calculate average billed amount
avg_bill_cols = ["BILL_AMT1", "BILL_AMT2", "BILL_AMT3", "BILL_AMT4", "BILL_AMT5", "BILL_AMT6"]
ccRaw = ccRaw.withColumn("AVG_BILL_AMT", round(sum(col(c) for c in avg_bill_cols) / 6, 2))

# Calculate average pay amount
avg_pay_cols = ["PAY_AMT1", "PAY_AMT2", "PAY_AMT3", "PAY_AMT4", "PAY_AMT5", "PAY_AMT6"]
ccRaw = ccRaw.withColumn("AVG_PAY_AMT", round(sum(col(c) for c in avg_pay_cols) / 6, 2))

# Calculate average pay duration
pay_duration_cols = ["PAY_0", "PAY_2", "PAY_3", "PAY_4", "PAY_5", "PAY_6"]
ccRaw = ccRaw.withColumn("AVG_PAY_DUR", round(sum(abs(col(c)) for c in pay_duration_cols) / 6))

# Calculate average percentage paid
ccRaw = ccRaw.withColumn("PER_PAID", round((col("AVG_PAY_AMT") / (col("AVG_BILL_AMT") + 1)) * 100 / 25) * 25)

# Cleaned-up DataFrame
ccDf = ccRaw.select(
    "ID", "LIMIT_BAL", "SEX", "EDUCATION", "MARRIAGE", "AGE",
    "AVG_PAY_DUR", "AVG_BILL_AMT", "AVG_PAY_AMT", "PER_PAID", "DEFAULT"
)

# Add SEX_NAME to the data using SQL Joins
genderDict = [{"SEX": 1, "SEX_NAME": "Male"}, {"SEX": 2, "SEX_NAME": "Female"}]
genderDf = spark.createDataFrame(pd.DataFrame(genderDict))
ccDf = ccDf.join(genderDf, on="SEX", how="left")

# Add ED_STR to the data with SQL Joins
eduDict = [{"EDUCATION": 1, "ED_STR": "Graduate"}, {"EDUCATION": 2, "ED_STR": "University"},
           {"EDUCATION": 3, "ED_STR": "High School"}, {"EDUCATION": 4, "ED_STR": "Others"}]
eduDf = spark.createDataFrame(pd.DataFrame(eduDict))
ccDf = ccDf.join(eduDf, on="EDUCATION", how="left")

# Add MARR_DESC to the data
marrDict = [{"MARRIAGE": 1, "MARR_DESC": "Single"}, {"MARRIAGE": 2, "MARR_DESC": "Married"},
            {"MARRIAGE": 3, "MARR_DESC": "Others"}]
marrDf = spark.createDataFrame(pd.DataFrame(marrDict))
ccDf = ccDf.join(marrDf, on="MARRIAGE", how="left")

# Correlation analysis
for col_name in ccDf.columns:
    if col_name != "ID":
        correlation = ccDf.stat.corr("DEFAULT", col_name)
        print(f"Correlation to DEFAULT for {col_name}: {correlation:.2f}")

# Feature scaling and clustering
ccClustDf = ccDf.select("SEX", "EDUCATION", "MARRIAGE", "AGE", "ID")
summary_stats = ccClustDf.describe().toPandas()
mean_values = summary_stats.iloc[1, 1:5].values.tolist()
std_dev_values = summary_stats.iloc[2, 1:5].values.tolist()


def center_and_scale(in_row):
    ret_array = [(float(in_row[i]) - mean_values[i]) / std_dev_values[i] for i in range(len(mean_values))]
    return Row(CUSTID=in_row[4], features=Vectors.dense(ret_array))


cc_map = ccClustDf.rdd.map(center_and_scale)
cc_f_clust_df = spark.createDataFrame(cc_map)

# Clustering using KMeans
kmeans = KMeans(k=4, seed=1)
model = kmeans.fit(cc_f_clust_df)
predictions = model.transform(cc_f_clust_df)
predictions.select("ID", "features", "prediction").show()

# Prepare data for machine learning
def transform_to_labeled_point(row):
    lp = (float(row["DEFAULT"]),
          Vectors.dense([row["AGE"], row["AVG_BILL_AMT"], row["AVG_PAY_AMT"], row["AVG_PAY_DUR"],
                         row["EDUCATION"], row["LIMIT_BAL"], row["MARRIAGE"], row["PER_PAID"], row["SEX"]]))
    return lp


cc_lp = ccDf.rdd.map(transform_to_labeled_point)
cc_norm_df = spark.createDataFrame(cc_lp)

# Indexing for Decision Trees
string_indexer = StringIndexer(inputCol="label", outputCol="indexed")
si_model = string_indexer.fit(cc_norm_df)
td = si_model.transform(cc_norm_df)

# Split into training and testing data
(training_data, test_data) = td.randomSplit([0.7, 0.3])

# Create and evaluate Decision Trees model
dt_classifier = DecisionTreeClassifier(labelCol="indexed", featuresCol="features")
dt_model = dt_classifier.fit(training_data)
predictions = dt_model.transform(test_data)
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="indexed", metricName="accuracy")
dt_accuracy = evaluator.evaluate(predictions)
dt_precision = evaluator.evaluate(predictions, {evaluator.metricName: "weightedPrecision"})
dt_recall = evaluator.evaluate(predictions, {evaluator.metricName: "weightedRecall"})
dt_f1 = evaluator.evaluate(predictions, {evaluator.metricName: "f1"})

# Create and evaluate Random Forest model
rm_classifier = RandomForestClassifier(labelCol="indexed", featuresCol="features")
rm_model = rm_classifier.fit(training_data)
predictions = rm_model.transform(test_data)
rm_accuracy = evaluator.evaluate(predictions)
rm_precision = evaluator.evaluate(predictions, {evaluator.metricName: "weightedPrecision"})
rm_recall = evaluator.evaluate(predictions, {evaluator.metricName: "weightedRecall"})
rm_f1 = evaluator.evaluate(predictions, {evaluator.metricName: "f1"})

# Create and evaluate Naive Bayes model
nb_classifier = NaiveBayes(labelCol="indexed", featuresCol="features")
nb_model = nb_classifier.fit(training_data)
predictions = nb_model.transform(test_data)
nb_accuracy = evaluator.evaluate(predictions)
nb_precision = evaluator.evaluate(predictions, {evaluator.metricName: "weightedPrecision"})
nb_recall = evaluator.evaluate(predictions, {evaluator.metricName: "weightedRecall"})
nb_f1 = evaluator.evaluate(predictions, {evaluator.metricName: "f1"})

# Create and evaluate Multilayer Perceptron Neural Network model
layers = [9, 6, 6, 2]
mlp_classifier = MultilayerPerceptronClassifier(layers=layers, seed=1)
mlp_model = mlp_classifier.fit(training_data)
predictions = mlp_model.transform(test_data)
mlp_accuracy = evaluator.evaluate(predictions)
mlp_precision = evaluator.evaluate(predictions, {evaluator.metricName: "weightedPrecision"})
mlp_recall = evaluator.evaluate(predictions, {evaluator.metricName: "weightedRecall"})
mlp_f1 = evaluator.evaluate(predictions, {evaluator.metricName: "f1"})

# Create a performance dataframe to collect all evaluation metrics results
performance_df = DataFrame(
    [("Decision Trees", dt_accuracy, dt_precision, dt_recall, dt_f1),
     ("Random Forest", rm_accuracy, rm_precision, rm_recall, rm_f1),
     ("Naive Bayes", nb_accuracy, nb_precision, nb_recall, nb_f1),
     ("Multilayer Perceptron", mlp_accuracy, mlp_precision, mlp_recall, mlp_f1)],
    columns=["Model", "Accuracy", "Precision", "Recall", "F1 Score"]
)

performance_df.show()
