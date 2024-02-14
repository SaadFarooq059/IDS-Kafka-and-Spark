from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, NaiveBayes, LinearSVC
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, ClusteringEvaluator, BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col
import time
from sparkxgb import XGBoostClassifier

spark = SparkSession.builder.appName("DDOS").getOrCreate()


dataPath = "hdfs://192.168.10.12:8020/home/clusters/final_csv"
trainDataPath = dataPath + "/train"
testDataPath = dataPath + "/test"

train_df = spark.read.parquet(trainDataPath)
test_df = spark.read.parquet(testDataPath)


logisticRegression = LogisticRegression(featuresCol='features', labelCol='label')
svm = LinearSVC(featuresCol='features', labelCol='label')
naiveBayes = NaiveBayes(featuresCol='features', labelCol='label')
randomForest = RandomForestClassifier(featuresCol='features', labelCol='label')
xgboost = XGBoostClassifier(featuresCol='features', labelCol='label', numRound=10)

models = {
    "Logistic Regression": logisticRegression,
    "SVM": svm,
    "Naive Bayes": naiveBayes,
    "Random Forest": randomForest,
    "XGBoost": xgboost
}

for name, model in models.items():
    start_time = time.time()
    trainedModel = model.fit(train_df)
    training_time = time.time() - start_time

    start_time = time.time()
    predictions = trainedModel.transform(test_df)
    prediction_time = (time.time() - start_time) * 1000  

    evaluator = MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction', metricName='accuracy')
    accuracy = evaluator.evaluate(predictions)
    
    
    binaryEvaluator = BinaryClassificationEvaluator(labelCol='label', rawPredictionCol="rawPrediction", metricName='areaUnderROC')
    auc = binaryEvaluator.evaluate(predictions)


    print(f"{name} - Training time: {training_time:.2f} minutes, Prediction time: {prediction_time:.2f} ms, Accuracy: {accuracy:.2f}, AUC: {auc:.2f}")

kmeans = KMeans(featuresCol='features', k=3)  
kmeansModel = kmeans.fit(train_df)
kmeansPredictions = kmeansModel.transform(test_df)
kmeansEvaluator = ClusteringEvaluator(featuresCol='features', predictionCol='prediction')
silhouette = kmeansEvaluator.evaluate(kmeansPredictions)
print(f"KMeans Silhouette Score: {silhouette}")

spark.stop()
