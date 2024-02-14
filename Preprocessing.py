from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, MinMaxScaler, ChiSqSelector
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, LongType

def get_spark_session(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()

def define_schema():
    return StructType([
        StructField("Unnamed0", StringType()),
        StructField("FlowID", StringType()),
        StructField("SourceIP", StringType()),
        StructField("SourcePort", IntegerType()),
        StructField("DestinationIP", StringType()),
        StructField("DestinationPort", IntegerType()),
        StructField("Protocol", IntegerType()),
        StructField("Timestamp", StringType()),
        StructField("FlowDuration", LongType()),
        StructField("TotalFwdPackets", IntegerType()),
        StructField("TotalBackwardPackets", IntegerType()),
        StructField("TotalLengthOfFwdPackets", LongType()),
        StructField("FwdPacketLengthMax", IntegerType()),
        StructField("FwdPacketLengthMin", IntegerType()),
        StructField("FwdPacketLengthMean", FloatType()),
        StructField("FwdPacketLengthStd", FloatType()),
        StructField("BwdPacketLengthMax", IntegerType()),
        StructField("BwdPacketLengthMin", IntegerType()),
        StructField("BwdPacketLengthMean", FloatType()),
        StructField("BwdPacketLengthStd", FloatType()),
        StructField("FlowBytesPerSecond", FloatType()),
        StructField("FlowPacketsPerSecond", FloatType()),
        StructField("FlowIATMean", FloatType()),
        StructField("FlowIATStd", FloatType()),
        StructField("FlowIATMax", LongType()),
        StructField("FlowIATMin", LongType()),
        StructField("FwdIATTotal", LongType()),
        StructField("FwdIATMean", FloatType()),
        StructField("FwdIATStd", FloatType()),
        StructField("FwdIATMax", LongType()),
        StructField("FwdIATMin", LongType()),
        StructField("BwdIATTotal", LongType()),
        StructField("BwdIATMean", FloatType()),
        StructField("BwdIATStd", FloatType()),
        StructField("BwdIATMax", LongType()),
        StructField("BwdIATMin", LongType()),
        StructField("FwdPSHFlags", IntegerType()),
        StructField("BwdPSHFlags", IntegerType()),
        StructField("FwdURGFlags", IntegerType()),
        StructField("BwdURGFlags", IntegerType()),
        StructField("FwdHeaderLength", IntegerType()),
        StructField("BwdHeaderLength", IntegerType()),
        StructField("FwdPacketsPerSecond", FloatType()),
        StructField("BwdPacketsPerSecond", FloatType()),
        StructField("MinPacketLength", IntegerType()),
        StructField("MaxPacketLength", IntegerType()),
        StructField("PacketLengthMean", FloatType()),
        StructField("PacketLengthStd", FloatType()),
        StructField("PacketLengthVariance", FloatType()),
        StructField("FINFlagCount", IntegerType()),
        StructField("SYNFlagCount", IntegerType()),
        StructField("RSTFlagCount", IntegerType()),
        StructField("PSHFlagCount", IntegerType()),
        StructField("ACKFlagCount", IntegerType()),
        StructField("URGFlagCount", IntegerType()),
        StructField("CWEFlagCount", IntegerType()),
        StructField("ECEFlagCount", IntegerType()),
        StructField("DownUpRatio", IntegerType()),
        StructField("AveragePacketSize", FloatType()),
        StructField("AvgFwdSegmentSize", FloatType()),
        StructField("AvgBwdSegmentSize", FloatType()),
        StructField("FwdHeaderLength1", IntegerType()),
        StructField("FwdAvgBytesBulk", IntegerType()),
        StructField("FwdAvgPacketsBulk", IntegerType()),
        StructField("FwdAvgBulkRate", IntegerType()),
        StructField("BwdAvgBytesBulk", IntegerType()),
        StructField("BwdAvgPacketsBulk", IntegerType()),
        StructField("BwdAvgBulkRate", IntegerType()),
        StructField("SubflowFwdPackets", IntegerType()),
        StructField("SubflowFwdBytes", IntegerType()),
        StructField("SubflowBwdPackets", IntegerType()),
        StructField("SubflowBwdBytes", IntegerType()),
        StructField("InitWinBytesForward", IntegerType()),
        StructField("InitWinBytesBackward", IntegerType()),
        StructField("ActDataPktFwd", IntegerType()),
        StructField("MinSegSizeForward", IntegerType()),
        StructField("ActiveMean", FloatType()),
        StructField("ActiveStd", FloatType()),
        StructField("ActiveMax", LongType()),
        StructField("ActiveMin", LongType()),
        StructField("IdleMean", FloatType()),
        StructField("IdleStd", FloatType()),
        StructField("IdleMax", LongType()),
        StructField("IdleMin", LongType())
    ])

def preprocess_and_write_to_hdfs(spark, input_topic, output_hdfs_path, brokers):
    kafkaDataFrame = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", brokers) \
        .option("subscribe", input_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    schema = define_schema()
    valueDataFrame = kafkaDataFrame.select(from_json(col("value").cast("string"), schema).alias("parsed_data"))
    networkTrafficDataFrame = valueDataFrame.select("parsed_data.*")

    # Example: Assemble features, scale them, and select top features via Chi-Squared test
    featureCols = [field.name for field in schema.fields if field.dataType != StringType() and field.name != "Protocol"]
    assembler = VectorAssembler(inputCols=featureCols, outputCol="features")
    scaler = MinMaxScaler(inputCol="features", outputCol="scaledFeatures")
    selector = ChiSqSelector(numTopFeatures=10, featuresCol="scaledFeatures", outputCol="selectedFeatures", labelCol="Protocol")
    
    pipeline = Pipeline(stages=[assembler, scaler, selector])
    model = pipeline.fit(networkTrafficDataFrame)
    processedDataFrame = model.transform(networkTrafficDataFrame)

    # Write the processed DataFrame to HDFS in parquet format
    query = processedDataFrame.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", output_hdfs_path) \
        .option("checkpointLocation", "/path/to/checkpoint/dir") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    app_name = "DataPreprocessingToHDFS"
    input_topic = "network-traffic"
    brokers = "broker1:9092,broker2:9092,broker3:9092"
    output_hdfs_path = "hdfs://namenode:8020/path/to/preprocessed/data"

    spark = get_spark_session(app_name)
    preprocess_and_write_to_hdfs(spark, input_topic, output_hdfs_path, brokers)
