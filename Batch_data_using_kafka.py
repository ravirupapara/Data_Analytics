
def kafka_batch_processing(kafka_broker,topic_name,output_kafka):
    spark=SparkSession.builder.appName('KafkaProcessing').getOrCreate()
    starting_offset="latest"
    max_offset_per_batch="1000"
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", starting_offset) \
        .option("maxOffsetsPerTrigger", max_offset_per_batch) \
        .load()


    processed_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # Write the processed data to CSV files with partitions
    os.mkdirs(output_kafka,exist_ok=True)

    processed_df.writeStream.format("csv").outputMode("append").option("path", output_kafka).option("sep","|").option("header","true").start()

    # Start the Spark streaming job
    spark.streams.awaitAnyTermination()

    spark.stop()

