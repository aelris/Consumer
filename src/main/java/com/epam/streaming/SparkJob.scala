package com.epam.streaming

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object SparkJob {
  private var csvPath ="hdfs://sandbox-hdp.hortonworks.com:8020/homework/streaming"
  def sparkJob() {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .getOrCreate()

    val dataFrameKafkaRecords: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      .option("subscribe", "StreamingTopic")
      .load()

    dataFrameKafkaRecords.write.mode(SaveMode.Append).csv(csvPath)
  }
}
