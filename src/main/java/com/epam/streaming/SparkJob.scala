package com.epam.streaming

import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.sql.{DataFrame, SparkSession}


object SparkJob {
  private var csvPath = "hdfs://sandbox-hdp.hortonworks.com:8020/homework/streaming"

  def sparkJob() {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .config("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
      .config("fs.file.impl", classOf[LocalFileSystem].getName)
      .getOrCreate()

    val dataFrameKafkaRecords: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      .option("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
      .option("fs.file.impl", classOf[LocalFileSystem].getName)
      .option("subscribe", "StreamingTopic")
      .csv(csvPath)

    //    dataFrameKafkaRecords.write.mode(SaveMode.Append).csv(csvPath)
  }
}
