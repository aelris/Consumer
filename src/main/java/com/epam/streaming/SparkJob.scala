package com.epam.streaming

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocalFileSystem}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkJob {
  private var csvPath = "hdfs://sandbox-hdp.hortonworks.com:8020/homework/streaming"

  def sparkJob() {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .getOrCreate()

    val fsConf = new Configuration()
    fsConf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    fsConf.set("fs.file.impl", classOf[LocalFileSystem].getName)
    FileSystem.get(URI.create(csvPath),fsConf)

    val dataFrameKafkaRecords: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      .option("subscribe", Consumer.topic)
      .csv(csvPath)

    dataFrameKafkaRecords.write.mode(SaveMode.Append).csv(csvPath)
  }
}
