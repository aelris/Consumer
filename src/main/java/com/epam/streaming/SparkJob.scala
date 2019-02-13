package com.epam.streaming

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocalFileSystem}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql._

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

    val schema = new StructType()
      .add("offset", DataTypes.LongType)
      .add("value", DataTypes.StringType)

    var dataFrameKafkaRecords: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      .option("subscribe", Consumer.topic)
      .load()

    val value: StreamingQuery = dataFrameKafkaRecords.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .coalesce(1).writeStream.format("csv")
      .option("header", "false").option("path", csvPath)
      .option("checkpointLocation", "/tmp/checkpoint")
      .trigger(Trigger.ProcessingTime(1000*3)).start

    value.awaitTermination()
  }

}
