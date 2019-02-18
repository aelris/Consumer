package com.epam.streaming

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocalFileSystem}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

object SparkJob {

  /**
    * Method reading input stream and write data as parquet files
    * @param topic to read from
    * @param sparkSession for creating data frames to write data as parquet
    * @param parquetPath path to the directory will contain saved data
    */

  private var parquetPath = "hdfs://sandbox-hdp.hortonworks.com:8020/homework/streaming"
  def sparkJob() {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .getOrCreate()

    //configs for connection to HDFS
    val fsConf = new Configuration()
    fsConf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    fsConf.set("fs.file.impl", classOf[LocalFileSystem].getName)
    FileSystem.get(URI.create(parquetPath),fsConf)

    //DataFrame that reads kafka topic line by line
    var dataFrameKafkaRecords: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      .option("subscribe", Consumer.topic)
      .load()

    //StreamingQuery that write lines from kafka topic into parquet file in @parquetPath path
    val value: StreamingQuery = dataFrameKafkaRecords.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .coalesce(1).writeStream.format("parquet")
      .option("header", "false").option("path", parquetPath)
      .option("checkpointLocation", "/tmp/checkpoint")
      .trigger(Trigger.ProcessingTime(1000*3)).start

    value.awaitTermination()
  }

}
