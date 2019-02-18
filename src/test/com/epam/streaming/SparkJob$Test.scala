package com.epam.streaming

import java.nio.file.{Files, Paths}
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfter, FlatSpec}


class SparkJob$Test extends FlatSpec with BeforeAndAfter{
  private var sparkSession: SparkSession = _

  private var parquetPath = "src/test/resources"

  def createConsumerConfig(brokers: String, groupId: String): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:6667")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-1")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    return props
  }

  def pathFolderExists(): Boolean = {
    Files.exists(Paths.get(parquetPath))
  }

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .getOrCreate()

  var dataFrameKafkaRecords: DataFrame = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:6667")
    .option("subscribe", Consumer.topic)
    .load()
  Thread.sleep(1000)
  assert(dataFrameKafkaRecords!=null)

  val value: StreamingQuery = dataFrameKafkaRecords.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .coalesce(1).writeStream.format("parquet")
    .option("header", "false").option("path", parquetPath)
    .option("checkpointLocation", "/tmp/checkpoint")
    .trigger(Trigger.ProcessingTime(1000*3)).start
  Thread.sleep(1000)
  assert(value!=null)
}

