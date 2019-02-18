Build project with maven plugin:

mvn package

Run jar with 2 parameters using spark-submit if running on HDP 2.6:

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 pathToJarFile.jar topicName

To run tests install Kafka on your PC and run it on localhost:6667