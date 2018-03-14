# spark_2.2_sbt_straming_kafka

export SPARK_KAFKA_VERSION=0.10

# On project directory
sbt package
cd target/scala-2.11/
spark2-submit --master yarn --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.1,org.apache.kafka:kafka-clients:0.10.2.1 spark22_2.11-1.0.jar


