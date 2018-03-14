import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object dgg {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession.builder.master("local")
      .appName("spark session example")
      .getOrCreate()

    import spark.implicits._


    val schema = StructType(
        Array(
          StructField("movieId", IntegerType),
          StructField("title", StringType),
          StructField("genres", StringType)
        )
    )

    val streamingDataFrame = spark
      .readStream
      .schema(schema)
      .option("header","true")
      .format("csv")
      .load("hdfs://192.168.131.30/user/ubuntu/movies.*")


    streamingDataFrame.selectExpr("CAST(movieId AS STRING) AS key", "to_json(struct(*)) AS value").
      writeStream
      .format("kafka")
      .option("topic", "topic")
      .option("kafka.bootstrap.servers", "192.168.2.207:9092")
      .option("checkpointLocation", "/tmp/")
      .start()


    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.2.207:9092")
      .option("subscribe", "topic")
      .load()

    val df1 = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)]
      .select(from_json($"value", schema).as("data"), $"timestamp")
      .select("data.*", "timestamp")

    df1.writeStream
      .format("console")
      .option("truncate","false")
      .start()
      .awaitTermination()
    
  }

}
