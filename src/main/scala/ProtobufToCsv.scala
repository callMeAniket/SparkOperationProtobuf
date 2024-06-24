import Aniket.Metric
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Encoders, SparkSession}

import java.util.Base64

object ProtobufToCsv extends App {

  // Initialize SparkSession
  val spark = SparkSession.builder()
    .appName("Serialise-Kafka-Topic")
    .master("local[2]")
    .config("spark.ui.port", "4060")
    .getOrCreate()

  import spark.implicits._

  // Read data from Kafka
  val kafkaDataFrame = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "demetric")
    .option("startingOffsets", "earliest")
    .load()

  // Deserialize Kafka messages
  val deserializedData = kafkaDataFrame
    .selectExpr("CAST(value AS STRING) as encoded_value")
    .as[String]
    .flatMap { encodedValue =>
      try {
        val bytes = Base64.getDecoder.decode(encodedValue)
        val metric = Metric.parseFrom(bytes)
        Some((
          metric.getHost,
          metric.getMetricName,
          metric.getRegion,
          metric.getTimestamp,
          metric.getValue
        ))
      } catch {
        case _: Exception => None
      }
    }(Encoders.tuple(Encoders.STRING, Encoders.STRING, Encoders.STRING, Encoders.STRING, Encoders.scalaDouble))
    .toDF("host", "metricName", "region", "timestamp", "value")

  // Write deserialized data to CSV
  val query = deserializedData
    .writeStream
    .outputMode("append")
    .format("csv")
    .option("path", "/Users/aniketsharma/Desktop/project2/csv_files")
    .option("checkpointLocation", "/Users/aniketsharma/Desktop/project2/csv_checkpoints")
    .trigger(Trigger.ProcessingTime("3 seconds"))
    .partitionBy("metricName")
    .start()

  // Await termination
  query.awaitTermination()
}