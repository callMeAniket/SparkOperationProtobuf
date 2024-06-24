import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

import java.util.Base64

object JsonKafkaToProtobufKafka extends App {
  val spark = SparkSession.builder()
    .appName("KafkaProtobufJob")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // Define the schema for the incoming JSON data
  val schema = new StructType()
    .add("host", StringType)
    .add("metricName", StringType)
    .add("region", StringType)
    .add("timestamp", StringType)
    .add("value", DoubleType)

  // Read data from Kafka
  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "metrics-topic1")
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("CAST(value AS STRING) as json")

  // Parse JSON data and extract fields
  val metrics = df.select(from_json($"json", schema).as("data")).select("data.*")

  // Convert to Protobuf and encode as Base64
  val protobufData = metrics.map { row =>
    import Aniket.Metric

    val metric = Metric.newBuilder()
      .setHost(row.getAs[String]("host"))
      .setMetricName(row.getAs[String]("metricName"))
      .setRegion(row.getAs[String]("region"))
      .setTimestamp(row.getAs[String]("timestamp"))
      .setValue(row.getAs[Double]("value"))
      .build()
    Base64.getEncoder.encodeToString(metric.toByteArray)
  }

  // Filter out invalid data
  val validatedData = protobufData.filter(row => row != null && row.nonEmpty)

  // Write to Kafka
  val query = validatedData
    .writeStream
    .outputMode("append")
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "demetric")
    .trigger(Trigger.ProcessingTime("3 seconds"))
    .option("checkpointLocation", "/Users/aniketsharma/Desktop/project2")
    .start()

  query.awaitTermination()
}