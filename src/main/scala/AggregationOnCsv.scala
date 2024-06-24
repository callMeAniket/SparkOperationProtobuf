import org.apache.spark.sql.SparkSession
import java.io.{File, PrintWriter}

object AggregationOnCsv extends App {

  val spark = SparkSession.builder()
    .appName("CSV Folder Processor")
    .master("local[*]")
    .config("spark.ui.port", "4080")
    .getOrCreate()

  def getListOfSubDirectories(directoryName: String): Array[String] = {
    new File(directoryName)
      .listFiles
      .filter(_.isDirectory)
      .filter(dir => !dir.getName.contains("_spark_metadata"))
      .map(_.getPath)
  }

  def processFolder(folderPath: String): Unit = {
    println(s"Processing folder: $folderPath")

    val df = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .csv(s"$folderPath/*.csv")
      .toDF("server", "region", "timestamp", "value")

    val recordCount = df.count()

    val maxServerValue = df.groupBy("server").max("value").collect().mkString("\n")

    val maxRegionValue = df.groupBy("region").max("value").collect().mkString("\n")

    val writer = new PrintWriter(new File(s"$folderPath/output.txt"))
    try {
      writer.write(s"Total records: $recordCount\nMaxServerValue:\n$maxServerValue\n\nMaxRegionValue:\n$maxRegionValue")
    } finally {
      writer.close()
    }
  }

  val baseDirectory = "/Users/aniketsharma/Desktop/project2/csv_files"

  val subFolders = getListOfSubDirectories(baseDirectory)
  subFolders.foreach(processFolder)

  spark.stop()
}