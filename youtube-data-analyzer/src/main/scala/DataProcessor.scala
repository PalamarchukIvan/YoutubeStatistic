import org.apache.spark.sql.functions.typedLit
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.time.LocalDate
import java.util.Properties
case class DataProcessor() {

  val spark = SparkSession.builder()
    .appName("KafkaStreamingApp")
    .master("local[*]") // Set your Spark master URL accordingly
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  def startListening(): Unit = {

    val schemaRowData = StructType(Seq(
      StructField("hiddenSubscriberCount", BooleanType),
      StructField("subscriberCount", StringType),
      StructField("videoCount", StringType),
      StructField("viewCount", StringType)
    ))

    val kafkaParams = Map(
      "bootstrap.servers" -> "localhost:9092", // Set your Kafka brokers accordingly
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "myGroupId"
    )

    val topics = Array("youtube-stats-update")

    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(1))

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.foreachRDD { rdd =>
      if(!rdd.isEmpty()) {
        val jsonStream = rdd.map{ line =>
          line.value()
        }

        val df = spark.read
          .schema(schemaRowData)
          .json(jsonStream)

        import spark.implicits._
        val ds = df.as[RowObtainedData]
        val prev = getPrevData()

        val analyzedDs =  ds.withColumn("subscribersDif", $"subscriberCount" - prev.subscriberCount)
          .withColumn("videoCountDif", $"videoCount" - prev.videoCount)
          .withColumn("viewCountDif", $"viewCount" - prev.viewCount)
          .withColumn("date", typedLit(LocalDate.now().toString))
          .drop("hiddenSubscriberCount")

        val resultDs = analyzedDs.as[ObtainedData]

        // Сохраняем DataFrame в базу данных PostgreSQL
        val connectionProperties = new Properties()
        connectionProperties.put("user", "postgres")
        connectionProperties.put("password", "postgres")
        resultDs.write.mode(SaveMode.Append)
          .jdbc("jdbc:postgresql://localhost:5432/employee",
            "youtube_project", connectionProperties)
      }
    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  private def getPrevData(): ObtainedData = {
    // Сохраняем DataFrame в базу данных PostgreSQL
    val connectionProperties = new Properties()
    connectionProperties.put("user", "postgres")
    connectionProperties.put("password", "postgres")

    val jdbcUrl = "jdbc:postgresql://localhost:5432/employee"

    val query =
      """
        |SELECT *
        |FROM youtube_project
        |ORDER BY id_data DESC
        |LIMIT 1
        |""".stripMargin

    import spark.implicits._
    val lastRecordDF = spark.read.jdbc(jdbcUrl, s"($query) AS tmp", connectionProperties)
    lastRecordDF.show

    val lastRecord = if (lastRecordDF.isEmpty) {
      ObtainedData("0", "0", "0", "0", "0", "0", "0")
    } else {
      lastRecordDF.as[ObtainedData].head()
    }

    lastRecord
  }

}
