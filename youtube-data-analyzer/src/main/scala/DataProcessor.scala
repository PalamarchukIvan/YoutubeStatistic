import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.youtube.YouTube
import model.{DataFromDB, ObtainedData, RowObtainedData, Video}
import org.apache.spark.sql.functions.typedLit
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.time.LocalDate
import java.util.Properties
import scala.collection.mutable
case class DataProcessor(channelId: String, API_KEY: String) {


  private val connectionProperties = new Properties()
  connectionProperties.put("user", "postgres")
  connectionProperties.put("password", "postgres")

  private val jdbcUrl = "jdbc:postgresql://localhost:5432/youtube_project"
  private val spark = SparkSession.builder()
    .appName("KafkaStreamingApp")
    .master("local[6]") // Set your Spark master URL accordingly
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
        resultDs.write.mode(SaveMode.Append)
          .jdbc(jdbcUrl,
            "youtube_data", connectionProperties)

        resultDs.collect.foreach { data =>
          updateOnNewVideo(data.videoCountDif.toDouble.toInt, prev.id_data)
        }
        println()
      }
    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }
  private def updateOnNewVideo(count: Int, id: Int): Unit = {
    val youtube = new YouTube.Builder(GoogleNetHttpTransport.newTrustedTransport(), GsonFactory.getDefaultInstance, null)
      .setApplicationName("youtube-stats")
      .build()

    val resultSearch = youtube.search()
      .list("snippet")
      .setChannelId(channelId)
      .setMaxResults(count)
      .setOrder("date") // Sort by date
      .setType("video")
      .setKey(API_KEY)
      .execute();

    val videos = mutable.Queue[Video]()
    resultSearch.getItems.forEach { video =>
        val title = video.getSnippet.getTitle
        val description = video.getSnippet.getDescription
        val dataId = id
      videos.addOne(Video(dataId, title, description))
    }
    saveNewVideo(videos.toList)
  }

  private def getPrevData(): DataFromDB = {
    val query =
      """
        |SELECT *
        |FROM youtube_data
        |ORDER BY id_data DESC
        |LIMIT 1
        |""".stripMargin

    import spark.implicits._
    val lastRecordDF = spark.read.jdbc(jdbcUrl, s"($query) AS tmp", connectionProperties)
    lastRecordDF.show

    val lastRecord = if (lastRecordDF.isEmpty) {
      DataFromDB(2, "0", "0", "0", "0", "0", "0", "0")
    } else {
      lastRecordDF.as[DataFromDB].head()
    }

    lastRecord
  }

  private def saveNewVideo(videos: List[Video]): Unit = {
    import spark.implicits._
    val videoDF = videos.toDF("data_id", "video_name", "video_description")
    videoDF.write.mode(SaveMode.Append).jdbc(jdbcUrl, "videos", connectionProperties)
  }
}
