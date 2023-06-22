package app

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.youtube.YouTube
import com.google.api.services.youtube.model.{Channel, ChannelListResponse}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.Queue

case class YoutubeStream(chanelName: String, API_KEY: String) {
  private val channelId = "UCtQqJgutaTeq4-5qydxpRxQ"

  private val spark = SparkSession.builder
    .appName("youtube-stats")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  def startListening(): Unit = {

    // Создание StreamingContext с указанием времени обновления
    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

    val rddQueue = new mutable.Queue[RDD[String]]()
    val stream = ssc.queueStream(rddQueue)

    listenForChannelStats(channelId, rddQueue)

    stream.print

    // Запуск потока
    ssc.start()
    ssc.awaitTermination()
  }

  private def listenForChannelStats(channelId: String, rdd: Queue[RDD[String]]) = {
    val thread = new Thread {
      override def run(): Unit = {
        while (true) {
          // Создание YouTube-клиента
          val youtube = new YouTube.Builder(GoogleNetHttpTransport.newTrustedTransport(), GsonFactory.getDefaultInstance, null)
            .setApplicationName("youtube-stats")
            .build()

          val channelsResponse: ChannelListResponse = youtube.channels().list("snippet,statistics")
            .setId(channelId)
            .setKey("AIzaSyCWj78cHqB_5n4IxHckBRVeVkWh9XbrKoE")
            .execute()
          val channels: java.util.List[Channel] = channelsResponse.getItems
          if (channels != null && !channels.isEmpty) {
            rdd.addOne(spark.sparkContext.parallelize(Seq(channels.get(0).toString)))
          }
          Thread.sleep(1000)
        }
      }
    }

    thread.start()
  }
}
