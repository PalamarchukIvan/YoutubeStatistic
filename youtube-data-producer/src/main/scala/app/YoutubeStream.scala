package app

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.youtube.YouTube
import com.google.api.services.youtube.model.{Channel, ChannelListResponse}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Properties
import scala.collection.mutable
import scala.collection.mutable.Queue

case class YoutubeStream(channelId: String, API_KEY: String) {
  private val topic = "youtube-stats-update"
  private val kafkaProps = new Properties()

  private val spark = SparkSession.builder
    .appName("youtube-stats")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  def start(): Unit = {

    // Создание StreamingContext с указанием времени обновления
    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

    val rddQueue = new mutable.Queue[RDD[String]]()
    val stream = ssc.queueStream(rddQueue)


    setUpKafka()

    listenForChannelStats(channelId, rddQueue)

    stream.foreachRDD { rdd =>
      rdd.foreach{ data =>

        val dataProducer = new KafkaProducer[String, String](kafkaProps)

        val record = new ProducerRecord[String, String](topic, data)
        dataProducer.send(record)
        System.err.println(data + "was sent")
        dataProducer.close()
      }
    }

    // Запуск потока
    ssc.start()
    ssc.awaitTermination()
  }

  private def setUpKafka(): Unit = {
    kafkaProps.put("bootstrap.servers", "localhost:9092")
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  }

  private def listenForChannelStats(channelId: String, rdd: Queue[RDD[String]]): Unit = {
    val thread = new Thread {
      override def run(): Unit = {
        while (true) {
          // Создание YouTube-клиента
          val youtube = new YouTube.Builder(GoogleNetHttpTransport.newTrustedTransport(), GsonFactory.getDefaultInstance, null)
            .setApplicationName("youtube-stats")
            .build()

          val channelsResponse: ChannelListResponse = youtube.channels().list("snippet,statistics")
            .setId(channelId)
            .setKey(API_KEY)
            .execute()
          val channels: java.util.List[Channel] = channelsResponse.getItems
          if (channels != null && !channels.isEmpty) {
            rdd.addOne(spark.sparkContext.parallelize(Seq(channels.get(0).toString.split("\"statistics\":")(1).dropRight(1))))
          }
          Thread.sleep(10000)
        }
      }
    }

    thread.start()
  }
}
