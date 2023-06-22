import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Properties


case class ObtainedData(longField: Long, stringField: String)
case class DataProcessor() {

  def startAnalyzing(): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkKafkaExample")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-consumer-group",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    val topics = Array("youtube-stats-update")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val df = spark.createDataFrame(rdd.map { record =>
          ObtainedData(0, record.value().toString)
        })

        // Сохраняем DataFrame в базу данных PostgreSQL
        val connectionProperties = new Properties()
        connectionProperties.put("user", "postgres")
        connectionProperties.put("password", "postgres")
        df.write.mode(SaveMode.Append)
          .jdbc("jdbc:postgresql://localhost:5432/employee",
            "test_table", connectionProperties)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
