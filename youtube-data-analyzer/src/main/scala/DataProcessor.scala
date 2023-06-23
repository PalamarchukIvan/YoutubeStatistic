import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.shaded.org.codehaus.jackson.`type`.TypeReference
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Encoder, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.time.Instant
import java.util.{Date, Properties}
case class DataProcessor() {
  def startListening(): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaStreamingApp")
      .master("local[*]") // Set your Spark master URL accordingly
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val schemaRowData = StructType(Seq(
      StructField("hiddenSubscriberCount", BooleanType),
      StructField("subscriberCount", StringType),
      StructField("videoCount", StringType),
      StructField("viewCount", StringType)
    ))

    val schemaActualData = StructType(Seq(
      StructField("date", StringType),
      StructField("subscriberCount", IntegerType),
      StructField("subscribersDif", IntegerType),
      StructField("videoCount", IntegerType),
      StructField("videoCountDif", IntegerType),
      StructField("viewCount", IntegerType),
      StructField("viewCountDif", IntegerType)
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
        ds.show
        ds.schema

        // Сохраняем DataFrame в базу данных PostgreSQL
        val connectionProperties = new Properties()
        connectionProperties.put("user", "postgres")
        connectionProperties.put("password", "postgres")
        df.write.mode(SaveMode.Append)
          .jdbc("jdbc:postgresql://localhost:5432/employee",
            "youtube_data", connectionProperties)
      }
    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
