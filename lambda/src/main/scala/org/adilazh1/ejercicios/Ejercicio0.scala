package org.adilazh1.ejercicios

import org.adilazh1.utils.Utils
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.JSONObject

object Ejercicio0 {

  def run(twitterPath:String): Unit ={

    val spark = SparkSession.builder.appName("lambda").master("local[*]").getOrCreate()
    val sparkContext = spark.sparkContext

    val streamContext = new StreamingContext(sparkContext,Seconds(1))

    val kafkaStream = Utils.getKafkaStream(streamContext,twitterPath)

    kafkaStream.map(tweet => {
     val json = new JSONObject(tweet.value())
     json.toString
   }).print()

    streamContext.start()
    streamContext.awaitTermination()
  }
}
