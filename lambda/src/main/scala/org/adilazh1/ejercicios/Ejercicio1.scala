package org.adilazh1.ejercicios

import java.util

import com.mongodb.spark.MongoSpark
import org.adilazh1.utils.{Utils, WriterClient}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.bson.Document
import twitter4j.JSONObject

import scala.collection.JavaConverters._
import scala.util.Try

object Ejercicio1 {

  import org.adilazh1.utils.WriterServer
  def run(twitterPath:String): Unit = {
    //Creating and starting up the writer server for HDFS
    val writerServer = new WriterServer
    writerServer.start

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("lambda")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/twitter.twitter_summary")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/twitter.twitter_summary")
      .getOrCreate();

    val sparkContext = spark.sparkContext
    val streamContext = new StreamingContext(sparkContext, Seconds(1))

    val kafkaStream = Utils.getKafkaStream(streamContext,twitterPath)


    kafkaStream.mapPartitions(messages => {
      val writer = new WriterClient

      val tweets = new util.ArrayList[Document]
      val tweetsRaw = new util.ArrayList[String]

      while (messages.hasNext) {
        val tweetRaw = messages.next().value()

        //Write the tweet into hdfs
        writer.write((tweetRaw + "\n").getBytes)

        val tweet = new JSONObject(tweetRaw)
        tweetsRaw.add(tweetRaw)

        val tweetSummary : Document = new Document()
        val hashtags : util.ArrayList[Document] = new util.ArrayList[Document]
        var inserted = false
        //Extract tweet id, hashtags,user, locality and time to save into mongodb
        val words = tweet.getString("text").split(" ")

        for (word <- words) {

          if (word.startsWith("#") && words.length > 1) {

            if (!inserted) {
              tweetSummary.put("_id", tweet.getLong("id"))
              tweetSummary.put("user_name", tweet.getString("user_name"))
              tweetSummary.put("location", if (Try(tweet.getString("location")).isSuccess) tweet.getString("location") else "null")
              tweetSummary.put("created", tweet.getString("created"))
              inserted = true
            }

            hashtags.add(new Document("hashtag", word.substring(1)))

          }
        }

        if(inserted){
          tweetSummary.put("hashtags", hashtags)
          tweets.add(tweetSummary)
        }

      }

      writer.close
      tweets.iterator
    }.asScala).foreachRDD(rdd =>{

      val tweetSummaries = rdd.map(tweetSummary =>{
        val tweetS = new Document()
        tweetS.put("_id",tweetSummary.getLong("_id"))
        tweetS.put("user_name",tweetSummary.getString("user_name"))
        tweetS.put("location",tweetSummary.getString("location"))
        tweetS.put("created",tweetSummary.getString("created"))
        tweetS.put("hashtags",tweetSummary.get("hashtags"))
        tweetS
      })

      //Save docs into mongoDB
      MongoSpark.save(tweetSummaries)
    })

    streamContext.start()
    streamContext.awaitTermination()

    writerServer.finish()

  }

}
