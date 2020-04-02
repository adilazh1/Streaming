package org.adilazh1.utils

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import twitter4j.TwitterStreamFactory
import twitter4j.conf.ConfigurationBuilder

import scala.collection.immutable.HashMap
import scala.io.Source

object Utils {

   var CONSUMER_KEY = ""
   var CONSUMER_SECRET = ""
   var ACCESS_KEY = ""
   var ACCESS_SECRET = ""
   def getKafkaStream(streamingContext: StreamingContext, twitterPath: String): InputDStream[ConsumerRecord[String,String]] = {

     setupTwitter(twitterPath)

     val cb = new ConfigurationBuilder()
     cb.setDebugEnabled(true)
       .setOAuthConsumerKey(CONSUMER_KEY)
       .setOAuthConsumerSecret(CONSUMER_SECRET)
       .setOAuthAccessToken(ACCESS_KEY)
       .setOAuthAccessTokenSecret(ACCESS_SECRET)

     val tf = new TwitterStreamFactory(cb.build).getInstance()
     tf.addListener(new LambdaListener)
     tf.sample()


     val topics = Array("twitter")

     val kafkaParams = Map[String, Object](
       "bootstrap.servers" -> "localhost:9092",
       "key.deserializer" -> classOf[StringDeserializer],
       "value.deserializer" -> classOf[StringDeserializer],
       "group.id" -> "use_a_separate_group_id_for_each_stream",
       "auto.offset.reset" -> "latest",
       "enable.auto.commit" -> (false: java.lang.Boolean)
     )

     val kafkaStream = KafkaUtils.createDirectStream[String,String](
       streamingContext,
       LocationStrategies.PreferConsistent,
       ConsumerStrategies.Subscribe[String,String](topics,kafkaParams) )

     kafkaStream

   }

  def configureTwitterCredentials(consumerKey: String, consumerSecret: String, accessToken: String, accessTokenSecret: String): Unit = {
    val config = HashMap("appKey"->consumerKey,
                         "consumerSecret"->consumerSecret,
                         "accessToken"->accessToken,
                         "accessTokenSecret"->accessTokenSecret)

    val keys = config.keySet.toArray

    var i = 0
    for(i <- 0 to keys.length-1){
      val key = keys(i).toString
      val value = config.get(key).get.trim
      val fullKey = "twitter4j.oauth." + key
      System.setProperty(fullKey, value)
      println(s"Property  ${key} set as [$value]")
    }
    CONSUMER_KEY = config.get("appKey").get.trim
    CONSUMER_SECRET = config.get("consumerSecret").get.trim
    ACCESS_KEY = config.get("accessToken").get.trim
    ACCESS_SECRET = config.get("accessTokenSecret").get.trim

  }

  def setupTwitter(twitterPath : String):Unit ={
    val file = Source.fromFile(twitterPath)
    val props = new Properties()
    props.load(file.reader())

    val consumerKey = props.getProperty("consumerKey")
    val consumerSecret = props.getProperty("consumerSecret")
    val accessToken = props.getProperty("accessToken")
    val accessTokenSecret = props.getProperty("accessTokenSecret")

    configureTwitterCredentials(consumerKey, consumerSecret, accessToken, accessTokenSecret)
  }

}
