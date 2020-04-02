package org.adilazh1.utils

import java.util.Properties

import twitter4j._

 class LambdaListener extends StatusListener{

  import org.apache.kafka.clients.producer.ProducerConfig

  private var KAFKA_CONFIG : Properties = new Properties()
  private val TOPIC = "twitter"

  KAFKA_CONFIG.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  KAFKA_CONFIG.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  KAFKA_CONFIG.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  import org.apache.kafka.clients.producer.KafkaProducer

   val producer = new KafkaProducer[String, String](KAFKA_CONFIG)

  def LambdaListener() {
    this.producer.initTransactions()
  }

  override def onStatus(tweet: Status): Unit = {
    val json = new JSONObject()
    import twitter4j.JSONException
    try {
      json.put("id", tweet.getId)
      json.put("text", tweet.getText.replaceAll("\n", " ").replaceAll(":", ""))
      json.put("created", tweet.getCreatedAt.toString.replaceAll("\n", " ").replaceAll(":", ""))
      json.put("user_name",tweet.getUser.getName)
      json.put("location",tweet.getUser.getLocation)
    } catch {
      case e: JSONException =>
        e.printStackTrace()
    }
    import org.apache.kafka.clients.producer.ProducerRecord
    val message = new ProducerRecord[String, String](TOPIC, String.valueOf(tweet.getId), json.toString)

    this.producer.send(message)

  }

  override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {}

  override def onTrackLimitationNotice(i: Int): Unit = {}

  override def onScrubGeo(l: Long, l1: Long): Unit = {}

  override def onStallWarning(stallWarning: StallWarning): Unit = {}

  override def onException(e: Exception): Unit = {}

}
