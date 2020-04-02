package org.adilazh1.ejercicios

import com.mongodb.spark.MongoSpark
import org.adilazh1.utils.{LanguageDetector, NegativeWords, PositiveWords, StopWords}
import org.apache.spark.sql.SparkSession
import org.bson.Document

object Ejercicio2 {

  private val HDFS = "hdfs://quickstart.cloudera:8020"
  def run():Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("lambda")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/twitter.twitter_summary")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/twitter.twitter_summary")
      .getOrCreate();

    val sparkContext = spark.sparkContext

    val englishTweets = sparkContext.textFile(s"$HDFS/user/cloudera/lambda").filter(tweet => LanguageDetector.isEnglish(Document.parse(tweet).getString("text"))).cache()

    //Get tweetID,Text and apply some filters.
    val tweetIDtext = englishTweets.map(tweet => {
      val doc = Document.parse(tweet)
      val id = doc.getLong("id")
      val text = doc.getString("text").replaceAll("[^a-zA-Z\\s]", "").trim.toLowerCase
      (id,text)
    }).filter(f => !Option(f._2).isEmpty)

    //apply stemming functions

    val tweetsFiltered = tweetIDtext.map(tweet => {
      var text = tweet._2
      val stopWords = new StopWords()
      val list = stopWords.getWords

      for (word <- list.toArray) {
        text = text.replaceAll("\\b " + word + " \\b", " ")
      }
      (tweet._1,text)
    }).cache()

    // Calculate positive score
    val pos_negTweets = tweetsFiltered.map(tweet => {
      val text = tweet._2
      val posWords = new PositiveWords
      val set_pos = posWords.getWords
      val negWords = new NegativeWords
      val set_neg = negWords.getWords()
      val words = text.split(" ")
      var posCount = 0
      var negCount = 0

      for(word <- words ){
        if(set_pos.contains(word)) posCount +=1
        if(set_neg.contains(word)) negCount +=1
      }
      (tweet._1,text, 1.0*posCount/words.length,1.0*negCount/words.length)
    }).filter(tweet => tweet._3 > 0.0 && tweet._4 > 0.0)

    val tweetSentiments =  pos_negTweets.map(tweet =>{
      val doc = new Document
      doc.put("id",tweet._1)
      doc.put("text",tweet._2)
      var score = "neutral"
      if(tweet._3 > tweet._4) score = "positive" else if(tweet._3 < tweet._4) score = "negative"
      doc.put("sentiment",score)
      doc
    })

    MongoSpark.save(tweetSentiments)


    spark.close()
  }

}
