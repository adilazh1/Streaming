package org.adilazh1

import org.adilazh1.ejercicios.{Ejercicio0, Ejercicio1, Ejercicio2}
import org.apache.log4j.{Level, LogManager}

/**
 * @author ${user.name}
 */
object App {

  lazy val TWITTER_CONFIG_PATH = "/home/cloudera/Downloads/Spark-master/lambda/src/main/resources/twitter.conf"

  def main(args : Array[String]) {

    LogManager.getRootLogger.setLevel(Level.ERROR)

    run(args(0))
  }

  def run(arg:String) = arg match {

    case "ejercicio_0" => Ejercicio0.run(TWITTER_CONFIG_PATH) //Print some atributs of tweets
    case "speed_layer" => Ejercicio1.run(TWITTER_CONFIG_PATH) //Save the tweets into hdfs and save same interests atributs like hashtads into MongoDB
    case "batch_layer" => Ejercicio2.run()  //Save tweet_id, hashtags and sentimental analysis of its hashtags. Only for English tweets
  }
}
