package org.adilazh1.utils

import java.io.IOException
import java.util

import org.apache.log4j.Logger

import scala.io.Source

class StopWords extends Serializable {

  private val serialVersionUID = 42L
  private val stopWords = new util.ArrayList[String]()
  private var _singleton= StopWords

  def StopWords(): Unit ={

    try{
      val source = Source.fromInputStream(this.getClass.getResourceAsStream("/stop-words.txt")).bufferedReader()
      var boolean = true
      var line = ""

      while( boolean ){
        line = source.readLine()
        this.stopWords.add(line)
        if(line == null) boolean = false
      }
      source.close()

    }catch {
      case e:IOException => Logger.getLogger(this.getClass).error("IO error while reading stopWords",e)
    }
  }

  def get(wordsType:String) = {
    if(this._singleton ==null) _singleton = new StopWords
    _singleton
  }
  def getWords()={
    this.stopWords
  }

}
