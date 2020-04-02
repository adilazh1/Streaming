package org.adilazh1.utils

import java.io.IOException
import java.util

import org.apache.log4j.Logger

import scala.io.Source

class PositiveWords extends Serializable {

  private val serialVersionUID = 42L
  private val posWords = new util.HashSet[String]()
  private var _singleton= PositiveWords


  def PositiveWords(): Unit ={

    try{
      val source = Source.fromInputStream(this.getClass.getResourceAsStream("/pos-words.txt")).bufferedReader()
      var boolean = true
      var line = ""

      while( boolean ){
        line = source.readLine()
        this.posWords.add(line)
        if(line == null) boolean = false
      }
      source.close()

    }catch {
      case e:IOException => Logger.getLogger(this.getClass).error("IO error while reading PositiveWords",e)
    }
  }
  
  def get(wordsType:String) = {
    if(this._singleton ==null) _singleton = new PositiveWords
    _singleton
  }
  def getWords()={
    this.posWords
  }

}
