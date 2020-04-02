package org.adilazh1.utils

import java.io.IOException
import java.util

import org.apache.log4j.Logger

import scala.io.Source

class NegativeWords extends Serializable {

  private val serialVersionUID = 42L
  private val negWords = new util.HashSet[String]()
  private var _singleton= NegativeWords


  def NegativeWords(): Unit ={

    try{
      val source = Source.fromInputStream(this.getClass.getResourceAsStream("/neg-words.txt")).bufferedReader()
      var boolean = true
      var line = ""

      while( boolean ){
        line = source.readLine()
        this.negWords.add(line)
        if(line == null) boolean = false
      }
      source.close()

    }catch {
      case e:IOException => Logger.getLogger(this.getClass).error("IO error while reading NegativeWords",e)
    }
  }
  
  def get(wordsType:String) = {
    if(this._singleton ==null) _singleton = new NegativeWords
    _singleton
  }
  def getWords()={
    this.negWords
  }

}
