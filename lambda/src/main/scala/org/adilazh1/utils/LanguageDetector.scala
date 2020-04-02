package org.adilazh1.utils

import org.apache.tika.language.LanguageIdentifier

object LanguageDetector {

  def isEnglish(s:String):Boolean ={
    val detector = new LanguageIdentifier(s)
    detector.getLanguage.equals("en")
  }

}
