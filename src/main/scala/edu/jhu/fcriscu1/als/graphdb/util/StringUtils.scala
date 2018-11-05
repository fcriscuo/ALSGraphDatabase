package edu.jhu.fcriscu1.als.graphdb.util

import java.util

object StringUtils {

  def parseGeneOntologyEntry(goEntry:String):Tuple2[String,String] = {
    val index:Int = goEntry.indexOf('[')+1
    val index2 = Math.max(0,index-1)
    new Tuple2[String,String](goEntry.substring(0,index2).trim,
      goEntry.slice(index, index+10 ))
  }
  object JFunction {
    def fun[T1, R](g: JFunction1[T1, R]): T1 => R = new (T1 => R) {
      override def apply(t: T1) = g.apply(t)
    }
  }

  def convertToJavaString(scalaList:List[String]):java.util.List[String]  = {
    val  jList:java.util.List[String] =new util.ArrayList[String]()
    val iter = scalaList.iterator
    while (iter.hasNext)
      {jList.add(iter.next())}
    jList
  }


}
