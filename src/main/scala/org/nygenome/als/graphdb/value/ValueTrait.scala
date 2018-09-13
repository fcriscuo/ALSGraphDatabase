package org.nygenome.als.graphdb.value

import scala.collection.JavaConverters._
trait ValueTrait {
  private val HUMAN_SPECIES = "Homo sapiens"
  protected var parseStringOnPipeFunction: Function[String, List[String]] = (s: String) => {
    s.split("\\|").toList.map(_.trim)
  }
  protected var parseStringOnColonFunction: Function[String, List[String]] = (s: String) => {
    s.split(":").toList.map(_.trim)
  }
  protected var parseStringOnSemiColonFunction: Function[String, List[String]] = (s: String) => {
    s.split(";").toList.map(_.trim)
  }
protected def isHuman(species:String):Boolean = species.trim().equalsIgnoreCase(HUMAN_SPECIES)

  class AsArrayList[T](input: List[T]) {
    def asArrayList : java.util.ArrayList[T] = new java.util.ArrayList[T](input.asJava)
  }

  protected var parseDoubleStringFunction: Function[String,Double] = (s:String) => {
    s.replace(',','.').toDouble
  }

  implicit def asArrayList[T](input: List[T]) = new AsArrayList[T](input)

  def isEmpty(x: String) = x == null || x.trim.isEmpty

  def isValidString(x:String):Boolean = x != null && x.trim.length> 0

  protected var reduceListToString: Function[List[String], String] =
    (list: List[String]) => list.mkString("|")



}
