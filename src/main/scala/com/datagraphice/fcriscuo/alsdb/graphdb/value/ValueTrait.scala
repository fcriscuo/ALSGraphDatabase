package com.datagraphice.fcriscuo.alsdb.graphdb.value

import javax.annotation.RegEx

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
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

  protected var parseOntologyListFunction: Function[String,List[String]] = (s:String) => {
    var results = new ListBuffer[String]
    for ( w <- parseStringOnPipeFunction(s)) {
      results +=  w.substring(w.indexOf('(')+1, w.length-1)
    }
    results.toList
  }
 def isHuman(species:java.lang.String):Boolean = species.trim().equalsIgnoreCase(HUMAN_SPECIES)

  class AsArrayList[T](input: List[T]) {
    def asArrayList : java.util.ArrayList[T] = new java.util.ArrayList[T](input.asJava)
  }

  protected var parseDoubleStringFunction: Function[String,Double] = (s:String) => {
    s.replace(',','.').toDouble
  }

  implicit def asArrayList[T](input: List[T]) = new AsArrayList[T](input)

  def isEmpty(x: String) = x == null || x.trim.isEmpty

  def isValidString(x:String):Boolean = x != null && x.trim.length> 0

  def booleanValueFromInt (x:Int):Boolean = x == 1

  protected var reduceListToString: Function[List[String], String] =
    (list: List[String]) => list.mkString("|")

  val onlyDigitsRegex = "^\\d+$".r

   val floatingPointRegEx = "[-+]?[0-9]*\\.?[0-9]+".r

  def validFloatingPointString(s:String):Float = s match {
    case floatingPointRegEx() => s.toFloat
    case _ => "0".toFloat
  }

  // filter out non-numeric values in numeric field
  def validIntegerString(s:String):Int = s match {
    case onlyDigitsRegex() => s.toInt
    case _ => 0
  }

  def generateProActGuid(id:Int):String = {
    "PROACT" + "%08d".format(id)
  }




}
