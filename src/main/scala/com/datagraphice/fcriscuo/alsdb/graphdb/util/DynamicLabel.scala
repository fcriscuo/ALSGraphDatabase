package com.datagraphice.fcriscuo.alsdb.graphdb.util

class DynamicLabel (var label:String) extends org.neo4j.graphdb.Label{

  override def name(): String = label
}
