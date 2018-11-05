package edu.jhu.fcriscu1.als.graphdb.util

class DynamicLabel (var label:String) extends org.neo4j.graphdb.Label{

  override def name(): String = label
}
