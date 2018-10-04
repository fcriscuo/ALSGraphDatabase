package org.nygenome.als.graphdb.util

class DynamicReaaltionshipTypes(var relType:String) extends org.neo4j.graphdb.RelationshipType{
  override def name(): String = relType
}
