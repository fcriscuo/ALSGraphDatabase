package edu.jhu.fcriscu1.als.graphdb.util

class DynamicRelationshipTypes(var relType:String) extends org.neo4j.graphdb.RelationshipType{
  override def name(): String = relType
}
