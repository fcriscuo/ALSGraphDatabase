package org.biodatagraphdb.alsdb.lib

class DynamicLabel (var label:String): org.neo4j.graphdb.Label{
    override fun name(): String = label
}

class DynamicRelationshipTypes(var relType:String): org.neo4j.graphdb.RelationshipType{
    override fun name(): String = relType
}
