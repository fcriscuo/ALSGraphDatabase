package edu.jhu.fcriscu1.als.graphdb.util;


import javax.annotation.Nonnull;
import org.neo4j.graphdb.RelationshipType;
/*
Public class that implements the Neo4j RelationshipType interface
Allows for creating dynamic RealtionshipTypes based on the contents
of source data
 */
public class DynamicRelationshipTypeO implements RelationshipType {

  private final String relationshipType;
  public DynamicRelationshipTypeO(@Nonnull String aType) {
    //TODO: validate parameter against a controlled vocabulary
    this.relationshipType = aType;
  }

  @Override
  public String name() {
    return this.relationshipType;
  }
}
