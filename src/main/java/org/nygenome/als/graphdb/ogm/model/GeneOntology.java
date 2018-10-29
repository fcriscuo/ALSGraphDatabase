package org.nygenome.als.graphdb.ogm.model;

import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVRecord;
import org.neo4j.ogm.annotation.GeneratedValue;
import org.neo4j.ogm.annotation.Id;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Property;
import org.nygenome.als.graphdb.ogm.Entity;

@NodeEntity
public class GeneOntology extends Entity {
  @Id
  @GeneratedValue
  private Long id;

  @Property(name="accession")
  private String accession;

  @Property(name="domain")
  private String domain;

  @Property(name="name")
  private String name;

  @Property(name="definition")
  private String definition;

   public GeneOntology(@Nonnull org.nygenome.als.graphdb.value.GeneOntology go) {
     this.accession = go.goTermAccession();
     this.domain = go.goDomain();
     this.name = go.goName();
     this.definition = go.goDefinition();
   }


}