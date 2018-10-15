package org.nygenome.als.graphdb.value

import org.nygenome.als.graphdb.util.StringUtils

case class GeneOntology (
                          goTermAccession:String,
                          goDomain:String,
                          goName:String,
                          goDefinition:String
                        ){

}
object GeneOntology  extends ValueTrait {
  // method to parse a gene ontology entry from the uniprot file
  def parseGeneOntologyEntry(aspect: String, entry:String):GeneOntology ={
    val tuple2:Tuple2[String,String]  = StringUtils.parseGeneOntologyEntry(entry)
    GeneOntology(tuple2._2, aspect, tuple2._1," ")
  }
}
