package org.nygenome.als.graphdb.value

import org.apache.commons.csv.CSVRecord
import scala.collection.JavaConverters._

case class UniProtDrugModel(id:String,name:String, geneName:String,
                            genbankProteinId:String, genbankGeneId:String,
                            uniprotId:String,  uniprotTitle:String, pdbId:String,
                            geneCardId:String, geneAtlasId:String, hgncId:String,
                            species:String, drugIdList:java.util.List[String]
                           ) {

}
object UniProtDrugModel extends ValueTrait {

  def parseCSVRecord(record:CSVRecord): UniProtDrugModel = {
      UniProtDrugModel(record.get("ID"),  record.get("Name"),
        record.get("Gene Name"), record.get("GenBank Protein ID"),
        record.get("GenBank Gene ID"),record.get("UniProt ID"),
        record.get("Uniprot Title"), record.get("PDB ID"),
        record.get("GeneCard ID"), record.get("GenAtlas ID"),
        record.get("HGNC ID"), record.get("Species"),
        parseStringOnSemiColonFunction.apply(record.get("Drug IDs")).asJava
      )
  }
}
