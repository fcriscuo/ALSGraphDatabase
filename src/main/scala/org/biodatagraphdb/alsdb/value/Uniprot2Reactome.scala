package org.biodatagraphdb.alsdb.value

import org.apache.commons.csv.CSVRecord

case class Uniprot2Reactome(uniProtId:String,
                            reactomeId:String,
                            url:String, eventName:String,
                            evidenceCode:String, species:String
                           ){

}

object Uniprot2Reactome extends ValueTrait {

  def parseCSVRecord (record:CSVRecord): Uniprot2Reactome= {
    new Uniprot2Reactome(
      record.get("UniProt_ID"), record.get("Reactome_ID"),
      record.get("URL"), record.get("Event_Name"),
      record.get("Evidence_Code"), record.get("Species")
    )
  }
}
