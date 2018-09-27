package org.nygenome.als.graphdb.value

import org.apache.commons.csv.CSVRecord

case class Pathway(uniprotId:String, reactomeId: String, eventName:String, evidenceCode:String,
                   species:String, id:String) {
}

object Pathway extends ValueTrait {
  val pathwayHeadings:String = "UniProt_ID\tReactome_ID\tURL\tEvent_Name\tEvidence_Code\tSpecies"
  val reactomeBaseUrl:String ="https://reactome.org/PathwayBrowser/#/"

  def parseCSVRecord(record: CSVRecord): Pathway = {
      new Pathway(
        record.get("UniProt_ID"),
        record.get("Reactome_ID"),
        record.get("Event_Name"),
        record.get("Evidence_Code"),
        record.get("Species"),
        record.get("UniProt_ID") +":" +record.get("Reactome_ID")
      )
  }
}