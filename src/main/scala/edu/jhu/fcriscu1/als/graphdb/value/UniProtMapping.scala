package org.nygenome.als.graphdb.value

import org.apache.commons.csv.CSVRecord

case class UniProtMapping(val uniProtId:String, val ensemblGeneId:String, val ensemblTranscriptId:String,
                          val geneSymbol:String){

}
object UniProtMapping {

  def parseCsvRecordFunction( record:CSVRecord):UniProtMapping = {
    UniProtMapping(record.get("UniProtKB/Swiss-Prot ID"),
      record.get("Gene stable ID"),
      record.get("Transcript stable ID"),
      record.get("HGNC symbol")
    )
  }
}
