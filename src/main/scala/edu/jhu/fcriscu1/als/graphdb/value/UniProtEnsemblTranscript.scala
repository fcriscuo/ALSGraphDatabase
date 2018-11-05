package edu.jhu.fcriscu1.als.graphdb.value

import org.apache.commons.csv.CSVRecord

case class  UniProtEnsemblTranscript (
                                     uniprotId:String,
                                     geneDescription:String,
                                     geneSymbol:String,
                                     ensemblTranscriptId:String
                                     ){}

object UniProtEnsemblTranscript extends ValueTrait {
  def parseCSVRecord(record:CSVRecord): Option[UniProtEnsemblTranscript] = {
   def valid:Boolean = isEmpty(record.get(record.get("UniProtKB/Swiss-Prot ID")))
    valid match {
      case true => Some(UniProtEnsemblTranscript(
        record.get("UniProtKB/Swiss-Prot ID"),
        record.get("Gene description"),
        record.get("HGNC symbol"),
        record.get("Transcript stable ID")
      ))
      case false => None
    }

  }
}
