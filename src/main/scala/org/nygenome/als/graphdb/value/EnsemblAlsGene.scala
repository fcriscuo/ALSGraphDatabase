package org.nygenome.als.graphdb.value

import org.apache.commons.csv.CSVRecord

case class EnsemblAlsGene (
                          ensemblGeneId:String, ensemblTranscriptId:String,
                          chromosome:String, geneStart:Int, geneEnd:Int,
                          strand:String,
                          uniprotId:String, hugoName:String
                          ){
  var geneLength:Int = geneEnd - geneStart + 1;

}
object EnsemblAlsGene extends ValueTrait {
  val columnHeadings:Array[String] = Array("Gene stable ID","Transcript stable ID","Gene start (bp)",
    "Chromosome/scaffold name","Gene end (bp)","UniProtKB/Swiss-Prot ID",
    "Gene name","Strand")
  
  def parseCSVRecord(record: CSVRecord):EnsemblAlsGene = {
    new EnsemblAlsGene(
      record.get("Gene stable ID"),
      record.get("Transcript stable ID"),
      record.get("Chromosome/scaffold name"),
      record.get("Gene start (bp)").toInt,
      record.get("Gene end (bp)").toInt,
      record.get("Strand"),
      record.get("UniProtKB/Swiss-Prot ID"),
      record.get("Gene name")
    )
  }
}

