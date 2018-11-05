package edu.jhu.fcriscu1.als.graphdb.value

import org.apache.commons.csv.CSVRecord

case class EnsemblGene (
              ensemblGeneId:String, ensemblTranscriptId:String, chromosome:String,
              geneStart:Int, geneEnd:Int, goEntry:GeneOntology,
             hugoSymbol:String, uniprotId:String,
              stand:Int
                       )
{

}

object EnsemblGene extends ValueTrait {

  val columnHeadings: Array[String] = Array(
    "Gene stable ID","Transcript stable ID","Chromosome/scaffold name","Gene start (bp)","Gene end (bp)",
    "GO term accession","GO term name","GO term definition","HGNC symbol","UniProtKB/Swiss-Prot ID","Strand",
    "GO domain"
  )

  def parseCSVRecord(record:CSVRecord):EnsemblGene = {
    EnsemblGene( record.get("Gene stable ID"),
      record.get("Transcript stable ID"),
      record.get("Chromosome/scaffold name"),
      record.get("Gene start (bp)").toInt,
      record.get("Gene end (bp)").toInt,
      new GeneOntology( record.get("GO term accession"),
        record.get("GO domain"),
        record.get("GO term name"),
        record.get("GO term definition")),
      record.get("HGNC symbol"),
      record.get("UniProtKB/Swiss-Prot ID"),
      record.get("Strand").toInt
    )
  }


}
