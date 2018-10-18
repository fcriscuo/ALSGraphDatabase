package org.nygenome.als.graphdb.value

import org.apache.commons.csv.CSVRecord

case class AlsodMutation (
                         mutationName:String, mutationCode:String,
                         gene:String, mutationType:String,  seqOriginal:String,
                         seqMutated:String, aaOriginal:String, aaMutated:String,
                         codonNumber:String, exonOrIntron:String,
                         exonOrIntronNumber:String, hgvsNucleotide:String,
                         hgvsProtein:String, chromosomeLocation:String,
                         dbSNP:String
                         ){
  val id:String = mutationCode
  val seqChange = seqOriginal +" -> " +seqMutated
  val aaChange = aaOriginal +" -> " +aaMutated
}


object AlsodMutation extends ValueTrait {
  val columnHeadings: Array[String] = Array(
  "Mutation name","Mutation code","Gene",	"Type","Seq. Original",
  "Seq. Mutated","AA. Original","AA. Mutated","Codon Number",
  "Exon/Intron","Exon/Intron Number",
  "HGVS_Nucleotide","HGVS_protein","Location(Chr)","dbSNP"
  )
  def parseCSVRecord(record:CSVRecord):AlsodMutation = {
    AlsodMutation(
      record.get(columnHeadings(0)),
      record.get(columnHeadings(1)),
      record.get(columnHeadings(2)),
      record.get(columnHeadings(3)),
      record.get(columnHeadings(4)),
      record.get(columnHeadings(5)),
      record.get(columnHeadings(6)),
      record.get(columnHeadings(7)),
      record.get(columnHeadings(8)),
      record.get(columnHeadings(9)),
      record.get(columnHeadings(10)),
      record.get(columnHeadings(11)),
      record.get(columnHeadings(12)),
      record.get(columnHeadings(13)),
      record.get(columnHeadings(14))
    )
  }

}
