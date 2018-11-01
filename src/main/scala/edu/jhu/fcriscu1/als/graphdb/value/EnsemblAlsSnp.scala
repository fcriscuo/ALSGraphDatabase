package org.nygenome.als.graphdb.value

import org.apache.commons.csv.CSVRecord

case class EnsemblAlsSnp(
                          ensemblGeneId: String, ensemblTranscriptId: String,
                          variantId: String, hugoName: String, distance: Int,
                          alleleVariation: String
                        ) {
}

object EnsemblAlsSnp extends ValueTrait {
  val columnHeadings: Array[String] = Array("Gene stable ID", "Transcript stable ID"
    , "Variant name", "Gene name", "Distance to transcript"
    , "Variant alleles")

  def parseCSVRecord(record: CSVRecord): EnsemblAlsSnp = {
    new EnsemblAlsSnp(
      record.get(columnHeadings(0)),
      record.get(columnHeadings(1)),
      record.get(columnHeadings(2)),
      record.get(columnHeadings(3)),
      record.get(columnHeadings(4)).toInt,
      record.get(columnHeadings(5))
    )
  }
}

