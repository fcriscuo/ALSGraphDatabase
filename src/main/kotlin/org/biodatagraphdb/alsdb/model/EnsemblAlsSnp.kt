package org.biodatagraphdb.alsdb.model

import org.apache.commons.csv.CSVRecord

/**
 * Created by fcriscuo on 2/4/20.
 */
data class EnsemblAlsSnp(
        val ensemblGeneId: String, val ensemblTranscriptId: String,
        val variantId: String, val hugoName: String, val distance: Int,
        val alleleVariation: String
) {
    companion object : AlsdbModel {
        private val columnHeadings: List<String> = listOf("Gene stable ID", "Transcript stable ID"
                , "Variant name", "Gene name", "Distance to transcript"
                , "Variant alleles")

        fun parseCSVRecord(record: CSVRecord): EnsemblAlsSnp =
                EnsemblAlsSnp(
                        record.get(columnHeadings[0]),
                        record.get(columnHeadings[1]),
                        record.get(columnHeadings[2]),
                        record.get(columnHeadings[3]),
                        record.get(columnHeadings[4]).toInt(),
                        record.get(columnHeadings[5])
                )

    }

}