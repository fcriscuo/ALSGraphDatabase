package org.biodatagraphdb.alsdb.model

import org.apache.commons.csv.CSVRecord

/**
 * Created by fcriscuo on 2/4/20.
 */
data class EnsemblAlsGene(
        val ensemblGeneId: String, val ensemblTranscriptId: String,
        val chromosome: String, val geneStart: Int, val geneEnd: Int,
        val strand: String,
        val uniprotId: String, val hugoName: String) {
    val geneLength: Int = geneEnd - geneStart + 1

    companion object : AlsdbModel {

        val columnHeadings: List<String> = listOf("Gene stable ID", "Transcript stable ID", "Gene start (bp)",
                "Chromosome/scaffold name", "Gene end (bp)", "UniProtKB/Swiss-Prot ID",
                "Gene name", "Strand")

        fun parseCSVRecord(record: CSVRecord): EnsemblAlsGene =
                EnsemblAlsGene(
                        record.get(columnHeadings[0]),
                        record.get(columnHeadings[1]),
                        record.get(columnHeadings[2]),
                        record.get(columnHeadings[3]).toInt(),
                        record.get(columnHeadings[4]).toInt(),
                        record.get(columnHeadings[5]),
                        record.get(columnHeadings[6]),
                        record.get(columnHeadings[7]))

    }
}