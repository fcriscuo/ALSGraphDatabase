package org.biodatagraphdb.alsdb.model

import org.apache.commons.csv.CSVRecord

/**
 * Created by fcriscuo on 2/4/20.
 */
data class EnsemblGene(
        val ensemblGeneId: String, val ensemblTranscriptId: String, val chromosome: String,
        val geneStart: Int, val geneEnd: Int, val goEntry: GeneOntology,
        val hugoSymbol: String, val uniprotId: String,
        val strand: Int
) {
    companion object : AlsdbModel {
        private val columnHeadings: List<String> = listOf(
                "Gene stable ID", "Transcript stable ID", "Chromosome/scaffold name", "Gene start (bp)", "Gene end (bp)",
                "GO term accession", "GO term name", "GO term definition", "HGNC symbol", "UniProtKB/Swiss-Prot ID", "Strand",
                "GO domain"
        )

        fun parseCSVRecord(record: CSVRecord): EnsemblGene =
                EnsemblGene(record.get(columnHeadings[0]),
                        record.get(columnHeadings[1]),
                        record.get(columnHeadings[2]),
                        record.get(columnHeadings[3]).toInt(),
                        record.get(columnHeadings[4]).toInt(),
                        GeneOntology(record.get(columnHeadings[5]),
                                record.get(columnHeadings[6]),
                                record.get(columnHeadings[7]),
                                record.get(columnHeadings[8])),
                        record.get(columnHeadings[9]),
                        record.get(columnHeadings[10]),
                        record.get(columnHeadings[11]).toInt()
                )

    }
}