package org.biodatagraphdb.alsdb.model

import org.apache.commons.csv.CSVRecord

/**
 * Created by fcriscuo on 2/3/20.
 * Model object for entries in Alsod_Mutation_Data.txt
 */

data class AlsodMutation(
        val mutationName: String, val mutationCode: String,
        val gene: String, val mutationType: String, val seqOriginal: String,
        val seqMutated: String, val aaOriginal: String, val aaMutated: String,
        val codonNumber: String, val exonOrIntron: String,
        val exonOrIntronNumber: String, val hgvsNucleotide: String,
        val hgvsProtein: String, val chromosomeLocation: String,
        val dbSNP: String) {
    val id = mutationCode
    val seqChange = "$seqOriginal -> $seqMutated"
    val aaChange = "$aaOriginal -> $aaMutated"

    companion object : AlsdbModel {
        val columnHeadings = listOf("Mutation name", "Mutation code", "Gene", "Type", "Seq. Original",
                "Seq. Mutated", "AA. Original", "AA. Mutated", "Codon Number",
                "Exon/Intron", "Exon/Intron Number",
                "HGVS_Nucleotide", "HGVS_protein", "Location(Chr)", "dbSNP")

        fun parseCSVRecord(record: CSVRecord): AlsodMutation =
                AlsodMutation(
                        record.get(columnHeadings[0]),
                        record.get(columnHeadings[1]),
                        record.get(columnHeadings[2]),
                        record.get(columnHeadings[3]),
                        record.get(columnHeadings[4]),
                        record.get(columnHeadings[5]),
                        record.get(columnHeadings[6]),
                        record.get(columnHeadings[7]),
                        record.get(columnHeadings[8]),
                        record.get(columnHeadings[9]),
                        record.get(columnHeadings[10]),
                        record.get(columnHeadings[11]),
                        record.get(columnHeadings[12]),
                        record.get(columnHeadings[13]),
                        record.get(columnHeadings[14]))
    }

}