package org.biodatagraphdb.alsdb.model

import org.apache.commons.csv.CSVRecord

/**
 * Created by fcriscuo on 2/4/20.
 */
data class GeneDiseaseAssociation(
        val geneId: Int, val geneSymbol: String, val diseaseId: String,
        val diseaseName: String, val score: Double, val nOfPmids: Int,
        val nOfSnps: Int, val source: String
) {
    companion object : AlsdbModel {
        fun parseCSVRecord( record: CSVRecord):GeneDiseaseAssociation =
             GeneDiseaseAssociation(record.get("geneId").toInt(), record.get("geneSymbol"),
            record.get("diseaseId"), record.get("diseaseName"),
            record.get("score").toDouble(), record.get("NofPmids").toInt(), record.get("NofSnps").toInt(),
            record.get("source")
            )
    }
}