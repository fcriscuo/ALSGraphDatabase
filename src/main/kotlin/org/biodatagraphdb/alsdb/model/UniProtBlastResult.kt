package org.biodatagraphdb.alsdb.model

import org.apache.commons.csv.CSVRecord

data class UniProtBlastResult(
        val sourceUniprotId: String,
        val hitUniprotId: String,
        val score: Double,
        val eValue: String // no direct way to store exponental data
) {


    companion object : AlsdbModel {
        val columnHeadings = arrayOf("sourceNumber", "sourceId", "sourceLength", "sourcePercentIdentity",
                "hitId", "hitLength", "hitPercentIdentity", "score", "eValue", "x")

        fun parseCSVRecord(record: CSVRecord): UniProtBlastResult =
                UniProtBlastResult(
                        parseStringOnPipe(record.get("sourceId"))[0],
                        record.get("hitId"), record.get("score").toDouble(),
                        record.get("eValue")
                )
    }
}