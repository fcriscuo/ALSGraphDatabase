package org.biodatagraphdb.alsdb.model

import org.apache.commons.csv.CSVRecord

data class StringSubjectProperty(
        val subjectId: String, val propertyName: String, val propertyValue: String,
        val externalSubjectId: String, val externalSampleId: String,
        val sampleId: Int, val sampleType: String, val analyteType: String
) {
    val subjectTuple = Pair(subjectId,externalSubjectId)
    val columnHeadings = arrayOf("subjectId", "propertyName", "propertyValue", "externalSubjectId",
            "nygcSubjectId", "externalSampleId", "sampleId"
            , "type", "analyteType")

    companion object : AlsdbModel {
        fun parseCSVRecord(record: CSVRecord): StringSubjectProperty =
                StringSubjectProperty(
                        record.get("subjectId"),
                        record.get("propertyName"),
                        record.get("propertyValue"),
                        record.get("externalSubjectId"),
                        record.get("externalSampleId"),
                        parseValidIntegerFromString(record.get("sampleId")),
                        record.get("type"),
                        record.get("analyteType")
                )
    }
}