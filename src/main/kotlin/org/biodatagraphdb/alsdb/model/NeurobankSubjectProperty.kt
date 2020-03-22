package org.biodatagraphdb.alsdb.model

import org.apache.commons.csv.CSVRecord

data class NeurobankSubjectProperty(
        val subjectId: String,
        val subjectGuid: String,
        val genomicDataFlag: Boolean,
        val eventCategory: String,
        val eventPropertyCode: String,
        val eventPropertyName: String,
        val eventPropertyValue: String
) {
    val id = "$subjectGuid:$eventCategory:$eventPropertyCode"
    val subjectIdPair = Pair(subjectId, subjectGuid)

    companion object : AlsdbModel {
        fun parseCSVRecord(record: CSVRecord): NeurobankSubjectProperty =
                NeurobankSubjectProperty(record.get("subject_id"),
                        record.get("subject_guid"),
                        booleanFromInt(record.get("genomic_data_flag").toInt()),
                        record.get("category"),
                        record.get("property_code"),
                        record.get("property_name"),
                        record.get("property_value"))
    }
}