package org.biodatagraphdb.alsdb.model

import org.apache.commons.csv.CSVRecord

data class NeurobankSubjectTimepointProperty(
        val timepointEventId: Int,
        val propertyCategory: String,
        val propertyCode: String,
        val propertyName: String,
        val propertyValue: String,
        val timepointId: Int
) {
    companion object : AlsdbModel {
        fun parseCSVRecord(record: CSVRecord): NeurobankSubjectTimepointProperty =
                NeurobankSubjectTimepointProperty(
                        parseValidIntegerFromString(record.get("timepoint_event_id")),
                        record.get("property_category"),
                        record.get("property_code"),
                        record.get("property_name"),
                        record.get("property_value"),
                        parseValidIntegerFromString(record.get("timepoint_id"))
                )
    }
}