package org.biodatagraphdb.alsdb.model

import org.apache.commons.csv.CSVRecord

data class NeurobankSubjectEventProperty(
        val subjectId: String,
        val subjectGuid: String,
        val timepointName: String,
        val timepointId: String,
        val eventCategory: String,
        val formName: String,
        val propertyCategory: String,
        val propertyCode: String,
        val propertyName: String,
        val propertyValue: String
) {
    val eventValueId = "$subjectGuid:$timepointName:$propertyName"
    val eventPropertyId = "$eventCategory:$propertyCode"
    val timepointIdPair = Pair(timepointId, timepointName)
    val subjectIdPair = Pair(subjectId, subjectGuid)
    val subjectEventNamePair = Pair(eventCategory, formName)

    companion object : AlsdbModel {
        val columnHeadings = arrayOf("subject_id"," subject_guid","timepoint_name","timepoint_id",
        "event_category","form_name","property_category","property_code","property_name","property_value")
       fun  parseCSVRecord(record: CSVRecord):NeurobankSubjectEventProperty =
            NeurobankSubjectEventProperty(
                    record.get("subject_id"),
                    record.get("subject_guid"),
                    record.get("timepoint_name"),
                    record.get("timepoint_id"),
                    record.get("event_category"),
                    record.get("form_name"),
                    record.get("property_category"),
                    record.get("property_code"),
                    record.get("property_name"),
                    record.get("property_value")
            )
    }
}