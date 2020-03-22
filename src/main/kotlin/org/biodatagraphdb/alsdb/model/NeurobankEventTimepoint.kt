package org.biodatagraphdb.alsdb.model

import org.apache.commons.csv.CSVRecord

data class NeurobankEventTimepoint(
        val timepointId: String,
        val subjectId: String,
        val timepointName: String,
        val timepoint: Int,
        val parentTimepointId: String,
        val timepointInterval: Int,
        val timepointEventId: Int,
        val subjectGuid: String
) {
    val id: String = timepointName
    val subjectIdPair = Pair(subjectId, subjectGuid)
    val timepointIdPair = Pair(timepointId, timepointName)

    companion object : AlsdbModel {
        fun parseCSVRecord(record: CSVRecord): NeurobankEventTimepoint =
                NeurobankEventTimepoint(
                        record.get("timepoint_id"),
                        record.get("subject_id"),
                        record.get("timepoint_name"),
                        parseValidIntegerFromString(record.get("timepoint")),
                        record.get("parent_timepoint_id"),
                        parseValidIntegerFromString(record.get("timepoint_interval")),
                        record.get("timepoint_event_id").toInt(),
                        record.get("subject_guid")
                )
    }
}