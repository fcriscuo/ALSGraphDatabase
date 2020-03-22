package org.biodatagraphdb.alsdb.model

import org.apache.commons.csv.CSVRecord

data class ProActConMeds(
        val subjectId: String,
        val subjectGuid: String,
        val medication: String,
        val startDelta: Int, val stopDelta: Int,
        val dose: Int, val dosageUnits: String,
        val frequency: String, val route: String
) {
    val id: String = subjectGuid
    val subjectIdPair = Pair(subjectId, subjectGuid)

    companion object : AlsdbModel {
        fun parseCSVRecord(record: CSVRecord): ProActConMeds =
            ProActConMeds(record.get("subject_id"),
                    generateProActGuid(record.get("subject_id").toInt()),
                    record.get("Medication_Coded"),
                    parseValidIntegerFromString(record.get("Start_Delta")),
                   parseValidIntegerFromString(record.get("Stop_Delta")),
                    parseValidIntegerFromString(record.get("Dose")),
                    record.get("Unit"),
                    record.get("Frequency"),
                    record.get("Route")
            )
    }
}