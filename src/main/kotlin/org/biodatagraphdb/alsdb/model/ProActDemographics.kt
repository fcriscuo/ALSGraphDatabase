package org.biodatagraphdb.alsdb.model

import org.apache.commons.csv.CSVRecord

data class ProActDemographics(
        val subjectId: Int,
        val subjectGuid: String,
        val age: Int,
        val daysSinceBirth: Int, val Sex: String
) {
    val id: String = subjectGuid
    /* column names
TODO: add support for determining Race attribute
subject_id,Demographics_Delta,Age,Date_of_Birth,
Ethnicity,Race_Americ_Indian_Alaska_Native,Race_Asian,
Race_Black_African_American,Race_Hawaiian_Pacific_Islander,
Race_Unknown,Race_Caucasian,Race_Other,Race_Other_Specify,
Sex
 */

    companion object : AlsdbModel {
        fun parseCSVRecord(record: CSVRecord): ProActDemographics =
                ProActDemographics(
                        record.get("subject_id").toInt(),
                        generateProActGuid(record.get("subject_id").toInt()),
                        parseValidIntegerFromString(record.get("Age")),
                        parseValidIntegerFromString(record.get("Date_of_Birth")),
                        record.get("Sex")
                )
    }
}