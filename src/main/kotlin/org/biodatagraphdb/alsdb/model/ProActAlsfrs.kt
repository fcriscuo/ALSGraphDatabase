package org.biodatagraphdb.alsdb.model

import org.apache.commons.csv.CSVRecord

data class ProActAlsfrs(
        val subjectId: String, val subjectGuid: String,
        val q1Speech: Float, val q2Salivation: Float, val q3Swallowing: Float,
        val q4Handwriting: Float, val q5aCuttingWithoutGastrostomy: Float,
        val q5bCuttingWithGastrostomy: Float,
        val q6Dressing: Float, val q7TurningInBed: Float, val q8Walking: Float,
        val q9ClimbingStairs: Float, val q10Respiratory: Float,
        val alsfrsDelta: Int, val alsfrsPartialTotal: Float = 0.0F,
        val r1Dyspnea: Float, val r2Orthopnea: Float, val r3RespiratoryInsufficiency: Float,
        val modeOfAdministration: Int, val respondedBy: Int
) {
    // some entries have vales for the individual FRS measurements but
    // fo not include a partial total
    // in either case, 3 additional measurement values must be included
    val alsfrsrTotal = if (alsfrsPartialTotal > 0.0F){
        alsfrsPartialTotal + r1Dyspnea + r2Orthopnea +r3RespiratoryInsufficiency
    } else {
        q1Speech + q2Salivation + q3Swallowing +q4Handwriting + q5aCuttingWithoutGastrostomy +
                q5bCuttingWithGastrostomy +q6Dressing +q7TurningInBed +q8Walking + q9ClimbingStairs +
                q10Respiratory + r1Dyspnea + r2Orthopnea +r3RespiratoryInsufficiency
    }

    val id: String = subjectGuid
    val subjectIdPair = Pair(subjectId, subjectGuid)
    

    companion object : AlsdbModel {

        fun parseCSVRecord(record: CSVRecord): ProActAlsfrs =
            ProActAlsfrs(
                    record.get("subject_id"),
                    generateProActGuid(record.get("subject_id").toInt()),
                    parseValidFloatFromString(record.get("Q1_Speech")),
                    parseValidFloatFromString(record.get("Q2_Salivation")),
                    parseValidFloatFromString(record.get("Q3_Swallowing")),
                    parseValidFloatFromString(record.get("Q4_Handwriting")),
                    parseValidFloatFromString(record.get("Q5a_Cutting_without_Gastrostomy")),
                    parseValidFloatFromString(record.get("Q5b_Cutting_with_Gastrostomy")),
                    parseValidFloatFromString(record.get("Q6_Dressing_and_Hygiene")),
                    parseValidFloatFromString(record.get( "Q7_Turning_in_Bed")),
                    parseValidFloatFromString(record.get("Q8_Walking")),
                    parseValidFloatFromString(record.get("Q9_Climbing_Stairs")),
                    parseValidFloatFromString(record.get("Q10_Respiratory")),
                    parseValidIntegerFromString(record.get("ALSFRS_Delta")),
                    parseValidFloatFromString(record.get("ALSFRS_Total")),
                    parseValidFloatFromString(record.get("R_1_Dyspnea")),
                    parseValidFloatFromString(record.get("R_2_Orthopnea")),
                    parseValidFloatFromString(record.get("R_3_Respiratory_Insufficiency")),
                    parseValidIntegerFromString(record.get("Mode_of_Administration")),
                    parseValidIntegerFromString(record.get("ALSFRS_Responded_By"))
            )

    }
}