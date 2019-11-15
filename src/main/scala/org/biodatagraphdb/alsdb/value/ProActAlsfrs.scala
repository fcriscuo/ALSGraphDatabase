package org.biodatagraphdb.alsdb.value

import org.apache.commons.csv.CSVRecord

case class ProActAlsfrs(
                         subjectId: String, subjectGuid: String,
                         q1Speech:Float, q2Salivation:Float, q3Swallowing:Float,
                         q4Handwriting:Float, q5aCuttingWithoutGastrostomy:Float,
                         q5bCuttingWithGastrostomy:Float,
                         q6Dressing:Float, q7TurningInBed:Float, q8Walking:Float,
                         q9ClimbingStairs: Float, q10Respiratory: Float,
                         alsfrsDelta: Int, alsfrsTotal: Float,
                         r1Dyspnea: Float, r2Orthopnea: Float, r3RespiratoryInsufficiency: Float,
                         modeOfAdministration: Int, respondedBy: Int

                       ) {
  val id: String = subjectGuid
  val subjectTuple: Tuple2[String, String] = Tuple2(subjectId, subjectGuid)

/*
There are records where the individual measurements have values but total
column is blank
 */
  val  alsTotal:Float = {
     if( alsfrsTotal > 0.0 ) alsfrsTotal
     else  q1Speech + q2Salivation + q3Swallowing +q4Handwriting + q5aCuttingWithoutGastrostomy +
          q5bCuttingWithGastrostomy +q6Dressing +q7TurningInBed +q8Walking + q9ClimbingStairs +
        q10Respiratory
  }

  val alsfrsrTotal:Float = alsTotal + r1Dyspnea + r2Orthopnea +r3RespiratoryInsufficiency

}

object ProActAlsfrs extends ValueTrait {


  val columnHeadings: Array[String] = Array("subject_id",
    "Q1_Speech", "Q2_Salivation", "Q3_Swallowing",
    "Q4_Handwriting", "Q5a_Cutting_without_Gastrostomy",
    "Q5b_Cutting_with_Gastrostomy", "Q6_Dressing_and_Hygiene",
    "Q7_Turning_in_Bed","Q8_Walking", "Q9_Climbing_Stairs",
    "Q10_Respiratory","ALSFRS_Delta",
    "ALSFRS_Total","ALSFRS_R_Total",
    "R_1_Dyspnea", "R_2_Orthopnea", "R_3_Respiratory_Insufficiency",
    "Mode_of_Administration", "ALSFRS_Responded_By"
  )

  def parseCSVRecord(record: CSVRecord): ProActAlsfrs = {
    ProActAlsfrs(
      record.get(columnHeadings(0)),
      generateProActGuid(record.get(columnHeadings(0)).toInt),
        validFloatingPointString(record.get(columnHeadings(1))),
        validFloatingPointString(record.get(columnHeadings(2))),
      validFloatingPointString(record.get(columnHeadings(3))),
      validFloatingPointString(record.get(columnHeadings(4))),
      validFloatingPointString(record.get(columnHeadings(5))),
      validFloatingPointString(record.get(columnHeadings(6))),
      validFloatingPointString(record.get(columnHeadings(7))),
      validFloatingPointString(record.get(columnHeadings(8))),
      validFloatingPointString(record.get(columnHeadings(9))),
      validFloatingPointString(record.get(columnHeadings(10))),
      validFloatingPointString(record.get(columnHeadings(11))),
      validIntegerString(record.get(columnHeadings(12))),
      validFloatingPointString(record.get(columnHeadings(13))),
      validFloatingPointString(record.get(columnHeadings(14))),
      validFloatingPointString(record.get(columnHeadings(15))),
      validFloatingPointString(record.get(columnHeadings(16))),
      validIntegerString(record.get(columnHeadings(17))),
      validIntegerString(record.get(columnHeadings(18)))
    )
  }

}