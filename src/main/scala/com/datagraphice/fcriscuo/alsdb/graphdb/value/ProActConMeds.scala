package edu.jhu.fcriscu1.als.graphdb.value

import edu.jhu.fcriscu1.als.graphdb.value.ProActAdverseEvent.validIntegerString
import org.apache.commons.csv.CSVRecord

case class ProActConMeds(
                          subjectId: String,
                          subjectGuid: String,
                          medication: String,
                          startDelta: Int, stopDelta: Int,
                          dose: Int, dosageUnits: String,
                          frequency: String, route: String
                        ) {
  val id: String = subjectGuid
  val subjectTuple: Tuple2[String, String] = Tuple2(subjectId, subjectGuid)
}

//subject_id,Medication_Coded,Start_Delta,Stop_Delta,Dose,Unit,Frequency,Route
object ProActConMeds extends ValueTrait {

  def parseCSVRecord(record: CSVRecord): ProActConMeds = {
    ProActConMeds(record.get("subject_id"),
      generateProActGuid(record.get("subject_id").toInt),
      record.get("Medication_Coded"),
      validIntegerString(record.get("Start_Delta")),
      validIntegerString(record.get("Stop_Delta")),
      validIntegerString(record.get("Dose")),
      record.get("Unit"),
      record.get("Frequency"),
      record.get("Route")
    )
  }

}