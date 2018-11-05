package edu.jhu.fcriscu1.als.graphdb.value

import org.apache.commons.csv.CSVRecord

case class ProActConMeds(
                        subjectId:String,
                        subjectGuid:String,
                        medication:String,
                        startDelta: Int, stopDelta:Int,
                        dose:Int, dosageUnits:String,
                        frequency:String , route:String
                        ) {
  val id:String = subjectGuid
  val subjectTuple:Tuple2[String,String] = Tuple2(subjectId,subjectGuid)
}
//subject_id,Medication_Coded,Start_Delta,Stop_Delta,Dose,Unit,Frequency,Route
object ProActConMeds extends ValueTrait {

  def parseCSVRecord(record:CSVRecord): ProActConMeds = {
    ProActConMeds( record.get("subject_id"),
    generateProActGuid(record.get("subject_id").toInt),
    record.get("Medication_Coded"),
    record.get("Start_Delta").toInt,
    record.get("Stop_Delta").toInt,
    record.get("Dose").toInt,
    record.get("Unit"),
    record.get("Frequency"),
    record.get("Route")
    )
  }

}