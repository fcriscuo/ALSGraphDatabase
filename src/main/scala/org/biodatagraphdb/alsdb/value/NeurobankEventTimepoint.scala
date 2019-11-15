package org.biodatagraphdb.alsdb.value

import org.apache.commons.csv.CSVRecord

case class NeurobankEventTimepoint(
                                    timepointId:String,
                                    subjectId:String,
                                    timepointName:String,
                                    timepoint:Int,
                                    parentTimepointId: String,
                                    timepointInterval:Int,
                                    timepointEventId:Int,
                                    subjectGuid:String
                                    ) {
  val id:String = timepointName
  val subjectTuple:Tuple2[String,String] = new Tuple2(subjectId,subjectGuid)
  val timepointTuple:Tuple2[String,String] = new Tuple2(timepointId, timepointName)
}
object NeurobankEventTimepoint extends ValueTrait {
  def parseCSVRecord(record:CSVRecord):NeurobankEventTimepoint = {
    new NeurobankEventTimepoint(
      record.get("timepoint_id"),
      record.get("subject_id"),
      record.get("timepoint_name"),
      validIntegerString(record.get("timepoint")),
      record.get("parent_timepoint_id"),
      validIntegerString(record.get("timepoint_interval")),
      record.get("timepoint_event_id").toInt,
      record.get("subject_guid")
    )
  }

}
