package org.nygenome.als.graphdb.value

import org.apache.commons.csv.CSVRecord

case class NeurobankSubjectTimepoint(
                                    timepointId:Int,
                                    subjectId:Int,
                                    timepointName:String,
                                    timepoint:Int,
                                    parentTimepointId: Int,
                                    timepointInterval:Int,
                                    timepointEventId:Int,
                                    subjectGuid:String
                                    ) {
  val id:String = subjectGuid +":" +timepointName

}
object NeurobankSubjectTimepoint extends ValueTrait {
  def parseCSVRecord(record:CSVRecord):NeurobankSubjectTimepoint = {
    new NeurobankSubjectTimepoint(
      record.get("timepoint_id").toInt,
      record.get("subject_id").toInt,
      record.get("timepoint_name"),
      validIntegerString(record.get("timepoint")),
      validIntegerString(record.get("parent_timepoint_id")),
      validIntegerString(record.get("timepoint_interval")),
      record.get("timepoint_event_id").toInt,
      record.get("subject_guid")
    )
  }

}
