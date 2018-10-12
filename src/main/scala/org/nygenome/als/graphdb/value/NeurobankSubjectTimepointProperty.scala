package org.nygenome.als.graphdb.value

import org.apache.commons.csv.CSVRecord

case class NeurobankSubjectTimepointProperty(
                                    timepointEventId:Int,
                                    propertyCategory:String,
                                    propertyCode:String,
                                    propertyName:String,
                                    propertyValue:String,
                                    timepointId:Int
                                       ) {
  val id:String = propertyCategory + ":" +propertyCode

}
object NeurobankSubjectTimepointProperty extends ValueTrait {
  def parseCSVRecord(record:CSVRecord):NeurobankSubjectTimepointProperty = {
    new NeurobankSubjectTimepointProperty(
      record.get("timepoint_event_id").toInt,
      record.get("property_category"),
      record.get("property_code"),
      record.get("property_name"),
      record.get("property_value"),
      record.get("timepoint_id").toInt
    )
  }
}

