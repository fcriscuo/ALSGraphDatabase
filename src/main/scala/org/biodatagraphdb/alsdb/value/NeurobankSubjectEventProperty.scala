package org.biodatagraphdb.alsdb.value

import org.apache.commons.csv.CSVRecord

case class NeurobankSubjectEventProperty(
                                          subjectId :String, subjectGuid: String,
                                          timepointName:String, timepointId:String,
                                          eventCategory:String, formName:String,
                                          propertyCategory:String, propertyCode: String,
                                          propertyName:String, propertyValue:String
                                        ) {
 val eventValueId: String = subjectGuid +":" +timepointName +":" +propertyName
  val eventPropertyId = eventCategory +":" +propertyCode
  val timepointTuple:Tuple2[String,String] = new Tuple2(timepointId, timepointName)
  val subjectTuple:Tuple2[String,String] = new Tuple2(subjectId, subjectGuid)
  val subjectEventTuple:Tuple2[String,String] = new Tuple2(eventCategory, formName)
}

object NeurobankSubjectEventProperty extends ValueTrait {
  val columnHeadings:Array[String] = Array("subject_id"," subject_guid","timepoint_name","timepoint_id",
    "event_category","form_name","property_category","property_code","property_name","property_value")

  def parseCSVRecord(record:CSVRecord):NeurobankSubjectEventProperty = {
    NeurobankSubjectEventProperty(
      record.get("subject_id"),
      record.get("subject_guid"),
      record.get("timepoint_name"),
      record.get("timepoint_id"),
      record.get("event_category"),
      record.get("form_name"),
      record.get("property_category"),
      record.get("property_code"),
      record.get("property_name"),
      record.get("property_value")
    )
  }
}
