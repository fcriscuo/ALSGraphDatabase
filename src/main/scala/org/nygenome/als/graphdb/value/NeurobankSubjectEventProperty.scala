package org.nygenome.als.graphdb.value

import org.apache.commons.csv.CSVRecord

case class NeurobankSubjectEventProperty(
                                        subjectId : Int, subjectGuid: String,
                                        timepointName:String, eventId:Int,
                                        eventCategory:String, formName:String,
                                        propertyCategory:String, propertyCode: String,
                                        propertyName:String, propertyValue:String
                                        ) {
 val eventValueId: String = subjectGuid +":" +timepointName +":" +propertyName
  val eventPropertyId = eventCategory +":" +propertyCode
}
/*
subject_id	subject_guid	timepoint_name	timepoint_id	event_category	form_name	property_category	property_code	property_name	property_value
 */
object NeurobankSubjectEventProperty extends ValueTrait {
  val columnHeadings:Array[String] = Array("subject_id"," subject_guid","timepoint_name","timepoint_id",
    "event_category","form_name","property_category","property_code","property_name","property_value")

  def parseCSVRecord(record:CSVRecord):NeurobankSubjectEventProperty = {
    NeurobankSubjectEventProperty(
      record.get("subject_id").toInt,
      record.get("subject_guid"),
      record.get("timepoint_name"),
      record.get("timepoint_id").toInt,
      record.get("event_category"),
      record.get("form_name"),
      record.get("property_category"),
      record.get("property_code"),
      record.get("property_name"),
      record.get("property_value")
    )
  }
}
