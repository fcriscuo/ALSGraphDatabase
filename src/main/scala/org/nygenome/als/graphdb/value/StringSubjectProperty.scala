package org.nygenome.als.graphdb.value

import org.apache.commons.csv.CSVRecord

case class StringSubjectProperty (
               subjectId:Int, propertyName:String, propertyValue:String,
               externalSubjectId:String, externalSampleId:String,
               sampleId:Int,sampleType:String, analyteType:String)
{
}
object StringSubjectProperty extends ValueTrait {
  def parseCSVRecord(record: CSVRecord):StringSubjectProperty  ={
    new StringSubjectProperty(
      validIntegerString(record.get("subjectId")),
      record.get("propertyName"), record.get("propertyValue"),
      record.get("externalSubjectId"), record.get("externalSampleId"),
      record.get("sampleId").toInt, record.get("type"),
      record.get("analyteType")
    )
  }
  val columnHeadings:Array[String] = Array("subjectId","propertyName","propertyValue","externalSubjectId",
    "nygcSubjectId","externalSampleId","sampleId"
    ,"type","analyteType")
}