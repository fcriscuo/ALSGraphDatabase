package edu.jhu.fcriscu1.als.graphdb.value

import org.apache.commons.csv.CSVRecord

case class ProActAdverseEvent (subjectId:String ,
                               subjectGuid:String,
                               lowestLevelTerm:String,preferredTerm:String,
                               highLevelTerm:String,highLevelGroupTerm:String,
                               systemOrganClass:String,
                               socAbbreviation:String,
                               socCode:String,severity:String,outcome:String,
                               startDateDelta:Int,endDateDelta:Int){
  val id:String = subjectGuid +":" + preferredTerm + ":" + startDateDelta.toString
  val subjectTuple:Tuple2[String,String] = Tuple2(subjectId,subjectGuid)

}

object ProActAdverseEvent extends ValueTrait {
  val columnHeadings: Array[String] = Array("subject_id","Lowest_Level_Term","Preferred_Term",
    "High_Level_Term","High_Level_Group_Term","System_Organ_Class",
    "SOC_Abbreviation","SOC_Code","Severity","Outcome",
    "Start_Date_Delta","End_Date_Delta")

  def parseCSVRecord(record: CSVRecord): ProActAdverseEvent = {
    ProActAdverseEvent(
      record.get(columnHeadings(0)),
      generateProActGuid(record.get(columnHeadings(0)).toInt),
      record.get(columnHeadings(1)),
      record.get(columnHeadings(2)),
      record.get(columnHeadings(3)),
      record.get(columnHeadings(4)),
      record.get(columnHeadings(5)),
      record.get(columnHeadings(6)),
      record.get(columnHeadings(7)),
      record.get(columnHeadings(8)),
      record.get(columnHeadings(9)),
      validIntegerString(record.get(columnHeadings(10))),
      validIntegerString(record.get(columnHeadings(11)))
      )

  }
}