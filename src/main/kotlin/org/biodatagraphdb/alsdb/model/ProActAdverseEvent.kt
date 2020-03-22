package org.biodatagraphdb.alsdb.model

import org.apache.commons.csv.CSVRecord

/**
 * Created by fcriscuo on 2/5/20.
 */
data class ProActAdverseEvent(val subjectId:String ,
                              val subjectGuid:String,
                              val lowestLevelTerm:String, val preferredTerm:String,
                              val highLevelTerm:String,val highLevelGroupTerm:String,
                              val systemOrganClass:String,
                              val socAbbreviation:String,
                              val socCode:String,val severity:String,val outcome:String,
                              val startDateDelta:Int,val endDateDelta:Int)

{
    val id:String = "$subjectGuid:$preferredTerm:$startDateDelta"
    val subjectTuple = Pair(subjectId,subjectGuid)

    companion object : AlsdbModel {
        fun parseCSVRecord(record: CSVRecord): ProActAdverseEvent =
                ProActAdverseEvent(
                        record.get("subject_id"),
                        generateProActGuid(record.get("subject_id").toInt()),
                        record.get("Lowest_Level_Term"),
                        record.get("Preferred_Term"),
                        record.get("High_Level_Term"),
                        record.get("High_Level_Group_Term"),
                        record.get("System_Organ_Class"),
                        record.get("SOC_Abbreviation"),
                        record.get("SOC_Code"),
                        record.get("Severity"),
                        record.get("Outcome"),
                        parseValidIntegerFromString(record.get("Start_Date_Delta")),
                        parseValidIntegerFromString(record.get("End_Date_Delta"))
                )

        }
}