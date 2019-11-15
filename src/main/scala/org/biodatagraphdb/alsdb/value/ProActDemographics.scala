package org.biodatagraphdb.alsdb.value

import org.apache.commons.csv.CSVRecord

case class ProActDemographics(
                             subjectId:Int,
                             subjectGuid:String,
                             age:Int,
                             daysSinceBirth:Int,Sex:String
                             ) {
  val id:String = subjectGuid

}
/* column names
TODO: add support for determining Race attribute
subject_id,Demographics_Delta,Age,Date_of_Birth,
Ethnicity,Race_Americ_Indian_Alaska_Native,Race_Asian,
Race_Black_African_American,Race_Hawaiian_Pacific_Islander,
Race_Unknown,Race_Caucasian,Race_Other,Race_Other_Specify,
Sex
 */
object ProActDemographics extends ValueTrait {
  def parseCSVRecord(record:CSVRecord):ProActDemographics = {
    ProActDemographics(
      record.get("subject_id").toInt,
      generateProActGuid(record.get("subject_id").toInt),
      validIntegerString(record.get("Age")),
      validIntegerString(record.get("Date_of_Birth")),
      record.get("Sex")
    )
  }
}
