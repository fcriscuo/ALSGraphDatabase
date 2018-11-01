package org.nygenome.als.graphdb.value

import org.apache.commons.csv.CSVRecord

case class NeurobankSubjectProperty ( subjectId:String,
                                      subjectGuid:String,
                                      genomicDataFlag:Boolean,
                                      eventCategory:String,
                                      eventPropertyCode:String,
                                      eventPropertyName:String,
                                      eventPropertyValue:String
                                    ) {
  val id:String = subjectGuid +":" +eventCategory +":" +eventPropertyCode
  val subjectTuple:Tuple2[String,String] = Tuple2(subjectId,subjectGuid)

}

object NeurobankSubjectProperty extends ValueTrait {
  def parseCSVRecord(record:CSVRecord):NeurobankSubjectProperty = {
    new  NeurobankSubjectProperty(record.get("subject_id"),
    record.get("subject_guid"),
      booleanValueFromInt(record.get("genomic_data_flag").toInt),
      record.get("category"),
      record.get("property_code"),
      record.get("property_name"),
      record.get("property_value"))

  }
}
