package edu.jhu.fcriscu1.als.graphdb.value

import org.apache.commons.csv.CSVRecord

/*
  TSV file header
  snpId	diseaseId	diseaseName	score	NofPmids	source
   */

case class VariantDiseaseAssociation(
                 snpId:String,
                 diseaseId:String,
                 diseaseName:String,
                 score:Double,
                 nOfPmids:Int,
                 source:String
    ) {

}

object VariantDiseaseAssociation extends ValueTrait {
  def parseCSVRecord(record:CSVRecord): VariantDiseaseAssociation = {
    VariantDiseaseAssociation(
      record.get("snpId"),
      record.get("diseaseId"),
      record.get("diseaseName"),
      record.get("score").toDouble,
        record.get("NofPmids").toInt,
      record.get("source")
    )
  }

}