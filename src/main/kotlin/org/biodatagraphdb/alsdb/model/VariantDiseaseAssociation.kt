package org.biodatagraphdb.alsdb.model

import org.apache.commons.csv.CSVRecord

/*
  TSV file header
  snpId	diseaseId	diseaseName	score	NofPmids	source
   */
data class VariantDiseaseAssociation(
        val snpId:String,
        val diseaseId:String,
        val diseaseName:String,
        val score:Double,
        val nOfPmids:Int,
        val source:String
) {
    companion object : AlsdbModel {
        fun parseCSVRecord(record: CSVRecord): VariantDiseaseAssociation =
            VariantDiseaseAssociation(
                    record.get("snpId"),
                    record.get("diseaseId"),
                    record.get("diseaseName"),
                    parseDoubleString(record.get("score")),
                    parseValidIntegerFromString(record.get("NofPmids")),
                    record.get("source")
            )
    }
}