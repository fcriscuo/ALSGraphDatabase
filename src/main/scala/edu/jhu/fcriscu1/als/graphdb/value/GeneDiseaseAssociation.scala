package org.nygenome.als.graphdb.value

import org.apache.commons.csv.CSVRecord



case class GeneDiseaseAssociation ( geneId:Int, geneSymbol:String, diseaseId:String,
                                    diseaseName:String, score:Double, nOfPmids:Int,
                                    nOfSnps:Int, source:String)
{}

 object GeneDiseaseAssociation extends ValueTrait {
    def parseCSVRecord( record:CSVRecord):GeneDiseaseAssociation =  {
    new  GeneDiseaseAssociation(record.get("geneId").toInt, record.get("geneSymbol"),
       record.get("diseaseId"), record.get("diseaseName"),
       record.get("score").toDouble, record.get("NofPmids").toInt, record.get("NofSnps").toInt,
         record.get("source")
     )}
}
