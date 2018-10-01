package org.nygenome.als.graphdb.value

import org.apache.commons.csv.CSVRecord


case class UniProtValue (
                          uniprotId:String, uniprotName:String, proteinNameList:List[String],
                          geneNameList:List[String],
                          pathwayList:List[String],
                          interactionList:List[String], goBioProcessList: List[String],
                          goCellComponentList:List[String], dieaseList:List[String],
                          tissueList:List[String], goMolFuncList:List[String],
                          phenotype:String, drugBankIdList:List[String], reactomeIdList:List[String],
                          pubMedIdList:List[String], mass:Double, length:Int, ensemblTranscriptList:List[String]
                        ) {

}

object UniProtValue extends ValueTrait {

  def defaultFilePath:java.nio.file.Path = java.nio.file.Paths.get("/data/als/uniprot-filtered-human.tsv")

  def parseCSVRecord(record:CSVRecord):UniProtValue = {
    UniProtValue( record.get("Entry"),
      record.get("Entry name"),
      parseStringOnSemiColonFunction.apply(record.get("Protein names")),
      parseStringOnSemiColonFunction.apply(record.get("Gene names")),
      parseStringOnSemiColonFunction.apply(record.get("Pathway")),
      parseStringOnSemiColonFunction.apply(record.get("Interacts with")),
      parseStringOnSemiColonFunction.apply(record.get("Gene ontology (biological process)")),
      parseStringOnSemiColonFunction.apply(record.get("Gene ontology (cellular component)")),
      parseStringOnSemiColonFunction.apply(record.get("Involvement in disease")),
      parseStringOnSemiColonFunction.apply(record.get("Tissue specificity")),
      parseStringOnSemiColonFunction.apply(record.get("Gene ontology (molecular function)")),
      record.get("Disruption phenotype"),
      parseStringOnSemiColonFunction.apply(record.get("Cross-reference (DrugBank)")),
      parseStringOnSemiColonFunction.apply(record.get("Cross-reference (Reactome)")),
      parseStringOnSemiColonFunction.apply(record.get("PubMed ID")),
      parseDoubleStringFunction.apply(record.get("Mass")),
      record.get("Length").toInt
      , parseStringOnSemiColonFunction.apply(record.get("Ensembl transcript"))
    )
  }
}