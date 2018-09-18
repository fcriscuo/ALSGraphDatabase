package org.nygenome.als.graphdb.value

import java.util.Optional

import org.apache.commons.csv.CSVRecord
import org.nygenome.als.graphdb.service.UniProtMappingService

case class RnaTpmGene(
                       hugoGeneName:String, ensemblGeneId:String,
                       uniProtMapping: Optional[UniProtMapping],
                       tpm:Double, externalSampleId:String,
                       externalSubjectId:String
                     ) {

}
object RnaTpmGene extends ValueTrait {


  def parseCsvRecordFunction( record:CSVRecord):RnaTpmGene = {
    val ensemblGeneId:String = record.get("EnsemblGeneId")
    new RnaTpmGene(
      record.get("HugoGeneName"),
      ensemblGeneId,
      UniProtMappingService.INSTANCE.resolveUniProtMappingByEnsemblGeneId(ensemblGeneId),
      record.get("TPM").toDouble,
      record.get("ExternalSampleId"),
      record.get("ExternalSubjectId")
    )
  }
  val columnHeadings:Array[String] = Array("HugoGeneName","EnsemblGeneId", "TPM", "ExternalSampleId","ExternalSubjectId")

}
