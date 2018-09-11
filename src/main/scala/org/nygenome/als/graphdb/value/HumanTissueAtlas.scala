package org.nygenome.als.graphdb.value

import java.util.Optional
import org.apache.commons.csv.CSVRecord
import org.nygenome.als.graphdb.service.UniProtMappingService
/*
TSV record header
Gene	Gene name	Tissue	Cell type	Level	Reliability
*/

case class HumanTissueAtlas(ensemblGeneId: String, geneName: String, tissue: String,
                          cellType: String, level: String, reliability: String,
                          ensemblTranscriptId: String, uniprotId: String
                         ) {
var resolveTissueCellTypeLabel: String = tissue + ":" + cellType
var proteinTissueKey: Tuple2[String, String] =
  new Tuple2[String, String](uniprotId, resolveTissueCellTypeLabel)

}


object HumanTissueAtlas extends ValueTrait {

private def resolveUniprotId(geneId: String): String = {
  val uniOpt: Optional[UniProtMapping] = UniProtMappingService.INSTANCE.resolveUniProtMappingByEnsemblGeneId(geneId)
  return if ((uniOpt.isPresent)) {
    uniOpt.get.uniPotId
  }
  else {
    ""
  }
}

private def resolveTranscriptId(geneId: String): String = {
  val uniOpt: Optional[UniProtMapping] = UniProtMappingService.INSTANCE.resolveUniProtMappingByEnsemblGeneId(geneId)
  return if ((uniOpt.isPresent)) {
    uniOpt.get.ensemblGeneId
  }
  else {
    ""
  }
}

def parseCSVRecord(record: CSVRecord): HumanTissueAtlas = {
  HumanTissueAtlas(record.get("Gene"), record.get("Gene name"),
    record.get("Tissue"), record.get("Cell type"),
    record.get("Level"), record.get("Reliability"),
    resolveTranscriptId(record.get("Gene")),
    resolveUniprotId(record.get("Gene"))
  )
}
}
