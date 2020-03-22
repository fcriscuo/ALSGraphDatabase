package org.biodatagraphdb.alsdb.model

import org.apache.commons.csv.CSVRecord
import org.biodatagraphdb.alsdb.service.UniProtMappingService
import java.util.*

/**
 * Created by fcriscuo on 2/4/20.
 */
data class HumanTissueAtlas(
        val ensemblGeneId: String, val geneName: String, val tissue: String,
        val cellType: String, val level: String, val reliability: String,
        val ensemblTranscriptId: String, val uniprotId: String
) {
    val tissueCellTypeLabel = "$tissue:$cellType"
    val proteinTissueKey: Pair<String, String> =
            Pair<String, String>(uniprotId, tissueCellTypeLabel)

    val isReliable:Boolean = reliability.equals("Approved", ignoreCase = true) ||
            reliability.equals("Supported", ignoreCase = true)

    companion object : AlsdbModel {
         fun resolveUniprotId(geneId: String): String {
            val uniOpt: Optional<UniProtMapping> = UniProtMappingService.INSTANCE.resolveUniProtMappingByEnsemblGeneId(geneId)
            return if (uniOpt.isPresent) {
                uniOpt.get().uniProtId
            } else {
                ""
            }
        }

        fun resolveTranscriptId(geneId: String): String {
            val uniOpt: Optional<UniProtMapping> = UniProtMappingService.INSTANCE.resolveUniProtMappingByEnsemblGeneId(geneId)
            return if ((uniOpt.isPresent)) {
                uniOpt.get().ensemblGeneId
            } else {
                ""
            }
        }

        fun parseCSVRecord(record: CSVRecord): HumanTissueAtlas =
                HumanTissueAtlas(record.get("Gene"), record.get("Gene name"),
                        record.get("Tissue"), record.get("Cell type"),
                        record.get("Level"), record.get("Reliability"),
                        resolveTranscriptId(record.get("Gene")),
                        resolveUniprotId(record.get("Gene"))
                )
    }

}