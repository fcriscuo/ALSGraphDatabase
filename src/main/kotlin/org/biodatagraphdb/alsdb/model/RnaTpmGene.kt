package org.biodatagraphdb.alsdb.model

import org.apache.commons.csv.CSVRecord
import org.biodatagraphdb.alsdb.service.UniProtMappingService
import java.util.*

data class RnaTpmGene(
        val hugoGeneName: String, val ensemblGeneId: String,
        // TODO: convert Optional to arrow-kt Option
        val uniProtMapping: Optional<UniProtMapping>,
        val tpm: Double, val externalSampleId: String,
        val externalSubjectId: String,
        val id: String
) {
    companion object : AlsdbModel {
        val columnHeadings = arrayOf("EnsemblGeneId", "HugoGeneName", "EnsemblGeneId",
                "TPM", "ExternalSampleId", "ExternalSubjectId")

        fun parseCsvRecordFunction(record: CSVRecord): RnaTpmGene {
            val ensemblGeneId: String = record.get("EnsemblGeneId")
            return RnaTpmGene(
                    record.get("HugoGeneName"),
                    record.get("EnsemblGeneId"),
                    UniProtMappingService.INSTANCE.resolveUniProtMappingByEnsemblGeneId(ensemblGeneId),
                    record.get("TPM").toDouble(),
                    record.get("ExternalSampleId"),
                    record.get("ExternalSubjectId"),
                    "{ensemblGeneId}:${record.get("ExternalSampleId")}"
            )
        }
    }
}