package org.biodatagraphdb.alsdb.model

import arrow.core.Option
import arrow.core.Some
import arrow.core.none
import org.apache.commons.csv.CSVRecord

data class UniProtEnsemblTranscript(
        val uniprotId:String,
        val geneDescription:String,
        val geneSymbol:String,
        val ensemblTranscriptId:String
) {
    companion object : AlsdbModel {
        // not every entry in the file contains a uniprot to ensembl mapping
        fun parseCSVRecord(record: CSVRecord): Option<UniProtEnsemblTranscript> {
            val valid:Boolean = record.get("UniProtKB/Swiss-Prot ID").isNotEmpty()
            when(valid) {
                true -> return  Some(UniProtEnsemblTranscript(
                        record.get("UniProtKB/Swiss-Prot ID"),
                        record.get("Gene description"),
                        record.get("HGNC symbol"),
                        record.get("Transcript stable ID"))
                )
                false -> return none()
            }
        }

    }
}