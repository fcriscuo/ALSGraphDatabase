package org.biodatagraphdb.alsdb.model

import org.apache.commons.csv.CSVRecord

/**
 * Created by fcriscuo on 2/4/20.
 */
data class UniProtMapping(
        val uniProtId:String, val ensemblGeneId:String, val ensemblTranscriptId:String,
        val geneSymbol:String
) {
    companion object : AlsdbModel {
        fun parseCsvRecordFunction(record: CSVRecord): UniProtMapping =
                UniProtMapping(record.get("UniProtKB/Swiss-Prot ID"),
                        record.get("Gene stable ID"),
                        record.get("Transcript stable ID"),
                        record.get("HGNC symbol"))
    }
}