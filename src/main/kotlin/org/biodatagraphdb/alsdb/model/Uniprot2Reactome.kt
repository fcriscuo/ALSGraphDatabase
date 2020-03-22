package org.biodatagraphdb.alsdb.model

import org.apache.commons.csv.CSVRecord

data class Uniprot2Reactome(
        val uniProtId: String,
        val reactomeId: String,
        val url: String,
        val eventName: String,
        val evidenceCode: String,
        val species: String
) {
    companion object : AlsdbModel {
        fun parseCSVRecord(record: CSVRecord): Uniprot2Reactome =
                Uniprot2Reactome(
                        record.get("UniProt_ID"),
                        record.get("Reactome_ID"),
                        record.get("URL"),
                        record.get("Event_Name"),
                        record.get("Evidence_Code"),
                        record.get("Species")
                )
    }
}