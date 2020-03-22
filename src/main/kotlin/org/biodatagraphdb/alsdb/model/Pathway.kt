package org.biodatagraphdb.alsdb.model

import org.apache.commons.csv.CSVRecord

/**
 * Created by fcriscuo on 2/5/20.
 */
data class Pathway(
        val uniprotId: String, val reactomeId: String, val eventName: String, val evidenceCode: String,
        val species: String, val id: String
) {
    val pathwayHeadings: String = "UniProt_ID\tReactome_ID\tURL\tEvent_Name\tEvidence_Code\tSpecies"
    val reactomeBaseUrl: String = "https://reactome.org/PathwayBrowser/#/"

    fun isHuman(): Boolean = species.equals("Homo sapiens",ignoreCase = true)

    companion object : AlsdbModel {
        fun parseCSVRecord(record: CSVRecord): Pathway =
                Pathway(record.get("UniProt_ID"),
                        record.get("Reactome_ID"),
                        record.get("Event_Name"),
                        record.get("Evidence_Code"),
                        record.get("Species"),
                        // composite identifier
                        record.get("UniProt_ID") + ":" + record.get("Reactome_ID")
                )
    }
}