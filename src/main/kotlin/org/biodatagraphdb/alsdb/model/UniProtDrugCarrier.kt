package org.biodatagraphdb.alsdb.model

import org.apache.commons.csv.CSVRecord

data class UniProtDrugCarrier (
        val drugModelType: String,
        val id:String,
        val name:String,
        val geneName:String,
        val genbankProteinId:String,
        val genbankGeneId:String,
        val uniprotId:String,
        val uniprotTitle:String,
        val pdbId:String,
        val geneCardId:String,
        val geneAtlasId:String,
        val hgncId:String,
        val species:String,
        val drugIdList: List<String>
){
    companion object : AlsdbModel {
        fun parseCSVRecord(record: CSVRecord): UniProtDrugCarrier =
            UniProtDrugCarrier(
                    "DRUG_CARRIER",
                    record.get("ID"),
                    record.get("Name"),
                    record.get("Gene Name"),
                    record.get("GenBank Protein ID"),
                    record.get("GenBank Gene ID"),
                    record.get("UniProt ID"),
                    record.get("Uniprot Title"),
                    record.get("PDB ID"),
                    record.get("GeneCard ID"),
                    record.get("GenAtlas ID"),
                    record.get("HGNC ID"),
                    record.get("Species"),
                    parseStringOnSemiColon(record.get("Drug IDs"))
            )
    }
}