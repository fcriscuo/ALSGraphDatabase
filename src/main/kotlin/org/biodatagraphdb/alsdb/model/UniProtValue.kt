package org.biodatagraphdb.alsdb.model

import org.apache.commons.csv.CSVRecord
import java.nio.file.Path
import java.nio.file.Paths

data class UniProtValue(
        val uniprotId: String,
        val uniprotName: String,
        val proteinNameList: List<String>,
        val geneNameList: List<String>,
        val pathwayList: List<String>,
        val interactionList: List<String>,
        val goBioProcessList: List<String>,
        val goCellComponentList: List<String>,
        val diseaseList: List<String>,
        val tissueList: List<String>,
        val goMolFuncList: List<String>,
        val phenotype: String,
        val drugBankIdList: List<String>,
        val reactomeIdList: List<String>,
        val pubMedIdList: List<String>,
        val mass: String,
        val length: String,
        val ensemblTranscriptList: List<String>
) {
    companion object : AlsdbModel {
        val defaultFilePath: Path = Paths.get("/data/als/uniprot-filtered-human.tsv")
        fun parseCSVRecord(record: CSVRecord): UniProtValue =
                UniProtValue(record.get("Entry"),
                        record.get("Entry name"),
                        parseStringOnSemiColon(record.get("Protein names")),
                        parseStringOnSemiColon(record.get("Gene names")),
                        parseStringOnSemiColon(record.get("Pathway")),
                        parseStringOnSemiColon(record.get("Interacts with")),
                        parseStringOnSemiColon(record.get("Gene ontology (biological process)")),
                        parseStringOnSemiColon(record.get("Gene ontology (cellular component)")),
                        parseStringOnSemiColon(record.get("Involvement in disease")),
                        parseStringOnSemiColon(record.get("Tissue specificity")),
                        parseStringOnSemiColon(record.get("Gene ontology (molecular function)")),
                        record.get("Disruption phenotype"),
                        parseStringOnSemiColon(record.get("Cross-reference (DrugBank)")),
                        parseStringOnSemiColon(record.get("Cross-reference (Reactome)")),
                        parseStringOnSemiColon(record.get("PubMed ID")),
                        record.get("Mass"),
                        record.get("Length"),
                        parseStringOnSemiColon(record.get("Ensembl transcript"))
                )
    }
}