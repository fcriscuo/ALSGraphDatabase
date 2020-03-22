package org.biodatagraphdb.alsdb.model

import org.apache.commons.csv.CSVRecord

/**
 * Created by fcriscuo on 2/4/20.
 */
data class DrugBankValue(
       val  drugBankId: String, val drugName: String, val casNumber: String,
        val drugType: String, val keggCompoundId: String, val keggDrugId: String,
        val pubChemCompoundId: String, val pubChemSubstanceId: String,
        val chebiId: String, val pharmGKBId: String, val hetId: String, val uniProtId: String,
        val uniProtTitle: String, val genBankId: String, val dpdId: String, val rxListLink: String,
        val pdrhealthLink: String, val wikipediaId: String, val drugsComLink: String,
        val ndcLink: String, val chemSpiderId: String, val bindinDbId: String, val ttdId: String
) {
    companion object : AlsdbModel {
        fun parseCSVRecord(record: CSVRecord): DrugBankValue =
            DrugBankValue(
                    record.get("DrugBank ID"),
                    record.get("Name"),
                    record.get("CAS Number"),
                    record.get("Drug Type"),
                    record.get("KEGG Compound ID"),
                    record.get("KEGG Drug ID"),
                    record.get("PubChem Compound ID"),
                    record.get("PubChem Substance ID"),
                    record.get("ChEBI ID"),
                    record.get("PharmGKB ID"),
                    record.get("HET ID"),
                    record.get("UniProt ID"),
                    record.get("UniProt Title"),
                    record.get("GenBank ID"),
                    record.get("DPD ID"),
                    record.get("RxList Link"),
                    record.get("Pdrhealth Link"),
                    record.get("Wikipedia ID"),
                    record.get("Drugs.com Link"),
                    record.get("NDC ID"),
                    record.get("ChemSpider ID"),
                    record.get("BindingDB ID"),
                    record.get("TTD ID"))
    }

}