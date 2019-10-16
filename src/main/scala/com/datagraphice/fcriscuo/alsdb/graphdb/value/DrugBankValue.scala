package edu.jhu.fcriscu1.als.graphdb.value

import org.apache.commons.csv.CSVRecord

/*
DrugBank ID,Name,CAS Number,Drug Type,KEGG Compound ID,
KEGG Drug ID,PubChem Compound ID,PubChem Substance ID,
ChEBI ID,PharmGKB ID,HET ID,UniProt ID,UniProt Title,
GenBank ID,DPD ID,RxList Link,Pdrhealth Link,Wikipedia ID,
Drugs.com Link,NDC ID,ChemSpider ID,BindingDB ID,TTD ID
 */
case class DrugBankValue(
                          drugBankId: String, drugName: String, casNumber: String,
                          drugType: String, keggCompoundId: String, keggDrugId: String,
                          pubChemCompoundId: String, pubChemSubstanceId: String,
                          chebiId: String, pharmGKBId: String, hetId: String, uniProtId: String,
                          uniProtTitle: String, genBankId: String, dpdId: String, rxListLink: String,
                          pdrhealthLink: String, wikipediaId: String, drugsComLink: String,
                          ndcLink: String, chemSpiderId: String, bindinDbId: String, ttdId: String
                        ) {}

object DrugBankValue extends ValueTrait {

  def parseCSVRecord(record: CSVRecord): DrugBankValue = {
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
      record.get("TTD ID")

    )
  }
}
