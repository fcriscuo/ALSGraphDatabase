package org.biodatagraphdb.alsdb.value

import org.apache.commons.csv.CSVRecord

case class HgncLocus (
                     hgncId:String, hugoSymbol:String,
                     hgncName:String, hgncLocusGroup:String,
                     hgncLocusType:String, status:String,hgncLocation:String,
                     entrezId:String,
                     geneFamily:String,ensemblGeneId:String,
                     refSeqAccession:String, uniprotId:String,
                     pubMedIdList:List[String], ccdsId:String,
                     omimId:String
                     ) {

   def isApprovedLocus:Boolean = status.equalsIgnoreCase("Approved")
  val validLocusTypeGroup:List[String] = List("protein-coding gene","non-coding RNA")
  def isApprovedLocusTypeGroup:Boolean = validLocusTypeGroup.contains(hgncLocusGroup)
  val id:String = hgncId
 }

object HgncLocus extends ValueTrait {
  val columnHeadings: Array[String] = Array(
    "hgnc_id","symbol","name","locus_group","locus_type","status","location","location_sortable",
    "alias_symbol","alias_name","prev_symbol","prev_name","gene_family","gene_family_id",
    "date_approved_reserved","date_symbol_changed","date_name_changed","date_modified",
    "entrez_id","ensembl_gene_id","vega_id","ucsc_id","ena","refseq_accession","ccds_id",
    "uniprot_ids","pubmed_id","mgd_id","rgd_id","lsdb","cosmic","omim_id","mirbase",
    "homeodb","snornabase","bioparadigms_slc","orphanet","pseudogene.org","horde_id",
    "merops","imgt","iuphar","kznf_gene_catalog","mamit-trnadb","cd","lncrnadb",
    "enzyme_id","intermediate_filament_db","rna_central_ids"
  )
  def parseCSVRecord(record:CSVRecord):HgncLocus = {
      HgncLocus(
        record.get("hgnc_id"),
        record.get("symbol"),
        record.get("name"),
        record.get("locus_group"),
        record.get("locus_type"),
        record.get("status"),
        record.get("location"),
        record.get("entrez_id"),
        record.get("gene_family"),
        record.get("ensembl_gene_id"),
        record.get("refseq_accession"),
        record.get("uniprot_ids"),
        parseStringOnPipeFunction(record.get("pubmed_id")),
        record.get("ccds_id"),
        record.get("omim_id")





      )

  }
}
