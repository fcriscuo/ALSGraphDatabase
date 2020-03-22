package org.biodatagraphdb.alsdb.model

import org.apache.commons.csv.CSVRecord

/**
 * Created by fcriscuo on 2/4/20.
 */
data class HgncLocus(
        val hgncId: String, val hugoSymbol: String,
        val hgncName: String, val hgncLocusGroup: String,
        val hgncLocusType: String, val status: String, val hgncLocation: String,
        val entrezId: String,
        val geneFamily: String, val ensemblGeneId: String,
        val refSeqAccession: String, val uniprotId: String,
        val pubMedIdList: List<String>,
        val ccdsId: String,
        val omimId: String
) {
    val isApprovedLocus: Boolean = status == "Approved"
    val isApprovedLocusTypeGroup = listOf("protein-coding gene", "non-coding RNA").contains(hgncLocusGroup)
    val id: String = hgncId

    companion object : AlsdbModel {
        fun parseCSVRecord(record: CSVRecord): HgncLocus =
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
                        parseStringOnPipe(record.get("pubmed_id")),
                        record.get("ccds_id"),
                        record.get("omim_id"))
    }
}