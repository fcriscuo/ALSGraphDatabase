package org.biodatagraphdb.alsdb.model

/**
 * Created by fcriscuo on 2/3/20.
 */
 data class AbstractUniProtDrug(
    val drugModelType: String,
    val id: String,
    val name: String,
    val geneName: String,
    val genbankProteinId: String,
    val genbankGeneId: String,
    val uniprotId: String,
    val uniprotTitle: String,
    val pdbId: String,
    val geneCardId: String,
    val geneAtlasId: String,
    val hgncId: String,
    val species: String,
    val drugIdList: List<String>

) {
}