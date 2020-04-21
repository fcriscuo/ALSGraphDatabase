package org.biodatagraphdb.alsdb.model

data class GeneDetail(
    val description: String,
    val hgncRootFamilies: List<HgncRootFamily>,
    val name: String,
    val ncbiEntrezGeneId: Int,
    val ncbiEntrezGeneUrl: String,
    val proteins: List<Protein>,
    val symbol: String,
    val synonyms: List<String>
)