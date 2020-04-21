package org.biodatagraphdb.alsdb.model.harmonizome

import org.biodatagraphdb.alsdb.model.harmonizome.Protein

data class HarmonizomeGene(
    val description: String,
    val hgncRootFamilies: List<HgncRootFamily>,
    val name: String,
    val ncbiEntrezGeneId: Int,
    val ncbiEntrezGeneUrl: String,
    val proteins: List<Protein>,
    val symbol: String,
    val synonyms: List<String>
)

data class HgncRootFamily(
        val href: String,
        val name: String
)