package org.biodatagraphdb.alsdb.model.harmonizome

data class HarmonizomeProtein(
    val description: String,
    val gene: Gene,
    val name: String,
    val symbol: String,
    val uniprotId: String,
    val uniprotUrl: String
)