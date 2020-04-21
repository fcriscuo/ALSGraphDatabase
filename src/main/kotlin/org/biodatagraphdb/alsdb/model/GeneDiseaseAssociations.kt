package org.biodatagraphdb.alsdb.model

/**
 * Represents the gene-disease associations mined from Harmonizome data resource
 * http://amp.pharm.mssm.edu/Harmonizome/
 * Data is in JSON format
 * Created by fcriscuo on 4/2/20.
 */

data class GeneDiseaseAssociations(val associations: List<Association>,
                                   val attribute: Attribute,
                                   val dataset: Dataset) {
}
data class Association(
        val gene: Gene,
        val standardizedValue: Double,
        val thresholdValue: Double
)
data class Attribute(
        val href: String,
        val name: String
)
data class Dataset(
        val href: String,
        val name: String
)

data class Gene(
        val href: String,
        val symbol: String
)