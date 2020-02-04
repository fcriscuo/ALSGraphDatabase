package org.biodatagraphdb.alsdb.model

data class GeneOntology(
        val goTermAccession: String,
        val goDomain: String,
        val goName: String,
        val goDefinition: String = " "
) {

    companion object : AlsdbModel {
        private fun resolveGeneOntologyTuple(goEntry: String): Pair<String, String> {
            val index = goEntry.indexOf('[') + 1
            val index2 = 0.coerceAtLeast(index - 1)
            return Pair(goEntry.substring(0, index2).trim(),
                    goEntry.slice(index..index + 9))
        }

        fun parseGeneOntologyEntry(aspect: String, entry: String): GeneOntology {
            val tuple2: Pair<String, String> = resolveGeneOntologyTuple(entry)
            return GeneOntology(goTermAccession = tuple2.second, goDomain = aspect,
                    goName = tuple2.first)

        }
    }

}