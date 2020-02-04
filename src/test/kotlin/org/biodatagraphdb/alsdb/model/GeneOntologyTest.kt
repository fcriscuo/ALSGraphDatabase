package org.biodatagraphdb.alsdb.model

import io.kotlintest.specs.WordSpec


/**
 * Created by fcriscuo on 2/3/20.
 * Test class for GeneOntology model object
 */
class GeneOntologyTest:  WordSpec() {
    init {
        val goTermAccession = "GO:0072686"
        val goDomain = "Biological function"
        val goName = "mitotic spindle"
        val entry = "$goName[$goTermAccession"
        val geneOntology = GeneOntology.parseGeneOntologyEntry(goDomain, entry)

        "Parse GeneOntology Entry" should {
            "should have a GO name of" {
                geneOntology.goName.equals(goName)
            }
            "should have an accession number of"{
                geneOntology.goTermAccession.equals(goTermAccession)
            }

        }

    }
}