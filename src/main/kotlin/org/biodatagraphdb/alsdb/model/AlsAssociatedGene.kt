package org.biodatagraphdb.alsdb.model

import org.apache.commons.csv.CSVRecord

/**
 * Created by fcriscuo on 2/3/20.
 */


data class AlsAssociatedGene (val geneSymbol: String, val geneName: String){
    val id = geneSymbol

    companion object : AlsdbModel {
        const val GENE_SYMBOL = "Gene symbol"
        const val GENE_NAME = "Gene name"
        fun parseCSVRecord(record: CSVRecord): AlsAssociatedGene =
                AlsAssociatedGene(record.get(GENE_SYMBOL), record.get(GENE_NAME))


    }
}