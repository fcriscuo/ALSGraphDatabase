package org.biodatagraphdb.alsdb.model

import org.apache.commons.csv.CSVRecord

/**
 * Represents an entry (i.e. line) from a tab-delimited file in GFF3 format
 * Re: https://github.com/The-Sequence-Ontology/Specifications/blob/master/gff3.md
 *     https://uswest.ensembl.org/info/website/upload/gff3.html
 * missing column values are represented by a "." or possibly a "?"
 * comment lines start with #
 *
 * Created by fcriscuo on 3/11/20.
 */
data class Gff3Entry(
        val seqId: String,
        val source: String,
        val type: String,
        val start: Long,
        val end: Long,
        val score: Float?,
        val strand: SequenceStrand?,
        val phase: Int,
        val attributeMap: Map<String, List<String>>)
{
    companion object : AlsdbModel {
        private const val SEQ_ID = 0
        private const val SOURCE = 1
        private const val TYPE = 2
        private const val START = 3
        private const val END = 4
        private const val SCORE = 5
        private const val STRAND = 6
        private const val PHASE = 7
        private const val ATTRIBUTES = 8

        private fun resolveGff3AttributeMap(attributeData: String): Map<String, List<String>> {
            var tmpMap = mutableMapOf<String, List<String>>()
            parseStringOnSemiColon(attributeData).forEach({
                val attributePair = parseStringOnEquals(it)
                if (attributePair != null) {
                    tmpMap.put(attributePair.first,
                            parseStringOnComma(attributePair.second))
                }
            })
            return tmpMap.toMap()
        }

        private fun processAbsentScoreValue(score: String) =
                if(score != ".") score.toFloat() else null

        fun parseCSVRecord(record: CSVRecord): Gff3Entry =
                Gff3Entry(record.get(SEQ_ID),
                        record.get(SOURCE),
                        record.get(TYPE),
                        record.get(START).toLong(),
                        record.get(END).toLong(),
                        parseValidFloatFromString(record.get(SCORE)),
                        record.get(STRAND).asSequenceStrand(),
                        parseValidIntegerFromString(record.get(PHASE)),
                        resolveGff3AttributeMap(record.get(ATTRIBUTES))
                )
    }
}


