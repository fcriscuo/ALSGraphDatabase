package org.biodatagraphdb.alsdb.model

import mu.KotlinLogging

/**
 * Created by fcriscuo on 3/12/20.
 */
private val logger = KotlinLogging.logger {}
inline class SequenceStrand(val strand: String) {
    companion object : Refined<String> {
        override fun isValid(value: String): Boolean {
            return when (value) {
                "+", "-", "0", "1" -> true
                else -> false
            }
        }
    }


}
fun String.asSequenceStrand(): SequenceStrand? =
        if ( SequenceStrand.isValid(this) ) SequenceStrand(this)
        else null

fun main() {
    logger.info{"+".asSequenceStrand()}
    logger.info {"X".asSequenceStrand()}
}