package org.biodatagraphdb.alsdb.it

import mu.KotlinLogging
import org.biodatagraphdb.alsdb.lib.Gff3RecordStreamSupplier
import org.biodatagraphdb.alsdb.model.Gff3Entry
import java.nio.file.Paths

/**
 * Created by fcriscuo on 3/12/20.
 * Integration test for parsing a sample GFF3 file from Gencode
 */
val logger = KotlinLogging.logger {}
fun main(args: Array<String>) {
    val logger = KotlinLogging.logger {}
    //TODO: make property
    val defaultTsvFile = "/Volumes/Data04TBBExt/data/Gencode/sample.gencode.v33.basic.annotation.gff3"
    val filePathName = when (args.size) {
        0 -> defaultTsvFile
        else -> args.get(0)
    }
    logger.info { "Processing data file: $filePathName" }
    val path = Paths.get(filePathName)
    Gff3RecordStreamSupplier(path).get()
            .limit(500L)
            .map(Gff3Entry.Companion::parseCSVRecord)
            .forEach { gff3Entry ->
                run {
                    logger.info("ID: ${gff3Entry.seqId}  Source: ${gff3Entry.source}  Start: ${gff3Entry.start}")
                }
            }
}