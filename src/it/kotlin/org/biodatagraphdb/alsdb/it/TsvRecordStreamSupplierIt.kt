package org.biodatagraphdb.alsdb.it

import mu.KotlinLogging
import org.biodatagraphdb.alsdb.lib.TsvRecordStreamSupplier
import org.biodatagraphdb.alsdb.model.HumanTissueAtlas
import java.nio.file.Paths

/**
 * Integration Test for reading tab delimited files
 * Created by fcriscuo on 2/25/20.
 */

fun main(args: Array<String>) {
   val logger = KotlinLogging.logger {}
    //TODO: make property
   val defaultTsvFile ="/Volumes/Data04TBBExt/data/HumanTissueAtlas/HumanTissueAtlas.tsv"
      val filePathName = when(args.size) {
         0 -> defaultTsvFile
         else -> args.get(0)
      }
   logger.info {"Processing data file: $filePathName"  }
    val path = Paths.get(filePathName)
   TsvRecordStreamSupplier(path).get()
           .limit(500L)
           .map (HumanTissueAtlas.Companion::parseCSVRecord)
           .forEach { tissueAtlas->
               run {
                  logger.info("Tissue: ${tissueAtlas.tissue}  Celltype: ${tissueAtlas.cellType}  HGNC: ${tissueAtlas.geneName}")
               }
           }

}