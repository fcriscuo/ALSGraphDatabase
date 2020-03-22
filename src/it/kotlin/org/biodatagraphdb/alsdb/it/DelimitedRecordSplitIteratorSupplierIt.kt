package org.biodatagraphdb.alsdb.it

import org.biodatagraphdb.alsdb.lib.DelimitedRecordSplitIteratorSupplier
import org.biodatagraphdb.alsdb.model.PsiMitab
import org.biodatagraphdb.alsdb.util.Utils
import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicInteger

/**
 * Created by fcriscuo on 2/28/20.
 */

fun main() {
    val testPath = Paths.get("/Volumes/Data04TBBExt/data/UniProt/human_intact.tsv")
    val columnHeadings = Utils.resolveColumnHeadingsFunction
            .apply(Paths.get("/Volumes/Data04TBBExt/data/UniProt/heading_intact.txt"))
    val count = AtomicInteger(0)
    DelimitedRecordSplitIteratorSupplier(testPath, *columnHeadings).get()
            .forEach { record ->
                try {
                    count.getAndIncrement()
                    if (count.get() % 10000 == 0) {
                        val psiMitab: PsiMitab? = record?.let { PsiMitab.Companion.parseCSVRecord(it) }
                        if (psiMitab != null){
                            println("${count.toString()}  ${psiMitab.xrefAList}")
                        }
                       // println("${count.toString()}  ${(record?.get("#ID(s) interactor A") ?: " ")}")
                    }
                } catch (e: Exception) {
                    var message: String? = e.message
                    e.printStackTrace()
                }
            }
    println("Number of intact records = ${count.get()}")
}


