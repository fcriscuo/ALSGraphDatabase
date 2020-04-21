package org.biodatagraphdb.alsdb.it

import org.biodatagraphdb.alsdb.model.uniprot.Uniprot
import java.io.FileReader
import javax.xml.bind.JAXBContext

/**
 * Created by fcriscuo on 4/3/20.
 */
fun main() {

    val text = FileReader("/tmp/P07900.xml").readText()
    val jaxbContext = JAXBContext.newInstance(Uniprot::class.java)
    val unmarshaller = jaxbContext.createUnmarshaller()
    text.reader().use { it ->
        val uniprot =  unmarshaller.unmarshal(it) as Uniprot
        uniprot.getEntryList()!!.forEach { entry ->
            run {
                entry.getGeneList()?.forEach { gene -> println("Gene ${gene.toString()}") }
            }
        }

    }


}