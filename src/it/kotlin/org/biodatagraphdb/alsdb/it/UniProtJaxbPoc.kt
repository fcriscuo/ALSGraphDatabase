package org.biodatagraphdb.alsdb.it

import org.biodatagraphdb.alsdb.model.uniprot.Uniprot
import java.io.FileReader
import java.net.URL
import javax.xml.bind.JAXBContext

/**
 * Created by fcriscuo on 4/3/20.
 */
fun main() {
    //val text = FileReader("/tmp/P07900.xml").readText()
    val text = URL("https://www.uniprot.org:443" +
            "/uniprot/Q16538.xml?include=yes").readText()
    println("Text: ${text.subSequence(0,140)}")
    val jaxbContext = JAXBContext.newInstance(Uniprot::class.java)
    val unmarshaller = jaxbContext.createUnmarshaller()
    text.reader().use { it ->
        val uniprot =  unmarshaller.unmarshal(it) as Uniprot
        uniprot.getEntryList()?.forEach { entry ->
            run {
                println("name: ${entry.getNameList()} db reference count = ${entry.getDbReferenceList()?.size}")
                entry.getDbReferenceList()?.forEach { ref ->
                    run{
                        println("ref id ${ref.id}  type: ${ref.type}  properties: ${ref.getPropertyList()?.size}")
                        ref.getPropertyList()?.stream()?.filter { it -> it.type.equals("term") }
                            ?.forEach { println("term:  ${it.value}" ) }
                    }
                }
            }
        }

    }


}