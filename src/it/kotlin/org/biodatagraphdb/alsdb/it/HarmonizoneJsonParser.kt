package org.biodatagraphdb.alsdb.it

import com.google.gson.Gson
import org.biodatagraphdb.alsdb.model.harmonizome.Association
import org.biodatagraphdb.alsdb.model.harmonizome.GeneDiseaseAssociations
import org.biodatagraphdb.alsdb.model.harmonizome.HarmonizomeGene
import org.biodatagraphdb.alsdb.model.harmonizome.HarmonizomeProtein
import org.biodatagraphdb.alsdb.model.uniprot.Entry
import org.biodatagraphdb.alsdb.model.uniprot.Uniprot
import java.net.URL
import java.util.stream.Collectors
import javax.xml.bind.JAXBContext

/**
 * Created by fcriscuo on 4/2/20.
 */
const val harmonizomeBaseUrl = "http://amp.pharm.mssm.edu/Harmonizome"
const val harmonizomeAlsGenesUrl = harmonizomeBaseUrl +
        "/api/1.0/gene_set/amyotrophic+lateral+sclerosis/DISEASES+Text-mining+Gene-Disease+Assocation+Evidence+Scores"
const val uniprotUrlTemplate = "https://www.uniprot.org:443/uniprot/UNIPROTID.xml?include=yes"
const val defaultEntryLimit = 30L
const val uniprotStartTag = "<uniprot"
val gson = Gson()
fun main() {
    val entryLimit = 30L
  fetchDiseasAssociatedUniProtEntries(harmonizomeAlsGenesUrl, entryLimit)
           ?.forEach { entry ->  run {
               println("name: ${entry?.getNameList()} db reference count = ${entry?.getDbReferenceList()?.size}")
               entry?.getDbReferenceList()?.forEach { ref ->
                   run {
                       println("ref id ${ref.id}  type: ${ref.type}  properties: ${ref?.getPropertyList()?.size}")
                       ref?.getPropertyList()?.stream()?.filter { it -> it.type.equals("term") }
                               ?.forEach { println("term:  ${it.value}") }
                   }
               }
           } }
}

fun fetchDiseasAssociatedUniProtEntries(harmonizeUrl: String, entryLimit: Long): MutableList<Entry?>? =
        fetchHarmonizomeDiseaseGeneAssoctions(harmonizomeAlsGenesUrl).stream()
                .limit(entryLimit)
                .map { assoc -> fetchHarmonizomeGene(assoc) }
                .filter { gene -> !gene.proteins.isEmpty() }
                .map { gene -> fetchHarmonizomeProteins(gene) }
                .flatMap {protein ->
                    fetchUniProtEntryList(protein).stream()}
                .collect(Collectors.toList())

fun fetchHarmonizomeDiseaseGeneAssoctions(harmonizomeUrl: String): List<Association> {
    val json = URL(harmonizomeUrl).readText()
    val alsGeneAssociation = gson.fromJson(json, GeneDiseaseAssociations::class.java)
    println("ALS gene association count = ${alsGeneAssociation.associations.size}")
    return alsGeneAssociation.associations
}

fun fetchUniProtEntryList(proteinList: List<HarmonizomeProtein>): List<Entry?> {
    var tmpList = arrayListOf<Entry?>()
    proteinList.stream().forEach { protein ->
        val uniprotUrl = protein?.uniprotId?.let { uniprotUrlTemplate.replace("UNIPROTID", it) }
        println("Fetching uniprot: ${protein.uniprotId}  URL = $uniprotUrl")
        val xmlText = skipUniprotXmlHeader(URL(uniprotUrl).readText())
        println("Text: ${xmlText.subSequence(0,120)}")
        val jaxbContext = JAXBContext.newInstance(Uniprot::class.java)
        val unmarshaller = jaxbContext.createUnmarshaller()
        xmlText.reader().use { it ->
            val uniprot = unmarshaller.unmarshal(it) as Uniprot
            if (uniprot.getEntryList()?.isNotEmpty()!!) {
                tmpList.add(uniprot.getEntryList()?.get(0))  // only want the 1st entry
            }
        }
    }
    return tmpList.toList()
}

fun fetchHarmonizomeGene(association: Association): HarmonizomeGene {
    val url = harmonizomeBaseUrl + association.gene.href
    val json = URL(url).readText()
    return gson.fromJson(json, HarmonizomeGene::class.java)
}

fun fetchHarmonizomeProteins(gene: HarmonizomeGene): List<HarmonizomeProtein> {
    var tmpList = arrayListOf<HarmonizomeProtein>()
    gene.proteins.stream()
            .map { protein -> harmonizomeBaseUrl + protein.href }
            .map { url -> URL(url).readText() }
            .map { json -> gson.fromJson(json, HarmonizomeProtein::class.java) }
            .forEach { t: HarmonizomeProtein -> tmpList.add(t) }
    return tmpList.toList()

}

private fun skipUniprotXmlHeader(xml: String): String =
        xml.substring(xml.indexOf(uniprotStartTag))