package org.biodatagraphdb.alsdb.it

import org.biodatagraphdb.alsdb.model.pubmed.PubmedArticleSet
import java.net.URL
import java.nio.charset.Charset
import javax.xml.bind.JAXBContext

/**
 * Created by fcriscuo on 4/18/20.
 */
const val pubMedTemplate = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&amp;id=PUBMEDID&amp;retmode=xml"
const val pubMedToken = "PUBMEDID"
const val startTag = "<PubmedArticleSet>"
const val UTF8_BOM = "\uFEFF"
val defaultId = "15489334"

fun main(args: Array<String>) {
    val id = if (args.size > 0) args[0] else defaultId
    val url = pubMedTemplate.replace(pubMedToken, id)
    val text = URL(url).readText(Charset.defaultCharset())
    val xmlText = skipXmlHeader(text)
    val jaxbContext = JAXBContext.newInstance(PubmedArticleSet::class.java)
    val unmarshaller = jaxbContext.createUnmarshaller()
    xmlText.reader().use { it ->
        val pubmed = unmarshaller.unmarshal(it) as PubmedArticleSet
        val article = pubmed.pubmedArticle?.medlineCitation?.article
        val journal = article?.journal
        val journalTitle = journal?.title
        val journalIssue = Pair(journal?.journalIssue?.volume, journal?.journalIssue?.issue)
        val articleTitle = article?.articleTitle
        val abstractText = article?.abstract?.abstractText
        println("PubMedId:$id ")
        println("Journal: $journalTitle  ${journalIssue.first}:${journalIssue.second}")
        println("Title:  $articleTitle")
        println("Abstract: ")
        splitText(article?.abstract?.abstractText).forEach { line -> println("   $line") }
    }
}
private fun splitText (text:String?): List<String> =
    if ( text != null)text.chunked(60) else emptyList()


private fun skipXmlHeader(xml: String): String =
    xml.substring(xml.indexOf(startTag))


private fun removeBOM(text:String): String = if (text.startsWith(UTF8_BOM)) text.substring(1) else text