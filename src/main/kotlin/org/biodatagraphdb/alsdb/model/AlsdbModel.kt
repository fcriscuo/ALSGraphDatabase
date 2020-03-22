package org.biodatagraphdb.alsdb.model

/*
Represents a collection of functions used by model objects to facilitate input processing
 */

interface AlsdbModel {

    /*
     def isValidString(x:String):Boolean = x != null && x.trim.length> 0
     */
   fun isValidString(s: String?):Boolean =  !s.isNullOrBlank()

    fun parseStringOnSemiColon(s: String): List<String> = parseStringOnDelimiter(s, ";")

    fun parseStringOnColon(s: String): List<String> = parseStringOnDelimiter(s, ":")

    fun parseStringOnPipe(s: String): List<String> = parseStringOnDelimiter(s, "|")

    fun parseStringOnComma(s: String): List<String> = parseStringOnDelimiter(s, ",")

    fun parseStringOnTab(s: String): List<String> = parseStringOnDelimiter(s, "\t")

    fun parseStringOnEquals(s:String): Pair<String,String>? {
        val list = parseStringOnDelimiter(s,"=")
        if ( list.size == 2)
            return Pair(list[0], list[1])
        return null
    }

    private fun parseStringOnDelimiter(s: String, delimiter: String): List<String> =
            s.split(delimiter).map { it.trim() }

    fun isHumanSpecies(species: String): Boolean = species.trim().toUpperCase().equals("HOMO SAPIENS")

    fun isEmptyString(s: String): Boolean = s.trim().isNullOrEmpty()

    fun booleanFromInt(i: Int): Boolean = i == 1

    private fun reduceListToDelimitedString(list: List<String>, delimiter: String): String =
            list.joinToString { delimiter }

    fun reduceListToPipeDelimitedString(list: List<String>) = reduceListToDelimitedString(list, "|")

    /*
    Function to return last element in a List
    Needed to support Java clients
     */
    fun <T> getLastListElement(list: List<T>): T = list.last()

    /*
    Function to return the individual ontologies form a  pipe joined String
    without their enclosing parenthesis
     */
    fun parseOntologyList(s: String): List<String> {
        val ontologyList = mutableListOf<String>()
        parseStringOnPipe(s).forEach {
            ontologyList += it.substring(it.indexOf('(' + 1), it.length - 1)
        }
        return ontologyList
    }

    fun parseDoubleString(ds: String): Double = ds.replace(',', '.').toDouble()

    /*
    Function to convert a floating point String into a Float
    returns 0.0 if the String is not in a floating point format
     */
    fun parseValidFloatFromString(fs: String): Float =
            when (Regex("[-+]?[0-9]*\\.?[0-9]+").matches(fs)) {
                true -> fs.toFloat()
                else -> 0.0F
            }

    /*
    Function to convert an Integer String to an Integer
    returns 0 if the field is not numeric
     */

    fun parseValidIntegerFromString(s: String): Int =
            when (Regex(" \"^\\\\d+\$\"").matches(s)) {
                true -> s.toInt()
                else -> 0
            }

    fun generateProActGuid(id:Int):String =
            "PROACT" + "%08d".format(id)



}

