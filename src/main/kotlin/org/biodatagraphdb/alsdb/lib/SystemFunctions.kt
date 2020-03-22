package org.biodatagraphdb.alsdb.lib

import arrow.core.Either
import java.io.InputStream

/**
 * Created by fcriscuo on 3/21/20.
 */

fun getEnvVariable(varname:String):String = System.getenv(varname) ?: "undefined"

fun getDrugBankCredentials():Pair<String,String> =
        Pair(getEnvVariable("DRUG_BANK_USER"),
                getEnvVariable("DRUGBANK_PASSWORD"))



 fun executeCurlOperation( curlCommand:String): Either<Exception, InputStream> {
     try {
         val proc = Runtime.getRuntime().exec(curlCommand)
         return Either.right(proc.inputStream)
     } catch (e: Exception) {
         return Either.left(e)
     }
 }