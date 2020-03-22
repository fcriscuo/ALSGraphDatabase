package org.biodatagraphdb.alsdb.app

import arrow.core.Either
import mu.KotlinLogging
import org.biodatagraphdb.alsdb.lib.AlsFileUtils
import org.biodatagraphdb.alsdb.service.property.DatafilesPropertiesService

/**
 * Created by fcriscuo on 3/21/20.
 * A Kotlin application that will retrieve application data from
 * resources specified in the datafiles.properties file. The mined
 * data sources can be filtered by specifying those sources as
 * input keyword arguments (eg. UniProt, DisGeNET, etc.). The default is
 * to use all properties whose name starts with data
 *
 */
val logger = KotlinLogging.logger {}

//TODO: make async coroutine
fun resolveDataSource(url: String) {
    val result = AlsFileUtils.retrieveRemoteFileByFtpUrl(url)
    when (result) {
        is Either.Right -> logger.info { "Success: ${result.b}" }
        is Either.Left -> logger.error { "ERROR: ${result.a.message}" }
    }
}

fun main(args: Array<String>) {
    val dataSourceList = if (args.size > 0) args.toList()
    else {
        listOf("data")
    }
    // find URLs for each data source
    dataSourceList
            .map { source ->
                DatafilesPropertiesService.filterProperties(source)
                        .forEach { resolveDataSource(it) }
            }
}