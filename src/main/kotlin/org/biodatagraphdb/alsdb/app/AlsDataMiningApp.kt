package org.biodatagraphdb.alsdb.app

import arrow.core.Either
import com.google.common.base.Stopwatch
import mu.KotlinLogging
import org.biodatagraphdb.alsdb.lib.AlsFileUtils
import org.biodatagraphdb.alsdb.service.property.DatafilesPropertiesService
import java.util.concurrent.TimeUnit

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

fun copySourceDataToLocalFile(propertyPair: Pair<String,String>) {
    val stopwatch = Stopwatch.createStarted()
    val result = AlsFileUtils.retrieveRemoteFileByDatafileProperty(propertyPair)
    when (result) {
        is Either.Right -> logger.info { "Success: ${result.b}" }
        is Either.Left -> logger.error { "ERROR: ${result.a.message}" }
    }
    logger.info("++++ Data retrieval required: ${stopwatch.elapsed(TimeUnit.SECONDS).toDouble()} seconds")
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
                        .forEach { copySourceDataToLocalFile(it) }
            }
}