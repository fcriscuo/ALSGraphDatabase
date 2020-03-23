package org.biodatagraphdb.alsdb.service.property

import arrow.core.Either
import arrow.core.None
import arrow.core.Option
import arrow.core.Some
import mu.KotlinLogging
import java.net.URI
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*

/**
 * Created by fcriscuo on 3/19/20.
 */
abstract class AbstractPropertiesService {
    val properties: Properties = Properties()
    val logger = KotlinLogging.logger {}


    fun resolveFrameworkProperties(propertiesFile: String) {
        val stream = AbstractPropertiesService::class.java.getResourceAsStream(propertiesFile)
        //val p = Properties()
        properties.load(stream)
    }

    fun resolvePropertyAsStringPair(propertyName: String): Either<Exception, Pair<String, String>> =
        if( properties.contains(propertyName))
             Either.right(Pair(propertyName, properties.getProperty(propertyName).toString()))
         else Either.left(Exception("property name $propertyName not found"))

    fun resolvePropertyAsString(propertyName: String): String? =
            if (properties.containsKey(propertyName)) {
                logger.info("Property Value: ${properties.getProperty(propertyName)}")
                properties.getProperty(propertyName).toString()
            } else {
                logger.warn { "$propertyName is an invalid property name " }
                null
            }


    fun resolvePropertyAsStringOption(propertyName: String): Option<String> =
            if (properties.containsKey(propertyName)) {
                logger.info("Property Value: ${properties.getProperty(propertyName)}")
                Some(properties.getProperty(propertyName).toString())
            } else {
                logger.warn { "$propertyName is an invalid property name " }
                None
            }

    fun resolvePropertyAsInt(propertyName: String): Int? =
            if (properties.containsKey(propertyName)) {
                properties.getProperty(propertyName).toIntOrNull()
            } else {
                null
            }

    fun resolvePropertyAsResourcePathOption(propertyName: String): Option<Path> {
        val propertyOption = resolvePropertyAsStringOption(propertyName)
        if (propertyOption.nonEmpty()) {
            return propertyOption.map { name -> URI(name) }
                    .map { uri -> Paths.get(uri) }
        }
        return None
    }

    fun resolvePropertyAsPathOption(propertyName: String): Option<Path> {
        val propertyOption = resolvePropertyAsStringOption(propertyName)
        if (propertyOption.nonEmpty()) {
            return propertyOption.map { path -> Paths.get(path) }
        }
        logger.info { "Requested Path property: $propertyName is invalid" }
        return None
    }

    fun filterProperties(filter: String): List<Pair<String,String>> {
        var tmpList = mutableListOf<Pair<String,String>>()
        properties.keys.filter{ it -> it.toString().contains(filter) }
                .map { key -> tmpList.add(Pair(key.toString(),properties.get(key).toString())) }

        return tmpList.toList()
    }

    fun displayProperties() {
        properties.keys.forEach { key ->
            println("key: $key  value: ${properties.get(key)}")
        }
    }

}