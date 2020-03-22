package org.biodatagraphdb.alsdb.it

import mu.KotlinLogging
import org.biodatagraphdb.alsdb.service.property.FrameworkPropertiesService

fun main() {
    val logger = KotlinLogging.logger {}
    // test Path property resolution
    val filePropertyName = "PPI_INTACT_FILE"
    FrameworkPropertiesService.resolvePropertyAsPathOption(filePropertyName)
            .map { path -> logger.info("Path $path") }
    // test string property resolution
    val stringPropertyName = "sftp_server"
    FrameworkPropertiesService.resolvePropertyAsStringOption(stringPropertyName)
            .map { s -> logger.info("String value: $s" )}
    // test for invlaid property
    // should get a warning message
    val invalidPropertyName = "XXXXX"
    if (FrameworkPropertiesService.resolvePropertyAsStringOption(invalidPropertyName)
            .isEmpty()) {
        logger.info("search for invalid property failed as expected")
    }
}