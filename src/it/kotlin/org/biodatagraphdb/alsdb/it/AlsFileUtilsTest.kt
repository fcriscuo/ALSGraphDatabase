package org.biodatagraphdb.alsdb.it

import org.biodatagraphdb.alsdb.lib.AlsFileUtils
import org.biodatagraphdb.alsdb.service.property.DatafilesPropertiesService

/**
 * Created by fcriscuo on 3/20/20.
 * An integration test to test retreiving data files from Web reesources and mapping
 * their contents to the local filesystem
 * This test will retrieve all DisGeNET files listed in datafiles.properties
 */

fun main() {
    DatafilesPropertiesService.filterProperties("disgenet").forEach {
        println("URL: $it")
        println(AlsFileUtils.retrieveRemoteFileByDatafileProperty(it))
    }
}