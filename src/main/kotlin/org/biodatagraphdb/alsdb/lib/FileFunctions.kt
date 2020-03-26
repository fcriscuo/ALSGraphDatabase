package org.biodatagraphdb.alsdb.lib

import arrow.core.Either
import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils
import org.biodatagraphdb.alsdb.lib.AlsFileUtils.retrieveRemoteFileByDatafileProperty
import org.biodatagraphdb.alsdb.service.property.DatafilesPropertiesService
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.IOException
import java.net.URL
import java.nio.file.Path
import java.util.zip.GZIPInputStream
import javax.annotation.Nullable

/**
 * A collection of File-related functions
 * Created by fcriscuo on 2/25/20.
 */
object AlsFileUtils {
    val BASE_DATA_DIRECTORY = DatafilesPropertiesService.resolvePropertyAsString("base.data.path") ?: "/tmp"
    val BASE_SUBDIRECTORY_NAME = DatafilesPropertiesService.resolvePropertyAsString("base.subdirectory.name") ?: "data"
    val fileSeparator = System.getProperty("file.separator")
    val compressedFileExtensions = listOf<String>("gz","zip")

    @JvmStatic
/*
Function to delete a directory recursively
 */
    fun deleteDirectoryRecursively(path: Path): Either<Exception, String> {
        return try {
            FileUtils.deleteDirectory(path.toFile())
            Either.Right("${path.fileName} and children have been deleted")
        } catch (e: Exception) {
            Either.Left(e)
        }
    }

    fun resolveDataSourceFromUrl(url: URL): String {
        val host = url.host.toUpperCase()
        return when {
            host.contains("EBI") -> "EBI"
            host.contains("ENSEMBL") -> "ENSEMBL"
            host.contains("GENCODE") -> "GenCode"
            host.contains("INTACT") -> "IntAct"
            host.contains("UNIPROT") -> "UniProt"
            host.contains("DRUGBANK") -> "DrugBank"
            host.contains("SEQUENCEONTOLOGY") -> "SequenceOntology"
            host.contains("PHARMGKB") -> "PharmgKB"
            host.contains("juniper.health.unm.ed".toUpperCase()) -> "DisGeNET"
            host.contains("disgenet".toUpperCase()) -> "DisGeNET"
            else -> "UNSPECIFIED"
        }
    }

    fun resolveLocalFileNameFromPropertyPair(propertyPair: Pair<String, String>): Either<Exception, String> {
        if (BASE_DATA_DIRECTORY != null) {
            val subdirectory = resolveDataSubDirectoryFromPropertyName(propertyPair.first)
            val localPath = subdirectory + fileSeparator + resolveSourceFileName(propertyPair.second)
            return Either.right(localPath)
        } else {
            return Either.left(IOException("base.data.path property not defined"))
        }
    }

    fun resolveSourceFileName(remotePath: String) =
            remotePath.split(fileSeparator).last()


    fun resolveDataSubDirectoryFromPropertyName(propertyName: String): String {
        if (propertyName.startsWith(BASE_SUBDIRECTORY_NAME.toString())) {
            return BASE_DATA_DIRECTORY.toString() + fileSeparator + propertyName.replace(".", fileSeparator)
        }
        return BASE_DATA_DIRECTORY.toString() + fileSeparator + BASE_SUBDIRECTORY_NAME.toString() +
                fileSeparator + propertyName.replace(".", fileSeparator)
    }

    /*
    Read the contents of a resource file as a Stream
     */
    @Nullable
    fun readFileAsLinesUsingGetResourceAsStream(fileName: String) =
            this::class.java.getResourceAsStream(fileName).bufferedReader().readLines()

    /*
    Function to access a remote file via anonymous FTP and copy its contents to
    the local filesystem at a specified location.
    Parameters: ftpUrl - Complete URL for remote file
    Returns: Either whose Left side is an Exception, and whose Right side contains a success message
     */
    fun retrieveRemoteFileByDatafileProperty(propertyPair: Pair<String, String>): Either<Exception, String> {
        val urlConnection = URL(propertyPair.second)
        val local = resolveLocalFileNameFromPropertyPair(propertyPair)
        when (local) {
            is Either.Right -> {
                val localFilePath = local.b
                urlConnection.openConnection()
                try {
                    FileUtils.copyInputStreamToFile(urlConnection.openStream(), File(localFilePath))
                    if (FilenameUtils.getExtension(localFilePath) in compressedFileExtensions) {
                        gunzipFile(localFilePath)
                    }
                    return Either.right("${propertyPair.second} downloaded to  $localFilePath")
                } catch (e: Exception) {
                    return Either.left(e)
                }
            }
            is Either.Left -> return Either.left(local.a)
        }
    }

    /*
    unzip a compressed file
    the expanded file is given the same filename without the .gz or .zip extension
    and the compressed file is deleted
    this code is a simple refactoring of a Java example
     */
    //TODO: make this asynchronous
    fun gunzipFile(compressedFile: String): Either<Exception, String> {
        val buffer = ByteArray(1024)
        val expandedFile = FilenameUtils.removeExtension(compressedFile)
        val gzis = GZIPInputStream(FileInputStream(compressedFile))
        val out = FileOutputStream(expandedFile)
        try {
            var len: Int
            while (true) {
                len = gzis.read(buffer)
                if (len > 0) {
                    out.write(buffer, 0, len)
                } else {
                    //delete compressed file
                    FileUtils.forceDelete(File(compressedFile))
                    return Either.right("$compressedFile expanded to $expandedFile")
                }
            }
        } catch (e: Exception) {
            return Either.left(e)
        } finally {
            gzis.close()
            out.close()
        }
    }
}

fun main() {
    ///data.disgenet.curated.gene-disease.associations=https://www.disgenet.org/static/disgenet_ap1/files/downloads/curated_gene_disease_associations.tsv.gz
    val result = retrieveRemoteFileByDatafileProperty(Pair("data.mint.human",
            "http://www.ebi.ac.uk/Tools/webservices/psicquic/mint/webservices/current/search/query/species:human" ))
    when (result){
        is Either.Right -> println(result.b)
        is Either.Left -> println(result.a.message)
    }
}
