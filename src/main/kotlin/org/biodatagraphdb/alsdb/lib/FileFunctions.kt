package org.biodatagraphdb.alsdb.lib

import arrow.core.Either
import arrow.core.rightIfNotNull
import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils
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
    const val FTP_USER = "anonymous"
    const val FTP_PASSWORD = "sdrc1888@gmail.com" //TODO get from environment
    const val FTP_PORT = 21
    val BASE_DATA_PATH = DatafilesPropertiesService.resolvePropertyAsPathOption("base.data.path").orNull()
    val fileSeparator = System.getProperty("file.separator")

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

    fun resolveDataSourceFromUrl( url: URL): String {
        val host = url.host.toUpperCase()
      return  when  {
          host.contains("EBI") -> "EBI"
          host.contains("ENSEMBL") -> "ENSEMBL"
          host.contains("GENCODE") -> "GenCode"
          host.contains("INTACT") -> "IntAct"
          host.contains("UNIPROT") -> "UniProt"
          host.contains("DRUGBANK") -> "DrugBank"
          host.contains("SEQUENCEONTOLOGY") -> "SequenceOntology"
          host.contains("PHARMGKB") -> "PharmgKB"
          //http://juniper.health.unm.edu/tcrd/download/TCRDexp_v4.6.10.csv
          host.contains("juniper.health.unm.ed".toUpperCase()) -> "DisGeNET"
          host.contains("disgenet".toUpperCase()) -> "DisGeNET"
          else -> "UNSPECIFIED"
      }
    }
//   ftp://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_33/gencode.v33.long_noncoding_RNAs.gff3.gz
    fun resolveLocalFileNameFromUrl(url: URL): Either<Exception,String> {
        if( BASE_DATA_PATH != null){
            val dataSourceName = resolveDataSourceFromUrl(url)
            val localPath = BASE_DATA_PATH.toString() + fileSeparator + dataSourceName  + url.path
            return Either.right(localPath)
        } else {
            return Either.left(IOException("base.data.path property not defined"))
        }
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
    Returns: the number of lines in the retrieved file
     */
    fun retrieveRemoteFileByFtpUrl(ftpUrl: String): Either<Exception, String> {
        val urlConnection = URL(ftpUrl)
        val local = resolveLocalFileNameFromUrl(urlConnection)
        when (local) {
            is Either.Right -> {
                val localFilePath = local.b
                urlConnection.openConnection()
                try {
                    FileUtils.copyInputStreamToFile(urlConnection.openStream(), File(localFilePath))
                    if (FilenameUtils.getExtension(localFilePath) == "gz") {
                        AlsFileUtils.gunzipFile(localFilePath)
                    }
                    return Either.right("$ftpUrl downloaded to  $localFilePath")
                } catch (e: Exception) {
                    return Either.left(e)
                }
            }
            is Either.Left -> return Either.left(local.a)
        }
    }
    /*
    unzip a file
    the expanded file is given the same filename without the .gz extension
    the compressed file is deleted
    this code is a simple refactoring of a Java example
     */
    fun gunzipFile (compressedFile: String): Either<Exception,String> {
        val buffer = ByteArray(1024)
        val expandedFile = FilenameUtils.removeExtension(compressedFile)
        val gzis = GZIPInputStream(FileInputStream(compressedFile))
        val out = FileOutputStream(expandedFile)
        try{
            var len:Int
            while(true){
                len = gzis.read(buffer)
                if (len > 0){
                    out.write(buffer,0,len)
                } else {
                    //delete compressed file
                    FileUtils.forceDelete(File(compressedFile))
                    return Either.right("$compressedFile expanded to $expandedFile")
                }
            }
        }catch (e:Exception){
            return Either.left(e)
        } finally {
            gzis.close()
            out.close()
        }
    }
}

fun main() {
    //val url = URL("ftp://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_33/gencode.v33.long_noncoding_RNAs.gff3.gz")
    //println("Local file name =  ${AlsFileUtils.resolveLocalFileNameFromUrl(url)}")
    // https://www.disgenet.org/static/disgenet_ap1/files/downloads/curated_gene_disease_associations.tsv.gz
    //http://juniper.health.unm.edu/tcrd/download/TCRDexp_v4.6.10.csv
    println(AlsFileUtils.retrieveRemoteFileByFtpUrl("https://www.disgenet.org/static/disgenet_ap1/files/downloads/curated_gene_disease_associations.tsv.gz"))
}