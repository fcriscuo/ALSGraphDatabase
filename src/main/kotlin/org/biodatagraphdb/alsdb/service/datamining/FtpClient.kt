package org.biodatagraphdb.alsdb.service.datamining

import arrow.core.Either
import mu.KotlinLogging
import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils
import org.apache.commons.net.PrintCommandListener
import org.apache.commons.net.ftp.FTP
import org.apache.commons.net.ftp.FTPClient
import org.apache.commons.net.ftp.FTPReply
import org.biodatagraphdb.alsdb.lib.AlsFileUtils
import org.biodatagraphdb.alsdb.lib.RefinedFilePath
import java.io.FileOutputStream
import java.io.IOException
import java.io.PrintWriter
import java.net.URL

/**
 * Created by fcriscuo on 3/18/20.
 */

const val FTP_USER = "anonymous"
const val FTP_PASSWORD = "sdrc1888@gmail.com" //TODO get from environment
const val FTP_PORT = 21

private val logger = KotlinLogging.logger {}

/*
FTP client class responsible for establishing a anonymous connection to an FTP server
and retrieving a specified remote file to a local file.
If the specified local file already exists, it will be overwritten
 */
data class FtpClient(val server: String) {
    val ftp = FTPClient()

    init {
        ftp.addProtocolCommandListener(PrintCommandListener(PrintWriter(System.out)))
        ftp.enterLocalPassiveMode()
    }

    fun downloadRemoteFile(remoteFilePath: String, localFilePath: RefinedFilePath): Either<Exception, String> {
        ftp.connect(server, FTP_PORT)
        val replyCode = ftp.replyCode
        if (FTPReply.isPositiveCompletion(replyCode)) {
            ftp.login(FTP_USER, FTP_PASSWORD)
            ftp.setFileType(FTP.ASCII_FILE_TYPE)
            try {
                val outputStream = FileOutputStream(localFilePath.getPath().toFile(),false)
                ftp.retrieveFile(remoteFilePath,outputStream)
                when (localFilePath.exists()){
                    true -> return Either.right("Remote file: $remoteFilePath has been downloaded to ${localFilePath.filePathName}")
                    false -> return Either.left(IOException("Download of remote file: $remoteFilePath to ${localFilePath.filePathName} failed"))
                }
            } catch (e: Exception) {
                return Either.left(e)
            } finally {
                ftp.logout()
                ftp.disconnect()
            }
        }
        return Either.left(IOException("FTP server $server refused anonymous connection"))
    }
}

/*
Function to access a remote file via anonymous FTP and copy its contents to
the local filesystem at a specified location.
Parameters: ftpUrl - Complete URL for remote file
            localFilePath - local filesystem location
Returns: the number of lines in the retrieved file
 */
fun retrieveRemoteFileByFtpUrl(ftpUrl: String, localFilePath: RefinedFilePath): Either<Exception, String> {
    val urlConnection = URL(ftpUrl)
    urlConnection.openConnection()
    // the FileUtils method closes the input stream
    try {
        FileUtils.copyInputStreamToFile(urlConnection.openStream(), localFilePath.getPath().toFile())
        if (FilenameUtils.getExtension(localFilePath.filePathName) == "gz") {
           AlsFileUtils.gunzipFile(localFilePath.filePathName)
        }
        return Either.right("$ftpUrl downloaded to  $localFilePath")
    } catch (e: Exception) {
        return Either.left(e)
    }
}
