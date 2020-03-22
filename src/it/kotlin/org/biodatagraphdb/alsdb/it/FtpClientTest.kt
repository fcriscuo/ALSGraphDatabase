package org.biodatagraphdb.alsdb.it

import arrow.core.right
import org.biodatagraphdb.alsdb.lib.asRefinedFilePath
import org.biodatagraphdb.alsdb.service.datamining.FtpClient
import org.biodatagraphdb.alsdb.service.datamining.retrieveRemoteFileByFtpUrl

/**
 * Created by fcriscuo on 3/18/20.
 */

private fun testFtpClient( ftpUrl: String, destinationFile:String ){
    val localPath = destinationFile.asRefinedFilePath()
    if (localPath != null) {
        val retEither = retrieveRemoteFileByFtpUrl(ftpUrl, localPath)
        if (retEither.isRight()) {
            logger.info { retEither.right() }
            localPath.readFileAsStream().limit(4)
                    .forEach { line -> logger.info{line} }
            localPath.deleteFile()
        } else {
            logger.error { retEither.map { e -> e.toString() } }
        }
    } else {
        logger.error{"Invalid file path specified"}
    }
}

private fun testFtpClientDataClass(server: String, remoteFilePath: String, destinationFile: String){
    val localPath = destinationFile.asRefinedFilePath()
    if(localPath != null){
        val client = FtpClient(server)
        val retEither = client.downloadRemoteFile(remoteFilePath,localPath)
        if (retEither.isRight()) {
            logger.info { retEither.right() }
            localPath.readFileAsStream().limit(4)
                    .forEach { line -> logger.info{line} }
            localPath.deleteFile()
        } else {
            logger.error { retEither.map { e -> e.toString() } }
        }
    } else {
        logger.error{"Invalid file path specified"}
    }
}

fun main() {
    // valid test
    val ftpUrl = "https://www.uniprot.org/uniprot/?query=reviewed:yes+AND+organism:9606&columns=id,database(HGNC),datbase(IntAct),database(DisGeNET),database(Reactome),database(RefSeq)&format=tab"
    val validPath = "/tmp/gencodeV33.lncRNA.gz"
    testFtpClient(ftpUrl,validPath )
    // test should fail - bad destination file
    val invalidPath01 = "bad_file.txt"
    testFtpClient(ftpUrl,invalidPath01 )
    val invalidPath02 = "/tmp"
    testFtpClient(ftpUrl,invalidPath02 )
    // Test FtpClient class
    testFtpClientDataClass("ftp.ebi.ac.uk","/pub/databases/gencode/Gencode_human/release_33/gencode.v33.metadata.HGNC.gz",
        "/tmp/gencodev33_hgnc.gz")
}