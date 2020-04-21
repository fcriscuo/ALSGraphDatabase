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
            logger.error { retEither.map { e -> e } }
        }
    } else {
        logger.error{"Invalid file path specified"}
    }
}

fun main() {
    // valid test
    //val ftpUrl = "ftp://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_33/gencode.v33.long_noncoding_RNAs.gff3.gz"
    //val validPath = "/tmp/gencodeV33.lncRNA.gz"
//    val ftpUrl = "https://amp.pharm.mssm.edu/static/hdfs/harmonizome/data/cosmiccnv/gene_list_terms.txt.gz"
//    val validPath = "/tmp/GO/gene_list_terms.txt.gz"
//    testFtpClient(ftpUrl,validPath )
    // test should fail - bad destination file
//    val invalidPath01 = "bad_file.txt"
//    testFtpClient(ftpUrl,invalidPath01 )
//    val invalidPath02 = "/tmp"
//    testFtpClient(ftpUrl,invalidPath02 )
//    // Test FtpClient class
//    testFtpClientDataClass("ftp.ebi.ac.uk","/pub/databases/gencode/Gencode_human/release_33/gencode.v33.metadata.HGNC.gz",
//        "/tmp/gencodev33_hgnc.gz")
       testFtpClientDataClass("amp.pharm.mssm.edu","/static/hdfs/harmonizome/data/cosmiccnv/gene_list_terms.txt.gz",
        "/tmp/gene_list_terms.txt.gz")
}