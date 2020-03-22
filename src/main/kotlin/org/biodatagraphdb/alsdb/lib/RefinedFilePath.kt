package org.biodatagraphdb.alsdb.lib

import arrow.core.Either
import org.apache.commons.io.FilenameUtils
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.stream.Stream

/**
 * Inline class and supporting functions to validate that a specified file path name
 * is valid for the local file system
 * n.b. specified file path names must be absolute (e.g. /tmp/xyz.txt not xyz.txt)
 * Created by fcriscuo on 3/18/20.
 */


inline class RefinedFilePath (val filePathName: String) {

    companion object: Refined<String> {
        override fun isValid(filePathName: String): Boolean {
            val dirPath = Paths.get(FilenameUtils.getFullPathNoEndSeparator(filePathName))
            val fileName = FilenameUtils.getName(filePathName)
            val filePrefix = FilenameUtils.getPrefix(filePathName)
            if(filePrefix == File.separator && fileName != null
                    && Files.isWritable(dirPath)) {
                return true
            }
            return false
        }
    }

    fun readFileAsStream(): Stream<String> = Files.lines(this.getPath())

    fun getPath(): Path = Paths.get(filePathName)

    fun exists():Boolean = File(filePathName).exists()

    fun deleteFile(): Either<Exception, String> {
        try {
            Files.deleteIfExists(this.getPath())
            return Either.right("$filePathName has been deleted")
        } catch (e: Exception){
            return Either.left(e)
        }
    }
}

fun String.asRefinedFilePath(): RefinedFilePath? =
        if(RefinedFilePath.isValid(this)) RefinedFilePath(this)
            else null