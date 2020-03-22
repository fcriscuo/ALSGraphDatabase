package org.biodatagraphdb.alsdb.lib

import io.kotlintest.should
import io.kotlintest.specs.AbstractStringSpec
import org.apache.commons.io.filefilter.TrueFileFilter
import java.io.File
import java.nio.charset.Charset
import java.nio.file.Paths
import org.apache.commons.io.FileUtils as ApacheFileUtils

/**
 * Created by fcriscuo on 3/1/20.
 */
class FileFunctionsTest: AbstractStringSpec() {

    val parentDirectory = "${ApacheFileUtils.getTempDirectoryPath()}/testdir"
    val file1 = "$parentDirectory/testfile1.txt"
    val childDirectory = "$parentDirectory/subdir"
    val file2 = "$childDirectory/testfile2.txt"


    init{
        ApacheFileUtils.forceMkdir(File(parentDirectory) )
        ApacheFileUtils.write(File(file1),
                "Test file 001", Charset.defaultCharset())
        ApacheFileUtils.forceMkdir(File(childDirectory) )
        ApacheFileUtils.write(File(file2),
                "Test file 002", Charset.defaultCharset())

        "should find two (2) files"  should{
            ApacheFileUtils.listFiles(File(parentDirectory),
                    TrueFileFilter.INSTANCE,TrueFileFilter.INSTANCE)
                    .size == 2
        }

        "should find four (4) subdirectories and files" should {
            ApacheFileUtils.listFilesAndDirs(File(parentDirectory),
                    TrueFileFilter.INSTANCE,TrueFileFilter.INSTANCE)
                    .size == 4
        }

        "should delete directory and all children succesfully" should {
            AlsFileUtils.deleteDirectoryRecursively(Paths.get(parentDirectory)).isRight()
        }

        "should not find parent directory" should {
            !File(parentDirectory).exists()
        }



    }
}