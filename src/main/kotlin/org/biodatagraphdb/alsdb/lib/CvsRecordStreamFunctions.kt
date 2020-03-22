package org.biodatagraphdb.alsdb.lib

import mu.KotlinLogging
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.commons.csv.CSVRecord
import java.io.FileReader
import java.io.IOException
import java.nio.charset.Charset
import java.nio.file.Path
import java.util.*
import java.util.function.Consumer
import java.util.function.Supplier
import java.util.stream.Stream
import java.util.stream.StreamSupport
import javax.annotation.Nonnull

private fun delimitedRecordStreamSupplier(path: Path, format: CSVFormat): Stream<CSVRecord> {
    val parser = CSVParser.parse(path, Charset.defaultCharset(),
            format.withFirstRecordAsHeader())
    return parser.records.stream()
}
/*
 Support legacy clients that expect a Supplier implementation
 */
class CsvRecordStreamSupplier(path: Path) : Supplier<Stream<CSVRecord>> {
    private val recordStream = delimitedRecordStreamSupplier(path, CSVFormat.RFC4180)
    override fun get(): Stream<CSVRecord> = recordStream
}

class TsvRecordStreamSupplier(path: Path) : Supplier<Stream<CSVRecord>> {
    private val recordStream = delimitedRecordStreamSupplier(path, CSVFormat.TDF)
    override fun get(): Stream<CSVRecord> = recordStream
}

class Gff3RecordStreamSupplier(val path: Path): Supplier<Stream<CSVRecord>> {
    // accommodate comment lines in GFF3 formated files
    override fun get(): Stream<CSVRecord> = delimitedRecordStreamSupplier(path,
            CSVFormat.TDF.withCommentMarker('#'))
}

private fun delimitedFileHeaderMapProducer(path: Path, format: CSVFormat): Map<String, Int> {
    var headerMap: Map<String, Int> = emptyMap()
    try {
        FileReader(path.toString()).use {
            val parser = CSVParser.parse(path.toFile(), Charset.defaultCharset(),
                    format.withFirstRecordAsHeader())
            headerMap = parser.headerMap
        }
    } catch (e: IOException) {
        e.printStackTrace()
    } finally {
        return headerMap
    }
}

/**
 * Responsible for generating s Stream of Apache Commons CVSRecord objects from a
 * specified Path (i.e. file)
 * The extension of the file is used to determine the delimiter used; comma or tab
 * This class uses a SplitIterator to process the data and is intended for very
 * large files
 * Created by fcriscuo on 2/27/20.
 */
private class CsvRecordSplitIterator(val parser: CSVParser) : Spliterator<CSVRecord> {
    override fun estimateSize(): Long = Long.MAX_VALUE

    override fun characteristics(): Int = Spliterator.DISTINCT or Spliterator.NONNULL or Spliterator.IMMUTABLE

    override fun tryAdvance(action: Consumer<in CSVRecord>?): Boolean {
        val iter: Iterator<CSVRecord> = parser.iterator()
        if (!iter.hasNext()) return false
        action?.accept(iter.next())
        return true
    }

    override fun trySplit(): Spliterator<CSVRecord> {
        TODO("Not yet implemented")
    }
}

class DelimitedRecordSplitIteratorSupplier(val path: Path, @Nonnull vararg headings: String) :
        Supplier<Stream<CSVRecord?>> {
    var csvFormat: CSVFormat = if (path.toFile().extension == "csv") {
        CSVFormat.RFC4180
    } else {
        CSVFormat.TDF
    }
    var columnHeadings = headings

    companion object {
        val logger = KotlinLogging.logger {}
    }

    override fun get(): Stream<CSVRecord?> {
        logger.info { "Processing delimited file: ${path.fileName}" }
        val parser = CSVParser.parse(path.toFile(), Charset.defaultCharset(),
                csvFormat.withHeader(*columnHeadings).withQuote(null).withIgnoreEmptyLines())
        val splitIter = CsvRecordSplitIterator(parser)
        return StreamSupport.stream(splitIter,false)
    }
}