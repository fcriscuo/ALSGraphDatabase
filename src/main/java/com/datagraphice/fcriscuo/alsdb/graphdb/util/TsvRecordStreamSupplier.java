package com.datagraphice.fcriscuo.alsdb.graphdb.util;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class TsvRecordStreamSupplier implements Supplier<Stream<CSVRecord>> {

    private Stream<CSVRecord> recordStream;
    private static final Logger log = Logger.getLogger(TsvRecordStreamSupplier.class);

    public TsvRecordStreamSupplier(@Nonnull  Path aPath) {
        try(Reader in = new FileReader(aPath.toString())){
            CSVParser parser = CSVParser.parse(aPath.toFile(), Charset.defaultCharset(),
                    CSVFormat.TDF.withFirstRecordAsHeader().withQuote(null).withIgnoreEmptyLines());
            this.recordStream = parser.getRecords().stream();
        }catch (IOException e) {
          System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }


   /*
   Constructs a TsvRecordStreamSupplier with a specified Array of column
   headings. This is to support parsing of large TSV files that need to be split into
   multiple files.
   These files should not contain a header and the first file from the Linux split
   command may need to be manually edited to remove the original header.
    */
    public TsvRecordStreamSupplier(@Nonnull Path aPath,
        @Nonnull String...columnHeadings) {
      try(Reader in = new FileReader(aPath.toString())){
        CSVParser parser = CSVParser.parse(aPath.toFile(), Charset.defaultCharset(),
            CSVFormat.TDF.withHeader(columnHeadings).withQuote(null).withIgnoreEmptyLines());
        this.recordStream = parser.getRecords().stream();
      }catch (IOException e) {
        System.out.println(e.getMessage());
        e.printStackTrace();
      }
    }

    @Override
    public Stream<CSVRecord> get() {
        return this.recordStream;
    }

    public static void main(String[] args) {
       String filePathName = (args.length>0 )? args[0]: "/data/als/human_intact.tsv";
        Path aPath = Paths.get(filePathName);
        log.info("Processing csv file: " +filePathName);
        final Map<String,Integer> headerMap = new CsvHeaderSupplier(aPath).get();
        new TsvRecordStreamSupplier(aPath).get()
                .limit(100L)
                .forEach((record) -> {
                    headerMap.keySet().forEach((key) -> {
                        log.info("*** column: " +key +"  value=" +record.get(key));
                    });
                    log.info("--------------------------------------------------");
                });
    }
}
