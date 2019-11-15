package org.biodatagraphdb.alsdb.util;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;

public class CsvRecordStreamSupplier implements Supplier<Stream<CSVRecord>> {

    private Stream<CSVRecord> recordStream;
    private static final Logger log = Logger.getLogger(CsvRecordStreamSupplier.class);

    public CsvRecordStreamSupplier(@Nonnull  Path aPath) {
        try(Reader in = new FileReader(aPath.toString())){
            CSVParser parser = CSVParser.parse(aPath.toFile(), Charset.defaultCharset(),
                    CSVFormat.RFC4180.withFirstRecordAsHeader());
            this.recordStream = parser.getRecords().stream();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Stream<CSVRecord> get() {
        return this.recordStream;
    }

    public static void main(String[] args) {
       String filePathName = (args.length>0 )? args[0]: "/data/als/UniProt2Reactome.tsv";
        Path aPath = Paths.get(filePathName);
        log.info("Processing csv file: " +filePathName);
        final Map<String,Integer> headerMap = new CsvHeaderSupplier(aPath).get();
        new CsvRecordStreamSupplier(aPath).get()
                .limit(100L)
                .forEach((record) -> {
                    headerMap.keySet().forEach((key) -> {
                        log.info("*** column: " +key +"  value=" +record.get(key));
                    });
                    log.info("--------------------------------------------------");
                });
    }
}
