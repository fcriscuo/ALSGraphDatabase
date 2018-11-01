package org.nygenome.als.graphdb.util;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;

/*
Supplier that provides a Map of column headings (keys) and column indices
(values) for a specified csv file Path
 */
public class CsvHeaderSupplier implements Supplier<Map<String,Integer>> {
    private static final Logger log = Logger.getLogger(CsvHeaderSupplier.class);
    Map<String,Integer> headerMap;
    public CsvHeaderSupplier(@Nonnull Path aPath) {
        try(Reader in = new FileReader(aPath.toString())){
            CSVParser parser = CSVParser.parse(aPath.toFile(), Charset.defaultCharset(),
                    CSVFormat.RFC4180.withFirstRecordAsHeader());
            this.headerMap = parser.getHeaderMap();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Map<String,Integer> get() {
        return this.headerMap;
    }

    public static void main(String[] args) {
        Path aPath = Paths.get("/data/als/drug_carrier_uniprot_links.csv" );
        Map<String,Integer> headerMap = new CsvHeaderSupplier(aPath).get();
        headerMap.keySet().forEach(System.out::println);
    }
}
