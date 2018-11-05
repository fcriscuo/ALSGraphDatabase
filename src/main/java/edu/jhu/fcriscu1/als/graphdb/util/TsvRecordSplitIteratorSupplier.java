package edu.jhu.fcriscu1.als.graphdb.util;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.twitter.logging.Logger;

import javax.annotation.Nonnull;
import javax.validation.constraints.NotNull;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
/*
Represents a Splititerator that will supply a Stream of CsvRecord objects
from a specified  tsv file
This is to support tab delimited files that are too large for the available
JVM heap size
 */
public class TsvRecordSplitIteratorSupplier implements Supplier<Stream<CSVRecord>> {
  private Logger log = Logger.get(TsvRecordSplitIteratorSupplier.class);
  private final Path filePath;
  private  String[] columnHeadings;


  public TsvRecordSplitIteratorSupplier(@NotNull Path aPath ,
      @Nonnull String...headings) {
    this.filePath = aPath;
    this.columnHeadings = headings;
  }

  @Override
  public Stream<CSVRecord> get() {
    log.info("Processing tab delimited record from file: " +this.filePath.toString());
    // cannot reference Path resources outside block if they're closed
    // close them later

    UncheckedCloseable close = null;
    try {
      Reader in = new FileReader(this.filePath.toString());
      close=UncheckedCloseable.wrap(in);
      CSVParser parser = CSVParser.parse(this.filePath.toFile(), Charset.defaultCharset(),
          CSVFormat.TDF.withHeader(columnHeadings).withQuote(null).withIgnoreEmptyLines());
     final Iterator<CSVRecord> iter = parser.iterator();

      return StreamSupport.stream(new Spliterators.AbstractSpliterator<CSVRecord>(
          Long.MAX_VALUE, Spliterator.ORDERED) {
        @Override
        public boolean tryAdvance(Consumer<? super CSVRecord> action) {
          try {
            if (!iter.hasNext())
              return false;
            action.accept(iter.next());
            return true;
          } catch (Exception ex) {
            throw new RuntimeException(ex);
          }
        }
      }, false).onClose(close);

    } catch (IOException e) {
      if(close!=null)
        try { close.close(); } catch(Exception ex) { e.addSuppressed(ex); }
      log.error(e.getMessage());
    }
    return Stream.empty();
  }

  // main method for stand alone testing
  //TODO: modify test to use resource files for portability
  public static void main(String[] args) {
   Path testPath = Paths.get("/data/als/human_intact.tsv");
    String[] columnHeadings = Utils.resolveColumnHeadingsFunction
        .apply(Paths.get("/data/als/heading_intact.txt"));

    AtomicInteger count = new AtomicInteger(0);
    new TsvRecordSplitIteratorSupplier(testPath, columnHeadings).get()
        .forEach((record) -> {
          try {
            count.getAndIncrement();
            if (count.get() % 10000 == 0 ) {
              System.out.println(count + "  " +record.get("#ID(s) interactor A"));
            }
          } catch (Exception e) {
            e.getMessage();
            e.printStackTrace();
          }
        });
    System.out.println( "Number of intact records = " + count.intValue() );
  }
}
