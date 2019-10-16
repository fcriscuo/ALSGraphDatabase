package com.datagraphice.fcriscuo.alsdb.graphdb.util;

import com.jcraft.jsch.JSch;
import com.twitter.logging.Logger;
import java.io.IOException;
import java.io.Reader;
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
import javax.annotation.Nonnull;
import javax.validation.constraints.NotNull;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/*
A Supplier that will supply a Stream of CSVRecords from a sftp server
This support allows access to large delimited files on a server
that has access to the large /data repository
The use of a Splititerator to process the files avoids out of memory exceptions
This is to support tab delimited files that are too large for the available
JVM heap size

 */
public class RemoteTsvRecordSplitIteratorSupplier
  extends RemoteRecordSupplier
    implements Supplier<Stream<CSVRecord>> {
  private Logger log = Logger.get(RemoteTsvRecordSplitIteratorSupplier.class);
  private final Path filePath;
  private  String[] columnHeadings;
  private Stream<CSVRecord> recordStream;

  public RemoteTsvRecordSplitIteratorSupplier(@Nonnull String sftpServer,
      @NotNull Path aPath ,
      @Nonnull String...headings) {
    this.filePath = aPath;
    this.columnHeadings = headings;
      this.jsch = new JSch();
      this.sftpServer = sftpServer;
  }

  @Override
  public Stream<CSVRecord> get() {
    log.info("Processing tab delimited record from file: " +this.filePath.toString());
    // cannot reference Path resources outside block if they're closed
    // close them later

    UncheckedCloseable close = null;
    try( Reader in = initRemoteReader(filePath.toString())) {
      close=UncheckedCloseable.wrap(in);
      CSVParser parser = CSVParser.parse(in,
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
    String remoteFileDirectory = FrameworkPropertyService.INSTANCE
        .getStringProperty("sftp_als_data_dir");
   Path testPath = Paths.get("human_intact.tsv");
    String[] columnHeadings = Utils.resolveColumnHeadingsFunction
        .apply(Paths.get("/data/als/heading_intact.txt"));

    AtomicInteger count = new AtomicInteger(0);
    new RemoteTsvRecordSplitIteratorSupplier(
        FrameworkPropertyService.INSTANCE.getStringProperty("sftp_server"),
        testPath, columnHeadings).get()
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
