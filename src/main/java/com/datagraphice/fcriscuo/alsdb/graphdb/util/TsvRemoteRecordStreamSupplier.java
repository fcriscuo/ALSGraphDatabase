package com.datagraphice.fcriscuo.alsdb.graphdb.util;

import com.jcraft.jsch.JSch;
import java.io.IOException;
import java.io.Reader;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import edu.jhu.fcriscu1.als.graphdb.value.HumanTissueAtlas;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;

/*
A Supplier that will open a TSV file on a remote SFTP server and
convert the tab-delimited lines to a stream of CSVRecords
The SFTP Server has access to the NYGC /data filesystem which
has sufficient space for the larger ALS data files
 */
public class TsvRemoteRecordStreamSupplier extends RemoteRecordSupplier
    implements Supplier<Stream<CSVRecord>> {

    /*
    sftp_user = fcriscuolo
sftp_password = Fjc117842
sftp_server = fcriscuolo-vm
sftp_als_data_dir = /data/metronome/als
     */
    private Stream<CSVRecord> recordStream;
    private static final Logger log = Logger.getLogger(TsvRemoteRecordStreamSupplier.class);


    public TsvRemoteRecordStreamSupplier (@Nonnull String sftpServer,
        @Nonnull String fileName) {
      jsch = new JSch();
     try (Reader in = initRemoteReader( fileName)) {
        CSVParser parser = CSVParser.parse(in,
            CSVFormat.TDF.withFirstRecordAsHeader().withQuote(null).withIgnoreEmptyLines());
        this.recordStream = parser.getRecords().stream();
      } catch (IOException ioe) {
       ioe.printStackTrace();
     }

    }



    @Override
    public Stream<CSVRecord> get() {
        return this.recordStream;
    }

    public static void main(String[] args) {
       String filePathName = (args.length>0 )? args[0]: "HumanTissueAtlas.tsv";
        new TsvRemoteRecordStreamSupplier(
            FrameworkPropertyService.INSTANCE.getStringProperty("sftp_server"),
            filePathName).get()
                .limit(100L)
                .map(HumanTissueAtlas::parseCSVRecord)
                .forEach(System.out::println);
    }
}
