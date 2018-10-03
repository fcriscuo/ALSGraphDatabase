package org.nygenome.als.graphdb.app;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Suppliers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import org.neo4j.graphdb.GraphDatabaseService;
import org.nygenome.als.graphdb.consumer.UniProtValueConsumer;
import org.nygenome.als.graphdb.supplier.GraphDatabaseServiceSupplier;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;

/*
Represents a Java Consumer that will process a set of
TSV and CSV files and load their contents into a Neo4j database
at a specified Path
 */
public class ALSDatabaseImportApp  {

  private  GraphDatabaseService graphDb;

  public ALSDatabaseImportApp(@Nonnull Path dbPath) {
   graphDb = Suppliers.memoize(new GraphDatabaseServiceSupplier(dbPath))
        .get();
  }

  private void importData() {
    Stopwatch stopwatch = Stopwatch.createStarted();
    //Uniprot data
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("UNIPROT_HUMAN_FILE")
        .ifPresent(new UniProtValueConsumer());

    stopwatch.stop();
    AsyncLoggingService.logInfo("Creation of the ALS Neo4j database required "
    +stopwatch.elapsed(TimeUnit.SECONDS) +"seconds");
  }

  public static void main(String[] args) {
    String pathName = (args.length>0)?args[0]
        : FrameworkPropertyService.INSTANCE.getStringProperty("neo4j.db.path");
    AsyncLoggingService.logInfo("ALS Database will be created at; " +pathName);
    ALSDatabaseImportApp app = new ALSDatabaseImportApp(Paths.get(pathName));


  }
}
