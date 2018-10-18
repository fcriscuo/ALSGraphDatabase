package org.nygenome.als.graphdb.integration;


/*

 */

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Suppliers;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.neo4j.graphdb.GraphDatabaseService;
import org.nygenome.als.graphdb.consumer.GraphDataConsumer;
import org.nygenome.als.graphdb.supplier.GraphDatabaseServiceSupplier;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;

/*
Test implemented as a BiConsumer that invokes a specified GraphDataConsumer subclass
using a specified file Path as a data source
Creates a temporary Neo4j graph
 */
public class TestGraphDataConsumer implements BiConsumer<Path, GraphDataConsumer> {

  private  final Path databasePath;

 public TestGraphDataConsumer() {
    databasePath =
        Paths.get(FrameworkPropertyService.INSTANCE.getStringProperty("testing.db.path"));
   try {
     Files.createDirectories(databasePath);
   } catch (IOException e) {
     AsyncLoggingService.logError(e.getMessage());
     e.printStackTrace();
   }
 }

  @Override
  public void accept(Path path, GraphDataConsumer graphDataConsumer) {
    Preconditions.checkArgument(null != path,
        "A Path to an input file is required");
    Preconditions.checkArgument(Files.exists(path, LinkOption.NOFOLLOW_LINKS),
        "File "+path.toString() +" is invalid");
    Preconditions.checkArgument(null != graphDataConsumer,
        "A GraphDataConsumer implementation is required");

    GraphDatabaseService graphDb = Suppliers.memoize(new GraphDatabaseServiceSupplier(databasePath))
        .get();
    Stopwatch stopwatch = Stopwatch.createStarted();
    graphDataConsumer.accept(path);
    stopwatch.stop();
    System.out.println( "GraphDataConsumer: " + graphDataConsumer.getClass().getName()
        +"  required : " + stopwatch.elapsed(TimeUnit.SECONDS) +" seconds.");

    System.out.println("Shutting down database ...");
    graphDb.shutdown();
  }
}
