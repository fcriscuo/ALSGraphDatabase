package org.nygenome.als.graphdb.integration;


/*

 */

import static org.neo4j.udc.UsageDataKeys.Features.bolt;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.nygenome.als.graphdb.consumer.GraphDataConsumer;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;

/*
Test implemented as a BiConsumer that invokes a specified GraphDataConsumer subclass
using a specified file Path as a data source
Creates a temporary Neo4j graph
 */
public class TestGraphDataConsumer implements BiConsumer<Path, GraphDataConsumer> {

  private  final Path databasePath;
  private final GraphDatabaseService graphDb;

  public TestGraphDataConsumer() {
    databasePath =
        Paths.get(FrameworkPropertyService.INSTANCE.getStringProperty("testing.db.path"));
    GraphDatabaseSettings.BoltConnector bolt = GraphDatabaseSettings.boltConnector( "0" );
     graphDb = new GraphDatabaseFactory()
        .newEmbeddedDatabaseBuilder( databasePath.toFile() )
        .setConfig( bolt.type, "BOLT" )
        .setConfig( bolt.enabled, "true" )
        .setConfig( bolt.address, "localhost:7687" )
        .newGraphDatabase();
     AsyncLoggingService.logInfo("Graph database established at: "
     +databasePath.toString());
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

    Stopwatch stopwatch = Stopwatch.createStarted();
    graphDataConsumer.accept(path);
    stopwatch.stop();
    System.out.println( "GraphDataConsumer: " + graphDataConsumer.getClass().getName()
        +"  required : " + stopwatch.elapsed(TimeUnit.SECONDS) +" seconds.");
    try (Transaction tx = graphDb.beginTx()) {
      AsyncLoggingService.logInfo("++++total graph node count = " +
          graphDb.getAllNodes().stream().count() );
    } catch (Exception e){
      e.printStackTrace();
    }
    graphDb.shutdown();
  }
}
