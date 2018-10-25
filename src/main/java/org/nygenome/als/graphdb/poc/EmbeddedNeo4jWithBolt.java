package org.nygenome.als.graphdb.poc;

import java.io.File;
import java.io.IOException;

import java.nio.file.Paths;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileUtils;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;

public class EmbeddedNeo4jWithBolt
{


  private static final File DB_PATH = new File(
      FrameworkPropertyService.INSTANCE.getStringProperty("neo4j.db.path")  );

  public static void main( final String[] args ) throws IOException
  {
    System.out.println( "Starting database ..." );
   // FileUtils.deleteRecursively( DB_PATH );

    // START SNIPPET: startDb
    GraphDatabaseSettings.BoltConnector bolt = GraphDatabaseSettings.boltConnector( "0" );

    GraphDatabaseService  graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( DB_PATH )
        .setConfig( GraphDatabaseSettings.read_only, "true" )
        .newGraphDatabase();

//    GraphDatabaseService graphDb = new GraphDatabaseFactory()
//        .newEmbeddedDatabaseBuilder( DB_PATH )
//        .setConfig( bolt.type, "BOLT" )
//        .setConfig( bolt.enabled, "true" )
//        .setConfig( bolt.address, "localhost:7687" )
//        .newGraphDatabase();
    try (Transaction tx = graphDb.beginTx()) {
      AsyncLoggingService.logInfo("++++Node count = " +
          graphDb.getAllNodes().stream().count() );
      graphDb.getAllLabels().stream().forEach(
          label -> System.out.println(label.name()));



    } catch (Exception e){
      e.printStackTrace();
    }

    graphDb.shutdown();
  }
}
