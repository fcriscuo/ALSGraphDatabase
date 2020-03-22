package org.biodatagraphdb.alsdb.supplier;

import com.google.common.base.Supplier;
import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Consumer;

import org.biodatagraphdb.alsdb.util.FrameworkPropertyService;
import org.biodatagraphdb.alsdb.util.Utils;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.biodatagraphdb.alsdb.util.AsyncLoggingService;


public class GraphDatabaseServiceLegacySupplier implements Supplier<GraphDatabaseService> {

  private GraphDatabaseService graphDb;

  public enum RunMode {TEST, PROD,READ_ONLY}

  /*
   private val DBPATH = File("target/neo4j-hello-db")
    private val dbName = org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
    private val managementService: DatabaseManagementService = DatabaseManagementServiceBuilder(DBPATH).build()
    private val graphDb: GraphDatabaseService = managementService.database(dbName)
   */

  private Consumer<Path> configureTestGraphConsumer = (path) -> {
    try {
      // in test mode any existing database is cleared
      Utils.deleteDirectoryAndChildren(path);
    } catch (IOException e) {
      e.printStackTrace();
    }
    DatabaseManagementService dbms = new DatabaseManagementServiceBuilder(path.toFile()).build();
    this.graphDb = dbms.database("test_alsdb");
    //this.graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(path.toFile());
    registerShutdownHook(dbms);
  };


  private Consumer<Path> configureProductionGraphConsumer = (path) -> {
    DatabaseManagementService dbms = new DatabaseManagementServiceBuilder(path.toFile()).build();
    this.graphDb = dbms.database("prod_alsdb");
    registerShutdownHook(dbms);
  };

  // read-only access to production database
  private Consumer<Path> configureReadOnlyGraphConsumer = (path) -> {
    DatabaseManagementService dbms = new DatabaseManagementServiceBuilder(path.toFile()).build();
   this.graphDb =dbms.database("ro_prod_alsdb");

    registerShutdownHook(dbms);
  };

  public GraphDatabaseServiceLegacySupplier(RunMode runMode) {
    switch (runMode) {
      case TEST:
        FrameworkPropertyService.INSTANCE.getOptionalPathProperty("testing.db.path")
            .ifPresent(configureTestGraphConsumer);
        AsyncLoggingService.logInfo("new test ALS Neo4j database will be created.");
        break;
      case PROD:
        FrameworkPropertyService.INSTANCE.getOptionalPathProperty("neo4j.db.path")
            .ifPresent(configureProductionGraphConsumer);
        AsyncLoggingService.logInfo("Production ALS Neo4j database will be created/updated.");
        break;
      case READ_ONLY:default:
        FrameworkPropertyService.INSTANCE.getOptionalPathProperty("neo4j.db.path")
            .ifPresent(configureReadOnlyGraphConsumer);
          AsyncLoggingService.logError("Read-only access to production graph database");
    }
  }



  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("readonly.db.path")
        .ifPresent(path -> {
          GraphDatabaseService graphDb = new GraphDatabaseServiceLegacySupplier(RunMode.READ_ONLY).get();
          try (Transaction tx = graphDb.beginTx()) {
            tx.getAllLabels()
                .forEach(label -> System.out.println(label.name()));
            System.out.println("Node count = " +tx.getAllNodes().stream().count());
          } catch (Exception e) {
            e.printStackTrace();
          }
        });
  }

  private void registerShutdownHook(final DatabaseManagementService dbms) {
    // Registers a shutdown hook for the Neo4j instance so that it shuts
    // down nicely when the VM exits (even if you "Ctrl-C" the
    // running application).
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        dbms.shutdown();
      }
    });
  }

  @Override
  public GraphDatabaseService get() {
    return graphDb;
  }
}
