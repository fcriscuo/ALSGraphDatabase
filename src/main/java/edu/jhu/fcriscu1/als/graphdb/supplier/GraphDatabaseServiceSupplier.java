package org.nygenome.als.graphdb.supplier;

import com.google.common.base.Supplier;
import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Consumer;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.Utils;

public class GraphDatabaseServiceSupplier implements Supplier<GraphDatabaseService> {

  private GraphDatabaseService graphDb;

  public enum RunMode {TEST, PROD,READ_ONLY}

  private Consumer<Path> configureTestGraphConsumer = (path) -> {
    try {
      // in test mode any existing database is cleared
      Utils.deleteDirectoryAndChildren(path);
    } catch (IOException e) {
      e.printStackTrace();
    }
    this.graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(path.toFile());
    registerShutdownHook(graphDb);
  };


  private Consumer<Path> configureProductionGraphConsumer = (path) -> {
    this.graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(path.toFile());
    registerShutdownHook(graphDb);
  };

  // read-only access to production database
  private Consumer<Path> configureReadOnlyGraphConsumer = (path) -> {
   this.graphDb = new GraphDatabaseFactory()
        .newEmbeddedDatabaseBuilder(path.toFile())
        .setConfig(GraphDatabaseSettings.read_only, "true")
        .newGraphDatabase();
    registerShutdownHook(graphDb);
  };

  public GraphDatabaseServiceSupplier(RunMode runMode) {
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
          GraphDatabaseService graphDb = new GraphDatabaseServiceSupplier(RunMode.READ_ONLY).get();
          try (Transaction tx = graphDb.beginTx()) {
            graphDb.getAllLabels()
                .stream()
                .limit(200L)
                .forEach(label -> System.out.println(label.name()));
            System.out.println("Node count = " +graphDb.getAllNodes().stream().count());
          } catch (Exception e) {
            e.printStackTrace();
          }
        });
  }

  private void registerShutdownHook(final GraphDatabaseService graphDb) {
    // Registers a shutdown hook for the Neo4j instance so that it shuts
    // down nicely when the VM exits (even if you "Ctrl-C" the
    // running application).
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        graphDb.shutdown();
      }
    });
  }

  @Override
  public GraphDatabaseService get() {
    return graphDb;
  }
}
