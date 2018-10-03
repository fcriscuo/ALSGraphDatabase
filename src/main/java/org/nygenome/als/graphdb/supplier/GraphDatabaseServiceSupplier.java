package org.nygenome.als.graphdb.supplier;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.nygenome.als.graphdb.util.Utils;

import javax.annotation.Nonnull;

public class GraphDatabaseServiceSupplier implements Supplier<GraphDatabaseService> {

  private GraphDatabaseService graphDb;

  public GraphDatabaseServiceSupplier(@Nonnull  Path path) {

    Preconditions.checkArgument(Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS),
        path +" is not a Path to a directory");
    // clear out any existing database
    try {
      Utils.deleteDirectoryAndChildren(path);
    } catch (IOException e) {
      e.printStackTrace();
    }
    this.graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(path.toFile());
    registerShutdownHook(graphDb);
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
