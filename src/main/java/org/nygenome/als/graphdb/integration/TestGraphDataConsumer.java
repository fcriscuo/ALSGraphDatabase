package org.nygenome.als.graphdb.integration;


/*

 */

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Suppliers;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.neo4j.graphdb.GraphDatabaseService;
import org.nygenome.als.graphdb.consumer.GraphDataConsumer;
import org.nygenome.als.graphdb.supplier.GraphDatabaseServiceSupplier;

/*
Test implemented as a BiConsumer that invokes a specified GraphDataConsumer subclass
using a specified file Path as a data source
Creates a temporary Neo4j graph
 */
public class TestGraphDataConsumer implements BiConsumer<Path, GraphDataConsumer> {

  private static final Path DB_PATH = Paths.get("/tmp/neo4j/test_graph");
 public TestGraphDataConsumer() {
 }

  @Override
  public void accept(Path path, GraphDataConsumer graphDataConsumer) {
    Preconditions.checkArgument(null != path,
        "A Path to an input file is required");
    Preconditions.checkArgument(Files.exists(path, LinkOption.NOFOLLOW_LINKS),
        "File "+path.toString() +" is invalid");
    Preconditions.checkArgument(null != graphDataConsumer,
        "A GraphDataConsumer implementation is required");

    GraphDatabaseService graphDb = Suppliers.memoize(new GraphDatabaseServiceSupplier(DB_PATH))
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
