package edu.jhu.fcriscu1.als.graphdb.integration;


/*

 */

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import edu.jhu.fcriscu1.als.graphdb.consumer.GraphDataConsumer;
import edu.jhu.fcriscu1.als.graphdb.supplier.GraphDatabaseServiceSupplier.RunMode;
import edu.jhu.fcriscu1.als.graphdb.util.AsyncLoggingService;

/*
Test implemented as a BiConsumer that invokes a specified GraphDataConsumer subclass
using a specified file Path as a data source
Creates a temporary Neo4j graph
 */
public class TestGraphDataConsumer implements BiConsumer<Path, GraphDataConsumer> {



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
    Preconditions.checkState(graphDataConsumer.consumerRunModeSupplier.get().compareTo(RunMode.TEST) == 0,
        "TestGraphDataConsumer invoke with non-test run mode");

    Stopwatch stopwatch = Stopwatch.createStarted();
    graphDataConsumer.accept(path);
    stopwatch.stop();
    System.out.println( "GraphDataConsumer: " + graphDataConsumer.getClass().getName()
        +"  required : " + stopwatch.elapsed(TimeUnit.SECONDS) +" seconds.");

  }
}
