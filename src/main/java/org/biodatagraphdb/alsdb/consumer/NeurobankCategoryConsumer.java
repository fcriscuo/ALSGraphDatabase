package org.biodatagraphdb.alsdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.biodatagraphdb.alsdb.integration.TestGraphDataConsumer;
import org.biodatagraphdb.alsdb.supplier.GraphDatabaseServiceSupplier;
import org.neo4j.graphdb.Node;
import edu.jhu.fcriscu1.als.graphdb.util.AsyncLoggingService;
import scala.Tuple2;

/*
Consumer responsible for loading Neurobank categories into the Neo4j
database.
This Consumer should be invoked before other Neurobank data are loaded
 */
public class NeurobankCategoryConsumer extends GraphDataConsumer {

  public NeurobankCategoryConsumer(GraphDatabaseServiceSupplier.RunMode runMode) {super(runMode);}

  private Consumer<org.biodatagraphdb.alsdb.value.AlsPropertyCategory> neurobankCategoryConsumer = (category) -> {
    Node categoryNode = resolveCategoryNode.apply(category.category());
    if(!category.isSelfReferential()){
      Node parentCategoryNode = resolveCategoryNode.apply(category.parentCategory());
      lib.getResolveNodeRelationshipFunction().apply(new Tuple2<>(parentCategoryNode, categoryNode),
          childRelationType);
    }
  };

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(path != null);
    new org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier(path).get()
        .map(org.biodatagraphdb.alsdb.value.AlsPropertyCategory::parseCSVRecord)
        .forEach(neurobankCategoryConsumer);
    lib.shutDown();
  }
  public static void importProdData() {
    Stopwatch sw = Stopwatch.createStarted();
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("ALS_PROPERTY_CATEGORY_FILE")
        .ifPresent(new NeurobankCategoryConsumer(GraphDatabaseServiceSupplier.RunMode.PROD));
    AsyncLoggingService.logInfo("processed neurobank category file : " +
        sw.elapsed(TimeUnit.SECONDS) +" seconds");
  }
  // run this Consumer independently
  public static void main(String[] args) {

    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("ALS_PROPERTY_CATEGORY_FILE")
        .ifPresent(
            path -> new TestGraphDataConsumer().accept(path, new NeurobankCategoryConsumer(GraphDatabaseServiceSupplier.RunMode.TEST)));
  }
}
