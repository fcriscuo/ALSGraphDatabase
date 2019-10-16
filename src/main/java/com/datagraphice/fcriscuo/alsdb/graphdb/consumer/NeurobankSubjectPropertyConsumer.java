package com.datagraphice.fcriscuo.alsdb.graphdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.datagraphice.fcriscuo.alsdb.graphdb.integration.TestGraphDataConsumer;
import com.datagraphice.fcriscuo.alsdb.graphdb.supplier.GraphDatabaseServiceSupplier;
import edu.jhu.fcriscu1.als.graphdb.value.NeurobankSubjectProperty;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import edu.jhu.fcriscu1.als.graphdb.util.AsyncLoggingService;
import scala.Tuple2;

public class NeurobankSubjectPropertyConsumer extends GraphDataConsumer {

  public NeurobankSubjectPropertyConsumer(GraphDatabaseServiceSupplier.RunMode runMode) {super(runMode);}

  private Consumer<NeurobankSubjectProperty> neurobankSubjectPropertyConsumer = (property)-> {
    // resolve new or existing subject node
    Node subjectNode = resolveSubjectNodeFunction.apply(property.subjectTuple());
    lib.getNovelLabelConsumer().accept(subjectNode, alsAssociatedLabel);
    lib.getNovelLabelConsumer().accept(subjectNode,neurobankLabel);
    // resolve new or existing subject property node
    Node subjectPropertyNode = resolveSubjectPropertyNode.apply(property.id());
    lib.getNodePropertyValueConsumer().accept(subjectPropertyNode, new Tuple2<>("PropertyCode",property.eventPropertyCode()));
    lib.getNodePropertyValueConsumer().accept(subjectPropertyNode, new Tuple2<>("PropertyName",property.eventPropertyName()));
    Relationship rel = lib.getResolveNodeRelationshipFunction().apply(new Tuple2<>(subjectNode, subjectPropertyNode),
        propertyRelationType);
    lib.getRelationshipPropertyValueConsumer().accept(rel,new Tuple2<>("propertyValue", property.eventPropertyValue()));
  };

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(path != null);
    new com.datagraphice.fcriscuo.alsdb.graphdb.util.TsvRecordStreamSupplier(path).get()
        .map(NeurobankSubjectProperty::parseCSVRecord)
        .forEach(neurobankSubjectPropertyConsumer);
    lib.shutDown();
  }
  public static void importProdData() {
    Stopwatch sw = Stopwatch.createStarted();
    com.datagraphice.fcriscuo.alsdb.graphdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("NEUROBANK_SUBJECT_PROPERTY_FILE")
        .ifPresent(new NeurobankSubjectPropertyConsumer(GraphDatabaseServiceSupplier.RunMode.PROD));
    AsyncLoggingService.logInfo("processed neurobank category file : " +
        sw.elapsed(TimeUnit.SECONDS) +" seconds");
  }
  public static void main(String[] args) {
    com.datagraphice.fcriscuo.alsdb.graphdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("TEST_NEUROBANK_SUBJECT_PROPERTY_FILE")
        .ifPresent(
            path -> new TestGraphDataConsumer().accept(path, new NeurobankSubjectPropertyConsumer(GraphDatabaseServiceSupplier.RunMode.TEST)));
  }
}
