package org.biodatagraphdb.alsdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.biodatagraphdb.alsdb.integration.TestGraphDataConsumer;
import org.biodatagraphdb.alsdb.model.NeurobankSubjectProperty;
import org.biodatagraphdb.alsdb.service.graphdb.RunMode;
import org.biodatagraphdb.alsdb.supplier.GraphDatabaseServiceLegacySupplier;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.biodatagraphdb.alsdb.util.AsyncLoggingService;
import scala.Tuple2;

public class NeurobankSubjectPropertyConsumer extends GraphDataConsumer {

  public NeurobankSubjectPropertyConsumer(RunMode runMode) {super(runMode);}

  private Consumer<org.biodatagraphdb.alsdb.model.NeurobankSubjectProperty> neurobankSubjectPropertyConsumer = (property)-> {
    // resolve new or existing subject node
    Node subjectNode = resolveSubjectNodeFunction.apply(property.getSubjectIdPair());
    lib.novelLabelConsumer.accept(subjectNode, alsAssociatedLabel);
    lib.novelLabelConsumer.accept(subjectNode,neurobankLabel);
    // resolve new or existing subject property node
    Node subjectPropertyNode = resolveSubjectPropertyNode.apply(property.getId());
    lib.nodePropertyValueConsumer.accept(subjectPropertyNode, new Tuple2<>("PropertyCode",property.getEventPropertyCode()));
    lib.nodePropertyValueConsumer.accept(subjectPropertyNode, new Tuple2<>("PropertyName",property.getEventPropertyName()));
    Relationship rel = lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(subjectNode, subjectPropertyNode),
        propertyRelationType);
    lib.relationshipPropertyValueConsumer.accept(rel,new Tuple2<>("propertyValue", property.getEventPropertyValue()));
  };

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(path != null);
    new org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier(path).get()
        .map(NeurobankSubjectProperty.Companion::parseCSVRecord)
        .forEach(neurobankSubjectPropertyConsumer);
    lib.shutDown();
  }
  public static void importProdData() {
    Stopwatch sw = Stopwatch.createStarted();
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("NEUROBANK_SUBJECT_PROPERTY_FILE")
        .ifPresent(new NeurobankSubjectPropertyConsumer(RunMode.PROD));
    AsyncLoggingService.logInfo("processed neurobank category file : " +
        sw.elapsed(TimeUnit.SECONDS) +" seconds");
  }
  public static void main(String[] args) {
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("TEST_NEUROBANK_SUBJECT_PROPERTY_FILE")
        .ifPresent(
            path -> new TestGraphDataConsumer().accept(path, new NeurobankSubjectPropertyConsumer(RunMode.TEST)));
  }
}
