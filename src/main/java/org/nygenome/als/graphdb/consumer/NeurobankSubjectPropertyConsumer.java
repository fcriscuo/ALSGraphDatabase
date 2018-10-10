package org.nygenome.als.graphdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp.RelTypes;
import org.nygenome.als.graphdb.integration.TestGraphDataConsumer;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import org.nygenome.als.graphdb.value.NeurobankSubjectProperty;
import scala.Tuple2;

public class NeurobankSubjectPropertyConsumer extends GraphDataConsumer {

  private Consumer<NeurobankSubjectProperty> neurobankSubjectPropertyConsumer = (property)-> {
    // resolve new or existing subject node
    Node subjectNode = resolveSubjectNodeFunction.apply(property.subjectGuid());
    lib.novelLabelConsumer.accept(subjectNode,alsLabel);
    lib.novelLabelConsumer.accept(subjectNode,neurobankLabel);
    // resolve new or existing subject property node
    Node subjectPropertyNode = resolveSubjectPropertyNode.apply(property.id());
    lib.nodePropertyValueConsumer.accept(subjectPropertyNode, new Tuple2<>("PropertyCode",property.eventPropertyCode()));
    lib.nodePropertyValueConsumer.accept(subjectPropertyNode, new Tuple2<>("PropertyName",property.eventPropertyName()));
    Relationship rel = lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(subjectNode, subjectPropertyNode),
        RelTypes.PROPERTY);
    lib.relationshipPropertyValueConsumer.accept(rel,new Tuple2<>("propertyValue", property.eventPropertyValue()));


  };

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(path != null);
    new TsvRecordStreamSupplier(path).get()
        .map(NeurobankSubjectProperty::parseCSVRecord)
        .forEach(neurobankSubjectPropertyConsumer);
  }
  public static void importData() {
    Stopwatch sw = Stopwatch.createStarted();
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("NEUROBANK_SUBJECT_PROPERTY_FILE")
        .ifPresent(new NeurobankSubjectPropertyConsumer());
    AsyncLoggingService.logInfo("processed neurobank category file : " +
        sw.elapsed(TimeUnit.SECONDS) +" seconds");
  }
  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("NEUROBANK_SUBJECT_PROPERTY_FILE")
        .ifPresent(
            path -> new TestGraphDataConsumer().accept(path, new NeurobankSubjectPropertyConsumer()));
  }
}
