package org.nygenome.als.graphdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import org.neo4j.graphdb.Node;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp.RelTypes;
import org.nygenome.als.graphdb.integration.TestGraphDataConsumer;
import org.nygenome.als.graphdb.lib.FunctionLib;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import org.nygenome.als.graphdb.value.StringSubjectProperty;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import scala.Tuple2;

public class SubjectPropertyConsumer extends GraphDataConsumer {

  private FunctionLib lib = new FunctionLib();


  private Function<StringSubjectProperty, Node> resolveSampleNodeFunction = (stringSubjectProperty) -> {
    String externalSampleId = stringSubjectProperty.externalSampleId();
    Node sampleNode = resolveSampleNodeFunction.apply(externalSampleId);
    // an existing Sample node may not have had these properties set
    lib.nodePropertyValueConsumer
        .accept(sampleNode, new Tuple2<>("ExternalSampleId", externalSampleId));
    lib.nodePropertyValueConsumer
        .accept(sampleNode, new Tuple2<>("SampleType", stringSubjectProperty.sampleType()));
    lib.nodePropertyValueConsumer
        .accept(sampleNode, new Tuple2<>("AnalyteType", stringSubjectProperty.analyteType()));
    return sampleNode;
  };

  private Consumer<StringSubjectProperty> stringSubjectPropertyConsumer = (subjectProperty) -> {
    Node subjectNode = resolveSubjectNodeFunction.apply(subjectProperty.externalSubjectId());
    lib.nodePropertyValueConsumer.accept(subjectNode,
        new Tuple2<>(subjectProperty.propertyName(), subjectProperty.propertyValue()));
    Node sampleNode = resolveSampleNodeFunction.apply(subjectProperty);
    lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(subjectNode,sampleNode),RelTypes.HAS_SAMPLE );
  };

  private Predicate<StringSubjectProperty> alsPredicate = (ssp) ->
      !ssp.externalSubjectId().toUpperCase().startsWith("TCGA");

  /*
  Public Consumer interface method to process the specified file
  as a CSV file containing data that can be mapped to StringSubjectProperty objects
   */
  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(null != path);
    new TsvRecordStreamSupplier(path,StringSubjectProperty.columnHeadings()).get()
        .map(StringSubjectProperty::parseCSVRecord)
        .filter(alsPredicate)
        .forEach(stringSubjectPropertyConsumer);
  }

  public static void importData() {
    Stopwatch sw = Stopwatch.createStarted();
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("SUBJECT_PROPERTY_FILE")
        .ifPresent(new SubjectPropertyConsumer());
    AsyncLoggingService.logInfo("processed subject properties file: " +
        sw.elapsed(TimeUnit.SECONDS) +" seconds");
  }
  public static void main(String... args) {
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("SUBJECT_PROPERTY_FILE")
        .ifPresent(path -> new TestGraphDataConsumer().accept(path, new SubjectPropertyConsumer()));
  }
}
