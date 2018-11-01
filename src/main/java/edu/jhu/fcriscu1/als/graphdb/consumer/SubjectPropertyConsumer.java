package edu.jhu.fcriscu1.als.graphdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import edu.jhu.fcriscu1.als.graphdb.integration.TestGraphDataConsumer;
import edu.jhu.fcriscu1.als.graphdb.supplier.GraphDatabaseServiceSupplier;
import edu.jhu.fcriscu1.als.graphdb.value.StringSubjectProperty;
import org.neo4j.graphdb.Node;
//import edu.jhu.fcriscu1.als.graphdb.app.ALSDatabaseImportApp.RelTypes;
import edu.jhu.fcriscu1.als.graphdb.util.FrameworkPropertyService;
import edu.jhu.fcriscu1.als.graphdb.util.TsvRecordStreamSupplier;
import edu.jhu.fcriscu1.als.graphdb.util.AsyncLoggingService;
import scala.Tuple2;

public class SubjectPropertyConsumer extends GraphDataConsumer {


  public SubjectPropertyConsumer(GraphDatabaseServiceSupplier.RunMode runMode) {super(runMode);}

  private Function<StringSubjectProperty, Node> completeSampleNodeFunction = (stringSubjectProperty) -> {
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
    Node subjectNode = resolveSubjectNodeFunction.apply(subjectProperty.subjectTuple());
    lib.nodePropertyValueConsumer.accept(subjectNode,
        new Tuple2<>(subjectProperty.propertyName(), subjectProperty.propertyValue()));
    Node sampleNode = completeSampleNodeFunction.apply(subjectProperty);
    lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(subjectNode,sampleNode),
        sampledFromRelationType );
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
    new TsvRecordStreamSupplier(path).get()
        .map(StringSubjectProperty::parseCSVRecord)
        .forEach(stringSubjectPropertyConsumer);
    lib.shutDown();
  }
  public static void importProdData() {
    Stopwatch sw = Stopwatch.createStarted();
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("SUBJECT_PROPERTY_FILE")
        .ifPresent(new SubjectPropertyConsumer(GraphDatabaseServiceSupplier.RunMode.PROD));
    AsyncLoggingService.logInfo("processed subject properties file: " +
        sw.elapsed(TimeUnit.SECONDS) +" seconds");
  }
  public static void main(String... args) {

    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("TEST_SUBJECT_PROPERTY_FILE")
        .ifPresent(path -> new TestGraphDataConsumer().accept(path, new SubjectPropertyConsumer(GraphDatabaseServiceSupplier.RunMode.TEST)));
  }
}
