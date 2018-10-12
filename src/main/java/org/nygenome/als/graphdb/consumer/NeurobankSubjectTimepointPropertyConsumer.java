package org.nygenome.als.graphdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.nygenome.als.graphdb.integration.TestGraphDataConsumer;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import org.nygenome.als.graphdb.value.NeurobankSubjectTimepointProperty;

public class NeurobankSubjectTimepointPropertyConsumer extends GraphDataConsumer {


  private Consumer<NeurobankSubjectTimepointProperty> neurobankSubjectTimepointPropertyConsumer =
      (property) -> {

      };

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(path != null);
    new TsvRecordStreamSupplier(path).get()
        .map(NeurobankSubjectTimepointProperty::parseCSVRecord)
        .forEach(neurobankSubjectTimepointPropertyConsumer);
  }

  public static void importData() {
    Stopwatch sw = Stopwatch.createStarted();
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("NEUROBANK_SUBJECT_TIMEPOINT_PROPERTY_FILE")
        .ifPresent(new NeurobankSubjectTimepointPropertyConsumer());
    AsyncLoggingService.logInfo("processed neurobank subject timepoint property file : " +
        sw.elapsed(TimeUnit.SECONDS) +" seconds");
  }
  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("NEUROBANK_SUBJECT_TIMEPOINT_PROPERTY_FILE")
        .ifPresent(
            path -> new TestGraphDataConsumer().accept(path, new NeurobankSubjectTimepointPropertyConsumer()));
  }

}
