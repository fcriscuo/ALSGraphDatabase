package org.nygenome.als.graphdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.neo4j.graphdb.Node;
import org.nygenome.als.graphdb.integration.TestGraphDataConsumer;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.CsvRecordStreamSupplier;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.value.ProActAdverseEvent;

public class ProActAdverseEventConsumer extends GraphDataConsumer {
  private static final String ADVERSE_EVENT_CATEGORY = "Adverse Event";

  private Consumer<ProActAdverseEvent> proactAdverseEventConsumer = (event) -> {
    Node subjectNode  = resolveSubjectNodeFunction.apply(event.subjectGuid());
    lib.novelLabelConsumer.accept(subjectNode, alsAssociatedLabel);
    lib.novelLabelConsumer.accept(subjectNode,proactLabel);

  };

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(path != null);
    new CsvRecordStreamSupplier(path).get()
        .map(ProActAdverseEvent::parseCSVRecord)
        .forEach(proactAdverseEventConsumer);
  }

  public static void importData() {
    Stopwatch sw = Stopwatch.createStarted();
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("PROACT_ADVERSE_EVENT_FILE")
        .ifPresent(new ProActAdverseEventConsumer());
    AsyncLoggingService.logInfo("processed proact adverse event file: " +
        sw.elapsed(TimeUnit.SECONDS) +" seconds");
  }
  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("PROACT_ADVERSE_EVENT_FILE")
        .ifPresent(path -> new TestGraphDataConsumer().accept(path, new ProActAdverseEventConsumer()));
  }
}
