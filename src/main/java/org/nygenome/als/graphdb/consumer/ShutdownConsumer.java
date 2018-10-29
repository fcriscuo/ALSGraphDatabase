package org.nygenome.als.graphdb.consumer;

import java.nio.file.Path;
import org.nygenome.als.graphdb.supplier.GraphDatabaseServiceSupplier.RunMode;

public class ShutdownConsumer extends GraphDataConsumer{
  public ShutdownConsumer(RunMode runMode) {
    super(runMode);
  }

  @Override
  public void accept(Path path) {
  }

  public static void importProdData() {
   new ShutdownConsumer(RunMode.READ_ONLY).lib.shutDown();
  }
}
