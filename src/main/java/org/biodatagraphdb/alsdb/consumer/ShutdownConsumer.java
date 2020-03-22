package org.biodatagraphdb.alsdb.consumer;

import java.nio.file.Path;

import org.biodatagraphdb.alsdb.service.graphdb.RunMode;
import org.biodatagraphdb.alsdb.supplier.GraphDatabaseServiceLegacySupplier;

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
