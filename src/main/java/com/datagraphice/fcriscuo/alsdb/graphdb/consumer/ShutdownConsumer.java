package com.datagraphice.fcriscuo.alsdb.graphdb.consumer;

import java.nio.file.Path;

import com.datagraphice.fcriscuo.alsdb.graphdb.supplier.GraphDatabaseServiceSupplier;

public class ShutdownConsumer extends GraphDataConsumer{
  public ShutdownConsumer(GraphDatabaseServiceSupplier.RunMode runMode) {
    super(runMode);
  }

  @Override
  public void accept(Path path) {
  }

  public static void importProdData() {
   new ShutdownConsumer(GraphDatabaseServiceSupplier.RunMode.READ_ONLY).lib.shutDown();
  }
}
