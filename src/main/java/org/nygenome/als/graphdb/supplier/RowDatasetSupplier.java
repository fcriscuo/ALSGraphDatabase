package org.nygenome.als.graphdb.supplier;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import javax.annotation.Nonnull;
import org.nygenome.als.graphdb.service.LocalSparkSessionSupplier;

import java.nio.file.Path;
import java.util.function.Supplier;

public class RowDatasetSupplier implements Supplier<Dataset<Row>>  {

  private Dataset<Row> rowDs;


  public RowDatasetSupplier(@Nonnull Path filePath, @Nonnull StructType aType) {
  this.rowDs =   LocalSparkSessionSupplier.INSTANCE.get().read()
        .format("csv")
        .schema(aType)
        .option("header", "true")
        .option("mode","DROPMALFORMED")
        .option("delimiter", "\t")
        .load(filePath.toString());
  }

  @Override public Dataset<Row> get() {
    return rowDs;
  }
}
