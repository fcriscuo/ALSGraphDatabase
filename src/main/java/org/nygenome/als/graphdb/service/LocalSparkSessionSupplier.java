package org.nygenome.als.graphdb.service;

import org.apache.spark.sql.SparkSession;

import java.util.function.Supplier;

public enum LocalSparkSessionSupplier implements Supplier<SparkSession> {
  INSTANCE;

  private final SparkSession session =  SparkSession
      .builder()
      .appName("ALS Database Spark Session")
      .config("spark.master", "local")
      .getOrCreate();

  @Override public SparkSession get() {
    return this.session;
  }

}
