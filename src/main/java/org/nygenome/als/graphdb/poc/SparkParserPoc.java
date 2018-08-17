package org.nygenome.als.graphdb.poc;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;

import com.twitter.logging.Logger;

import javax.annotation.Nonnull;
import org.nygenome.als.graphdb.model.ModelObject;
import org.nygenome.als.graphdb.model.PsiMitab;
import org.nygenome.als.graphdb.service.LocalSparkSessionSupplier;
import scala.Function1;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.function.Consumer;

/*
POC using Spark to parse a TSV file
 */
public class SparkParserPoc {
  private final String tsvFileName;


  public SparkParserPoc (@Nonnull Path tsvPath) {
    this.tsvFileName = tsvPath.toString();
    System.out.println("Processing TSV file " +tsvFileName);
    tsvFileConsumer.accept(tsvFileName);
  }

  private Consumer<String> tsvFileConsumer = (fileName) -> {
    SparkSession spark = LocalSparkSessionSupplier.INSTANCE.get();

    Encoder encoder = PsiMitab.encoderSupplier.get();

    Dataset<Row>ds = spark.read()
        .format("csv")
       .schema(PsiMitab.schemaSupplier.get())
       .option("header", "true")
       .option("mode","DROPMALFORMED")
       .option("delimiter", "\t")
       .load(fileName);

    ForeachFunction<Row> processRow = (row) -> {
      String idB = row.getAs("ID(s) interactor B");
      System.out.println("ID B: " + idB);
    };

    ForeachFunction<PsiMitab> processPpi = (ppi) -> {
      System.out.println("+++++++++++++++PPI ++++++++++++++++++++++");
      System.out.println("Protein A: " +ppi.getIntearctorAId());
      System.out.println("Protein B: " +ppi.getInteractorBId());
      System.out.println("Publications: " + ModelObject.reduceListToStringFunction.apply(ppi.getPublicationIdList()));
    };


    Arrays.asList(ds.columns()).forEach(System.out::println);
    ds.limit(20)
        .map(PsiMitab.parseDatasetRowFunction, encoder)
        .foreach(processPpi);
  };

  public static void main(String[] args) {
    Path path = Paths.get("/data/als/human_intact.tsv");
    SparkParserPoc poc = new SparkParserPoc(path);
  }


}
