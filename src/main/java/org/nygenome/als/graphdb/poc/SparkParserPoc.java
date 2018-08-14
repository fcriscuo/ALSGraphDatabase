package org.nygenome.als.graphdb.poc;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
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
import org.nygenome.als.graphdb.model.PsiMitab;
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
    SparkSession spark = SparkSession
        .builder()
        .appName("Java Spark SQL data sources example")
        .config("spark.master", "local")
        .getOrCreate();

    Dataset<Row>ds = spark.read()
       .format("csv")
       .schema(PsiMitab.schemaSupplier.get())
       .option("header", "true")
       .option("mode","DROPMALFORMED")
       .option("delimiter", "\t")
      // .option("inferSchema","true")
       .load(fileName);

    ForeachFunction<Row> processRow = (row) -> {
      String idB = row.getAs("ID(s) interactor B");
      System.out.println("ID B: " + idB);
    };


    Encoder encoder = PsiMitab.encoderSupplier.get();
    Arrays.asList(ds.columns()).forEach(System.out::println);
    ds.limit(20)
        .foreach(processRow);

      // .load(fileName);
//   dsr
//       .toDF()
//       .limit(20)
//      .collectAsList()
//       .forEach(row -> {
//         String idB = row.getAs("ID(s) interactor B");
//         System.out.println("ID B: " + idB);
//       });





   // System.out.println(fileName + " contains " +dsr.count() +" records");
    //Arrays.asList(dsr.columns()).forEach(System.out::println);


  };

  public static void main(String[] args) {
    Path path = Paths.get("/data/als/human_intact.tsv");
    SparkParserPoc poc = new SparkParserPoc(path);
  }


}
