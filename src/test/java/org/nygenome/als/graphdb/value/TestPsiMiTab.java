package org.nygenome.als.graphdb.value;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.types.StructType;
import com.twitter.logging.Logger;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import java.nio.file.Paths;


public class TestPsiMiTab {
  static Logger log = Logger.get(TestPsiMiTab.class);
  public static void main(String[] args) {
    try {
      new TsvRecordStreamSupplier(Paths.get("/data/als/intact_negative.txt")).get()
          .limit(50)
          .map(record  ->PsiMitab.parseCSVRecord(record))
          .forEach(psi -> {
            log.info(">>>>> " +psi.intearctorAId() + " to " + psi.interactorBId() +"  negative = " +psi.negative());
           log.info( psi.altIdAList().mkString(" "));
          });

    } catch (Exception e) {
      e.printStackTrace();
    }
//    StructType schema = PsiMitab.schemaSupplier.get();
//    Arrays.asList(schema.fieldNames()).forEach(System.out::println);
//
//    Encoder<PsiMitab> encoder = PsiMitab.encoderSupplier.get();
//    System.out.println(encoder.toString());

  }

}
