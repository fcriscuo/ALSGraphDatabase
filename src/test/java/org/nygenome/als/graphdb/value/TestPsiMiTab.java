package org.nygenome.als.graphdb.value;

import com.twitter.logging.Logger;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import java.nio.file.Paths;


public class TestPsiMiTab {
  static Logger log = Logger.get(TestPsiMiTab.class);
  public static void main(String[] args) {
    try {
      new TsvRecordStreamSupplier(Paths.get("/data/als/short_human_intact.tsv")).get()
          .limit(50)
          .map(record  ->PsiMitab.parseCSVRecord(record))
        //  .filter(psi -> psi.interactorAId().startsWith("uniprotkb"))
        //  .filter(psi -> psi.interactorBId().startsWith("uniprotkb"))
          .forEach(psi -> {
            log.info(">>>>> " +psi.interactorAId() + " to " + psi.interactorBId() +"  negative = " +psi.negative());
           log.info( psi.altIdAList().mkString(" "));
           log.info("Confidence score: " +psi.confidenceValuesList().last());
           log.info(psi.interactionTypeList().mkString(" "));
          });

    } catch (Exception e) {
      e.printStackTrace();
    }


  }

}
