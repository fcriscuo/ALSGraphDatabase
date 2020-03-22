package org.biodatagraphdb.alsdb.value;

import com.twitter.logging.Logger;
import org.biodatagraphdb.alsdb.model.PsiMitab;
import org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier;
import java.nio.file.Paths;


public class TestPsiMiTab {
  static Logger log = Logger.get(TestPsiMiTab.class);
  public static void main(String[] args) {
    try {
      new TsvRecordStreamSupplier(Paths.get("/data/als/short_human_intact.tsv")).get()
          .limit(50)
          .map(record  -> PsiMitab.Companion.parseCSVRecord(record))
        //  .filter(psi -> psi.interactorAId().startsWith("uniprotkb"))
        //  .filter(psi -> psi.interactorBId().startsWith("uniprotkb"))
          .forEach(psi -> {
            log.info(">>>>> " +psi.getInteractorAId() + " to " + psi.getInteractorBId() +"  negative = " +psi.getNegative());
           log.info( String.join(" ",psi.getAltIdAList()));
           log.info("Confidence score: " +PsiMitab.Companion.getLastListElement(psi.getConfidenceValuesList()));
           log.info(String.join(" ",psi.getInteractionTypeList()));
          });

    } catch (Exception e) {
      e.printStackTrace();
    }


  }

}
