package org.nygenome.als.graphdb.consumer;


import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp;
import org.nygenome.als.graphdb.integration.TestGraphDataConsumer;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.DynamicRelationshipType;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.TsvRecordSplitIteratorSupplier;
import org.nygenome.als.graphdb.value.PsiMitab;
import scala.Tuple2;

public class IntactDataConsumer extends GraphDataConsumer implements BiConsumer<Path,Path> {

  private Predicate<PsiMitab> selfInteractionPredicate = (ppi) ->
      ! ppi.interactorAId().equals(ppi.interactorBId());

  /*
  Process the human intact data. The file provided from IntAct is excessively
  large.  Use a SplitIterator to process in chunks
   */

  private Consumer<PsiMitab> proteinInteractionConsumer = (ppi) -> {
    Tuple2<String, String> abTuple = new Tuple2<>(ppi.interactorAId(), ppi.interactorBId());

    if (!proteinProteinIntactMap.containsKey(abTuple)) {
      Node proteinNodeA = resolveProteinNodeFunction.apply(ppi.interactorAId());
      Node proteinNodeB = resolveProteinNodeFunction.apply(ppi.interactorBId());
      //RelTypes relType = Utils.convertStringToRelType(ppi.interactionTypeList().head());
      // create Relationships within a Transaction
      Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get();
      try  {
        RelationshipType interactionType = new DynamicRelationshipType(
            ppi.interactionTypeList().head()
        );
        proteinProteinIntactMap.put(abTuple,
            proteinNodeA.createRelationshipTo(proteinNodeB, interactionType)
        );
        proteinNodeB.createRelationshipTo(proteinNodeA, interactionType);
        AsyncLoggingService.logInfo("Created new  bi-directional PPI, Protein A: " + ppi.interactorAId()
            + "  Protein B: " + ppi.interactorBId() + "   rel type: " + interactionType.name());

        // set or update relationship properties
        Relationship ppRel = proteinProteinIntactMap.get(abTuple);
        ppRel.setProperty("Interaction_method_detection",
            ppi.detectionMethodList().mkString("|"));
        ppRel.setProperty("References",
            ppi.publicationIdList().mkString("|"));
        ppRel.setProperty("Negative",String.valueOf(ppi.negative()));
        if (null != ppi.confidenceValuesList() && ppi.confidenceValuesList().size() > 0) {
          ppRel.setProperty("Confidence_level",
              Double.parseDouble(ppi.confidenceValuesList().last()));
        }
        tx.success();
      }
     catch(Exception e){
      AsyncLoggingService.logError(e.getMessage());
      tx.failure();
    } finally {
        tx.close();
      }
  }
  };

  @Override
  public void accept(Path path_data, Path path_headings) {
    Preconditions.checkArgument(Files.isRegularFile(path_data));
    Preconditions.checkArgument(Files.isRegularFile(path_headings));
    String[] columnHeadings = PsiMitab.INTACT_HEADER_STRING().split("\\t");
//    String[] columnHeadings = Utils.resolveColumnHeadingsFunction
//        .apply(path_headings);
    new TsvRecordSplitIteratorSupplier(path_data,columnHeadings).get()
        .map(PsiMitab::parseCSVRecord)
        .filter(selfInteractionPredicate)
        .forEach(proteinInteractionConsumer);
  }

  // required override
  // used for testing with default headings file
  @Override
  public void accept(Path path) {
    Path headPath = Paths.get("/data/als/heading_intact.txt");
    accept(path, headPath);
  }

  public static void importData() {
    Stopwatch sw = Stopwatch.createStarted();
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("PPI_INTACT_FILE")
        .ifPresent(new IntactDataConsumer());
    AsyncLoggingService.logInfo("read protein-protein interaction file: "
        +sw.elapsed(TimeUnit.SECONDS) +" seconds");
  }
 // main method for stand alone testing
  // use short file: short_human_intact.tsv
  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("PPI_INTACT_FILE")
        .ifPresent(path ->
            new TestGraphDataConsumer().accept(path, new IntactDataConsumer()));
  }
}

