package org.nygenome.als.graphdb.consumer;


import com.google.common.base.Preconditions;
import java.nio.file.Paths;
import java.util.function.BiConsumer;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.nygenome.als.graphdb.EmbeddedGraph;
import org.nygenome.als.graphdb.EmbeddedGraph.RelTypes;
import org.nygenome.als.graphdb.integration.TestGraphDataConsumer;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.TsvRecordSplitIteratorSupplier;
import org.nygenome.als.graphdb.util.Utils;
import org.nygenome.als.graphdb.value.PsiMitab;
import scala.Tuple2;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class IntactDataConsumer extends GraphDataConsumer implements BiConsumer<Path,Path> {

  private Predicate<PsiMitab> selfInteractionPredicate = (ppi) ->
      ! ppi.intearctorAId().equals(ppi.interactorBId());

  /*
  Process the human intact data. The file provided from IntAct is excessively
  large.  Use a SplitIterator to process in chunks
   */

  private Consumer<PsiMitab> proteinInteractionConsumer = (ppi) -> {
    Tuple2<String, String> abTuple = new Tuple2<>(ppi.intearctorAId(), ppi.interactorBId());

    if (!proteinProteinIntactMap.containsKey(abTuple)) {
      Node proteinNodeA = resolveProteinNodeFunction.apply(ppi.intearctorAId());
      Node proteinNodeB = resolveProteinNodeFunction.apply(ppi.interactorBId());
      RelTypes relType = Utils.convertStringToRelType(ppi.interactionTypeList().head());
      // create Relationships within a Transaction
      Transaction tx = EmbeddedGraph.INSTANCE.transactionSupplier.get();
      try  {
        proteinProteinIntactMap.put(abTuple,
            proteinNodeA.createRelationshipTo(proteinNodeB, relType)
        );
        AsyncLoggingService.logInfo("Created new PPI, Protein A: " + ppi.intearctorAId()
            + "  Protein B: " + ppi.interactorBId() + "   rel type: " + relType.name());

        // set or update relationship properties
        proteinProteinIntactMap.get(abTuple).setProperty("Interaction_method_detection",
            ppi.detectionMethodList().mkString("|"));

        proteinProteinIntactMap.get(abTuple).setProperty("References",
            ppi.publicationIdList().mkString("|"));

        if (null != ppi.confidenceValuesList() && ppi.confidenceValuesList().size() > 0) {
          proteinProteinIntactMap.get(abTuple).setProperty("Confidence_level",
              Double.parseDouble(ppi.confidenceValuesList().head()));
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
 // main method for stand alone testing
  // use short file: short_human_intact.tsv
  public static void main(String[] args) {


    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("TEST_PPI_INTACT_FILE")
        .ifPresent(path ->
            new TestGraphDataConsumer().accept(path, new IntactDataConsumer()));
  }
}

