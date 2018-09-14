package org.nygenome.als.graphdb.consumer;


import com.google.common.base.Preconditions;
import java.util.function.BiConsumer;
import org.neo4j.graphdb.Node;
import org.nygenome.als.graphdb.EmbeddedGraph.RelTypes;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.TsvRecordSplitIteratorSupplier;
import org.nygenome.als.graphdb.util.Utils;
import org.nygenome.als.graphdb.value.PsiMitab;
import scala.Tuple2;
import java.nio.file.Files;
import java.nio.file.LinkOption;
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
    Tuple2<String,String> abTuple = new Tuple2<>(ppi.intearctorAId(),ppi.interactorBId());
    if (!proteinProteinIntactMap.containsKey(abTuple)) {
      Node proteinNodeA = resolveProteinNodeFunction.apply(ppi.intearctorAId());
      Node proteinNodeB = resolveProteinNodeFunction.apply(ppi.interactorBId());
      RelTypes relType = Utils.convertStringToRelType(ppi.interactionTypeList().head());
      proteinProteinIntactMap.put(abTuple,
          proteinNodeA.createRelationshipTo(proteinNodeB, relType)
      );
      AsyncLoggingService.logInfo("Created new PPI, Protein A: " + ppi.intearctorAId()
        +"  Protein B: " + ppi.interactorBId() +"   rel type: " +relType.name());
    }
    // set or update relationship properties
    proteinProteinIntactMap.get(abTuple).setProperty("Interaction_method_detection",
        ppi.detectionMethodList().mkString("|"));

    proteinProteinIntactMap.get(abTuple).setProperty("References",
        ppi.publicationIdList().mkString("|"));

    if (null != ppi.confidenceScoreList() && ppi.confidenceScoreList().size() >0) {
      proteinProteinIntactMap.get(abTuple).setProperty("Confidence_level",
          Double.parseDouble(ppi.confidenceScoreList().head()));
    }

  };

  @Override
  public void accept(Path path_data, Path path_headings) {
    Preconditions.checkArgument(Files.isRegularFile(path_data),LinkOption.NOFOLLOW_LINKS);
    Preconditions.checkArgument(Files.isRegularFile(path_headings),LinkOption.NOFOLLOW_LINKS);
    String[] columnHeadings = Utils.resolveColumnHeadingsFunction
        .apply(path_headings);
    new TsvRecordSplitIteratorSupplier(path_data,columnHeadings).get()
        .map(PsiMitab::parseCSVRecord)
        .filter(selfInteractionPredicate)
        .forEach(proteinInteractionConsumer);
  }

  @Override
  public void accept(Path path) {

  }
}

