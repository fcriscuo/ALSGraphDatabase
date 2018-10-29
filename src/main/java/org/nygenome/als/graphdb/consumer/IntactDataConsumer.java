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
import org.nygenome.als.graphdb.supplier.GraphDatabaseServiceSupplier.RunMode;
import org.nygenome.als.graphdb.util.DynamicRelationshipTypes;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp;
import org.nygenome.als.graphdb.integration.TestGraphDataConsumer;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.TsvRecordSplitIteratorSupplier;
import org.nygenome.als.graphdb.value.PsiMitab;
import scala.Tuple2;

public class IntactDataConsumer extends GraphDataConsumer implements BiConsumer<Path, Path> {

  public IntactDataConsumer(RunMode runMode) {
    super(runMode);
  }

  private Predicate<PsiMitab> selfInteractionPredicate = (ppi) ->
      !ppi.interactorAId().equals(ppi.interactorBId());

  /*
  Process the human intact data. The file provided from IntAct is very
  large.  Use a SplitIterator to process in chunks
   */

  private Consumer<PsiMitab> proteinInteractionConsumer = (ppi) -> {
    Tuple2<String, String> abTuple = new Tuple2<>(ppi.interactorAId(), ppi.interactorBId());

    Node proteinNodeA = resolveProteinNodeFunction.apply(ppi.interactorAId());
    Node proteinNodeB = resolveProteinNodeFunction.apply(ppi.interactorBId());
    RelationshipType interactionType = new DynamicRelationshipTypes(
        ppi.interactionTypeList().head()
    );
    Relationship ppRel = lib.resolveNodeRelationshipFunction
        .apply(new Tuple2<>(proteinNodeA, proteinNodeB), interactionType);
    AsyncLoggingService.logInfo("Created new PPI between Protein: " + ppi.interactorAId()
        + "  and Protein: " + ppi.interactorBId() + "   rel type: " + interactionType.name());
    // set ppi relationship properties
    lib.relationshipPropertyValueConsumer.accept(ppRel, new Tuple2<>("Interaction_method_detection",
        ppi.detectionMethodList().mkString("|") ));
    lib.relationshipPropertyValueConsumer.accept(ppRel, new Tuple2<>(
        "References",
            ppi.publicationIdList().mkString("|")
        ));
    lib.relationshipPropertyValueConsumer.accept(ppRel,
        new Tuple2<>("Neagtive", String.valueOf(ppi.negative())));

    if (null != ppi.confidenceValuesList() && ppi.confidenceValuesList().size() > 0) {
      lib.relationshipPropertyValueConsumer.accept(ppRel,
          new Tuple2<>("Confidence_level",
              ppi.confidenceValuesList().last()));
    }
  };

  @Override
  public void accept(Path path_data, Path path_headings) {
    Preconditions.checkArgument(Files.isRegularFile(path_data));
    Preconditions.checkArgument(Files.isRegularFile(path_headings));
    String[] columnHeadings = PsiMitab.INTACT_HEADER_STRING().split("\\t");
//    String[] columnHeadings = Utils.resolveColumnHeadingsFunction
//        .apply(path_headings);
    new TsvRecordSplitIteratorSupplier(path_data, columnHeadings).get()
        .map(PsiMitab::parseCSVRecord)
        .filter(selfInteractionPredicate)
        .forEach(proteinInteractionConsumer);
    lib.shutDown();
  }

  // required override
  // used for testing with default headings file
  @Override
  public void accept(Path path) {
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("PPI_INTACT_HEADINGS_FILE")
        .ifPresent(headPath -> accept(path,headPath));
  }

  public static void importProdData() {
    Stopwatch sw = Stopwatch.createStarted();
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("PPI_INTACT_FILE")
        .ifPresent(new IntactDataConsumer(RunMode.PROD));
    AsyncLoggingService.logInfo("read protein-protein interaction file: "
        + sw.elapsed(TimeUnit.SECONDS) + " seconds");
  }

  // main method for stand alone testing
  // use short file: short_human_intact.tsv
  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("TEST_PPI_INTACT_FILE")
        .ifPresent(path ->
            new TestGraphDataConsumer().accept(path, new IntactDataConsumer(RunMode.TEST)));
  }
}

