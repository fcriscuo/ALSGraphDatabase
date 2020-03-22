package org.biodatagraphdb.alsdb.consumer;


import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.biodatagraphdb.alsdb.integration.TestGraphDataConsumer;
import org.biodatagraphdb.alsdb.model.PsiMitab;
import org.biodatagraphdb.alsdb.service.graphdb.RunMode;
import org.biodatagraphdb.alsdb.supplier.GraphDatabaseServiceLegacySupplier;
import org.biodatagraphdb.alsdb.util.AsyncLoggingService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import scala.Tuple2;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class IntactDataConsumer extends GraphDataConsumer implements BiConsumer<Path, Path> {

    public IntactDataConsumer(RunMode runMode) {
        super(runMode);
    }

    /*
    Filter out self interactions
    n.b. negative predicate
     */
    private Predicate<PsiMitab> selfInteractionFilter = (ppi) -> {
        return !(ppi.getInteractorAId() == ppi.getInteractorBId());
    };

  /*
  Process the human intact data. The file provided from IntAct is very
  large.  Use a SplitIterator to process in chunks
   */

    private Consumer<PsiMitab> proteinInteractionConsumer = (ppi) -> {
        Tuple2<String, String> abTuple = new Tuple2<String, String>(ppi.getInteractorAId(), ppi.getInteractorBId());

        Node proteinNodeA = resolveProteinNodeFunction.apply(ppi.getInteractorAId());
        Node proteinNodeB = resolveProteinNodeFunction.apply(ppi.getInteractorBId());
        RelationshipType interactionType = new org.biodatagraphdb.alsdb.lib.DynamicRelationshipTypes(
                ppi.getInteractionTypeList().get(0)
        );
        Relationship ppRel = lib.resolveNodeRelationshipFunction
                .apply(new Tuple2<>(proteinNodeA, proteinNodeB), interactionType);
        AsyncLoggingService.logInfo("Created new PPI between Protein: " + ppi.getInteractorAId()
                + "  and Protein: " + ppi.getInteractorBId() + "   rel type: " + interactionType.name());
        // set ppi relationship properties
        lib.relationshipPropertyValueConsumer.accept(ppRel, new Tuple2<>("Interaction_method_detection",
                String.join("|", ppi.getDetectionMethodList())));
        lib.relationshipPropertyValueConsumer.accept(ppRel, new Tuple2<>(
                "References",
                String.join("|", ppi.getPublicationIdList()
                )));
        lib.relationshipPropertyValueConsumer.accept(ppRel,
                new Tuple2<>("Neagtive", String.valueOf(ppi.getNegative())));

        if ( ppi.getConfidenceValuesList().isEmpty()) {
            lib.relationshipPropertyValueConsumer.accept(ppRel,
                    new Tuple2<>("Confidence_level",
                            ppi.Companion.getLastListElement(ppi.getConfidenceValuesList())));
        }
    };

    @Override
    public void accept(Path path_data, Path path_headings) {
        Preconditions.checkArgument(Files.isRegularFile(path_data));
        Preconditions.checkArgument(Files.isRegularFile(path_headings));
        String[] columnHeadings = PsiMitab.Companion.getINTACT_HEADER_STRING().split("\\t");
//    String[] columnHeadings = Utils.resolveColumnHeadingsFunction
//        .apply(path_headings);
        new org.biodatagraphdb.alsdb.util.TsvRecordSplitIteratorSupplier(path_data, columnHeadings).get()
                .map(PsiMitab.Companion::parseCSVRecord)
                .filter(selfInteractionFilter)
                .forEach(proteinInteractionConsumer);
        lib.shutDown();
    }

    // required override
    // used for testing with default headings file
    @Override
    public void accept(Path path) {
        org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE.getOptionalPathProperty("PPI_INTACT_HEADINGS_FILE")
                .ifPresent(headPath -> accept(path, headPath));
    }

    public static void importProdData() {
        Stopwatch sw = Stopwatch.createStarted();
        org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
                .getOptionalPathProperty("PPI_INTACT_FILE")
                .ifPresent(new IntactDataConsumer(RunMode.PROD));
        AsyncLoggingService.logInfo("read protein-protein interaction file: "
                + sw.elapsed(TimeUnit.SECONDS) + " seconds");
    }

    // main method for stand alone testing
    // use short file: short_human_intact.tsv
    public static void main(String[] args) {
        Stopwatch sw = Stopwatch.createStarted();
        org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE.getOptionalPathProperty("TEST_PPI_INTACT_FILE")
                .ifPresent(path ->
                        new TestGraphDataConsumer().accept(path, new IntactDataConsumer(RunMode.TEST)));
        AsyncLoggingService.logInfo("read protein-protein interaction test file: "
                + sw.elapsed(TimeUnit.SECONDS) + " seconds");
    }
}

