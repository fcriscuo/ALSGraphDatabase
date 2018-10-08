package org.nygenome.als.graphdb.consumer;

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp.RelTypes;
import org.nygenome.als.graphdb.integration.TestGraphDataConsumer;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.value.HumanTissueAtlas;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import java.nio.file.Path;
import java.util.function.Consumer;
import java.util.function.Predicate;
import scala.Tuple2;

/*
Consumer of records from the Human Tissue Atlas file
Only records whose status is Approved or Supported are retained
Processing a HumanTissue Node may also result in the creation of a
Protein Node
A uni-directional Relationship between a Protein Node and a HumanTissue Node
 */
public class HumanTissueAtlasDataConsumer extends GraphDataConsumer {

  /*
  Predicate that only accepts HumanTissueAtlas value objects
  whose reliability values are Approved or Supported
   */
  private Predicate<HumanTissueAtlas> reliabilityPredicate = (ht) ->
      ht.reliability().equalsIgnoreCase("Approved")
          || ht.reliability().equalsIgnoreCase("Supported");

  /*
  Predicate that filters out HumanTissue Atlas value objects
  whose level values are "Not detected"
   */
  private Predicate<HumanTissueAtlas> levelPredicate = (ht) ->
      !ht.level().equalsIgnoreCase("Not detected");

  @Override
  public void accept(Path path) {
    new TsvRecordStreamSupplier(path)
        .get()
        .map(HumanTissueAtlas::parseCSVRecord)
        // filter out records based on reliability value
        .filter(reliabilityPredicate)
        .filter(levelPredicate)
        .forEach(consumeHumanTissueAtlasObject);
  }


  private BiConsumer<String, String> createTissueTranscriptRelationshipConsumer =
      (transcriptId, tissueId) -> {
        Node transcriptNode = resolveEnsemblTranscriptNodeFunction.apply(transcriptId);
        Node tissueNode = resolveHumanTissueNodeFunction.apply(tissueId);
        lib.resolveNodeRelationshipFunction
            .apply(new Tuple2<>(transcriptNode, tissueNode), RelTypes.TISSUE_ENHANCED);
        AsyncLoggingService.logInfo("created transcript-tissue relationship for transcript "
            + transcriptId + " tissue " + tissueId);
      };
  /*
  Private Consumer that processes a valid HumanTissueAtlas object
  creates a Tissue Node if the tissue is new
  It establishes a Relationship between the Protein and Tissue Nodes
  It will also create a Protein Node if the protein is new
   */
  private Consumer<HumanTissueAtlas> consumeHumanTissueAtlasObject = (ht) -> {
    Node tissueNode = resolveHumanTissueNodeFunction.apply(ht.resolveTissueCellTypeLabel());
    lib.nodePropertyValueConsumer.accept(tissueNode, new Tuple2<>("Tissue", ht.tissue()));
    lib.nodePropertyValueConsumer.accept(tissueNode, new Tuple2<>("CellType", ht.cellType()));
    Node proteinNode = resolveProteinNodeFunction.apply(ht.uniprotId());
    Relationship rel = lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(proteinNode, tissueNode),
        RelTypes.TISSUE_ENHANCED );
   lib.relationshipPropertyValueConsumer.accept(rel, new Tuple2<>("Level", ht.level()));
    lib.relationshipPropertyValueConsumer.accept(rel, new Tuple2<>("Reliability", ht.reliability()));
    if (!Strings.isNullOrEmpty(ht.ensemblTranscriptId())) {
      createTissueTranscriptRelationshipConsumer
          .accept(ht.ensemblTranscriptId(), ht.resolveTissueCellTypeLabel());
    }
  };

  public static void importData() {
    Stopwatch sw = Stopwatch.createStarted();
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("HUMAN_TISSUE_ATLAS_FILE")
        .ifPresent(new HumanTissueAtlasDataConsumer());
    AsyncLoggingService.logInfo("read the Human Tissue Atlas data: " +
        sw.elapsed(TimeUnit.SECONDS) + " seconds");
  }

  // main method for standalone testing
  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("HUMAN_TISSUE_ATLAS_FILE")
        .ifPresent(path ->
            new TestGraphDataConsumer().accept(path, new HumanTissueAtlasDataConsumer()));
  }


}
