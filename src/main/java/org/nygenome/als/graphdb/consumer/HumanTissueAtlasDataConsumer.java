package org.nygenome.als.graphdb.consumer;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.nygenome.als.graphdb.EmbeddedGraph;
import org.nygenome.als.graphdb.EmbeddedGraph.RelTypes;
import org.nygenome.als.graphdb.integration.TestGraphDataConsumer;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.value.HumanTissueAtlas;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;

import java.nio.file.Path;
import java.util.Arrays;
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


    private Predicate<HumanTissueAtlas> reliabilityPredicate = (ht) ->
        ht.reliability().equalsIgnoreCase("Approved")
        || ht.reliability().equalsIgnoreCase("Supported");

  private Predicate<HumanTissueAtlas> levelPredicate = (ht) ->
      !ht.level().equalsIgnoreCase("Not detected");

    @Override
    public void accept(Path path) {
        new TsvRecordStreamSupplier(path)
            .get()
            .map(record->HumanTissueAtlas.parseCSVRecord(record))
            // filter out records based on reliability value
            .filter(reliabilityPredicate)
            .filter(levelPredicate)
            .forEach(consumeHumanTissueAtlasObject);
    }

    private Consumer<HumanTissueAtlas> consumeHumanTissueAtlasObject = (ht) -> {
      try ( Transaction tx = EmbeddedGraph.INSTANCE.transactionSupplier.get()) {
        Node tissueNode = resolveHumanTissueAtlasNodeFunction.apply(ht);
        nodePropertyValueConsumer.accept(tissueNode,new Tuple2<>("tissue", ht.tissue()));
        nodePropertyValueConsumer.accept(tissueNode, new Tuple2<>("CellType",ht.cellType()));
        Node proteinNode = resolveProteinNodeFunction.apply(ht.uniprotId());
        // complete uni-directional relationship between protein and tissue
        Tuple2<String,String> keyTuple = new Tuple2<>(ht.uniprotId(),
            ht.resolveTissueCellTypeLabel());
        if (!proteinTissRelMap.containsKey(keyTuple)) {
          Relationship rel = proteinNode.createRelationshipTo(tissueNode, RelTypes.TISSUE_ENHANCED);
          rel.setProperty("Transcript", ht.ensemblTranscriptId());
          rel.setProperty("Level", ht.level());
          rel.setProperty("Reliability", ht.reliability());
          proteinTissRelMap.put(keyTuple, rel);
        }
        tx.success();
      } catch (Exception e) {
        AsyncLoggingService.logError(e.getMessage());
        e.printStackTrace();
      }
    };
// main method for standalone testing
  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("HUMAN_TISSUE_ATLAS_FILE")
        .ifPresent(path ->
            new TestGraphDataConsumer().accept(path, new HumanTissueAtlasDataConsumer()));
  }


}
