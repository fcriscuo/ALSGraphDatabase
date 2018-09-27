package org.nygenome.als.graphdb.consumer;

import com.google.common.base.Preconditions;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.function.Consumer;
import java.util.function.Function;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.nygenome.als.graphdb.EmbeddedGraph;
import org.nygenome.als.graphdb.EmbeddedGraph.LabelTypes;
import org.nygenome.als.graphdb.EmbeddedGraph.RelTypes;
import org.nygenome.als.graphdb.integration.TestGraphDataConsumer;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.TsvRecordSplitIteratorSupplier;
import org.nygenome.als.graphdb.value.RnaTpmGene;
import scala.Tuple2;

/*
Public Consumer responsible for creating graph nodes and relationships from
data contained in a TSV specified TSV file
Because it is antidipated that tthese files will be extremely large,
a streamin utility based on a Splititerator is used
 */
public class RnaTpmGeneConsumer extends GraphDataConsumer {

  /*
  Only this Consumer creates RnaTpmGene Nodes so it can be private and set the properties
   */
  private Function<RnaTpmGene, Node> resolveRnaTpmGeneNode = (rnaTpmGene) -> {
    String id = rnaTpmGene.id();
    if (!rnaTpmGeneMap.containsKey(id)) {
      AsyncLoggingService.logInfo("creating RnaTpmNode for id  " +
          id);

        Node node = EmbeddedGraph.getGraphInstance()
            .createNode(LabelTypes.Expression);
        node.addLabel(LabelTypes.TPM);
        nodePropertyValueConsumer.accept(node, new Tuple2<>("SampleGeneId", id));
        // persist tpm value as a String
        nodePropertyValueConsumer
            .accept(node, new Tuple2<>("TPM", String.valueOf(rnaTpmGene.tpm())));
        rnaTpmGeneMap.put(id, node);

    }
    return rnaTpmGeneMap.get(id);
  };

  /*
  private Consumer to process a RnaTpmGene object
  Will create an RnaTpmGene Node  and if necessary an EnsembleGene Node
  and/or a HugoGene Node
  It will create bidirectional Relationships between all three Node types
   */
  private Consumer<RnaTpmGene> rnaTpmGeneConsumer = (tpm) -> {

    try (Transaction tx = EmbeddedGraph.INSTANCE.transactionSupplier.get()) {
      Node rnaNode = resolveRnaTpmGeneNode.apply(tpm);
      // Optional.get is OK because we've already filtered on it presence
      // and this is a private method only called from processing the  stream of TSV records
      Node hugoGeneNode = resolveGeneNodeFunction
          .apply(tpm.uniProtMapping().get().geneSymbol());
      Node ensemblGeneNode = resolveEnsemblGeneNodeFunction
          .apply(tpm.uniProtMapping().get().ensemblGeneId());
      Node ensemblTranscriptNode = resolveEnsemblTranscriptNodeFunction
          .apply(tpm.uniProtMapping().get().ensemblTranscriptId());
      Node proteinNode = resolveProteinNodeFunction
          .apply(tpm.uniProtMapping().get().uniProtId());
      // establish a relationship between the RNA node and the protein node
      createBiDirectionalRelationship(proteinNode, rnaNode,
          new Tuple2<>(tpm.uniProtMapping().get().uniProtId(), tpm.id()),
          proteinTPMRelMap, RelTypes.EXPRESSION_LEVEL, RelTypes.EXPRESSED_PROTEIN
      );
      createBiDirectionalRelationship(proteinNode, hugoGeneNode,
          new Tuple2<>(tpm.uniProtMapping().get().uniProtId(),
              tpm.uniProtMapping().get().geneSymbol()), proteinXrefRelMap, RelTypes.MAPS_TO,
          RelTypes.MAPS_TO);
      createBiDirectionalRelationship(proteinNode, ensemblGeneNode,
          new Tuple2<>(tpm.uniProtMapping().get().uniProtId(),
              tpm.uniProtMapping().get().ensemblGeneId()), proteinXrefRelMap, RelTypes.MAPS_TO,
          RelTypes.MAPS_TO);
      createBiDirectionalRelationship(proteinNode, ensemblTranscriptNode,
          new Tuple2<>(tpm.uniProtMapping().get().uniProtId(),
              tpm.uniProtMapping().get().ensemblTranscriptId()), proteinXrefRelMap,
          RelTypes.MAPS_TO,
          RelTypes.MAPS_TO);
      tx.success();
    } catch ( Exception e ) {
      AsyncLoggingService.logError(e.getMessage());
    }
  };

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(null != path
        && Files.exists(path, LinkOption.NOFOLLOW_LINKS));
    new TsvRecordSplitIteratorSupplier(path, RnaTpmGene.columnHeadings())
        .get()
        .map(RnaTpmGene::parseCsvRecordFunction)
        .filter(rnaTpmGene -> rnaTpmGene.uniProtMapping().isPresent())
        .filter(rtg -> rtg.tpm() > 0.0D)
        .forEach(rnaTpmGeneConsumer);
  }

  /*
  main method for standalone testing of this Consumer
  Uses a truncated version of the actual source file
   */
  public static void main(String[] args) {
      // use generic TestGraphDataConsumer to test
    FrameworkPropertyService.INSTANCE
           .getOptionalPathProperty("TEST_RNA_TPM_GENE_FILE")
            .ifPresent(path->
                new TestGraphDataConsumer().accept(path,new RnaTpmGeneConsumer()));
    }


}
