package org.nygenome.als.graphdb.consumer;

import com.twitter.util.Duration;
import com.twitter.util.Stopwatches;
import java.util.function.Consumer;
import org.apache.commons.csv.CSVRecord;

import lombok.extern.log4j.Log4j;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.nygenome.als.graphdb.EmbeddedGraph;
import org.nygenome.als.graphdb.EmbeddedGraph.RelTypes;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import org.nygenome.als.graphdb.value.Pathway;
import scala.Tuple2;
import javax.annotation.Nonnull;
import java.nio.file.Path;


public class PathwayInfoConsumer extends GraphDataConsumer implements Consumer<Path>{


  /*
  Private Consumer to resolve (i.e. find/create) a Pathway Node
  and establish a protein->pathway Relationship if novel
   */
  private Consumer<Pathway> pathwayConsumer = (pathway)-> {
    Node pathwayNode = resolvePathwayNodeFunction.apply(pathway.id());
    // if this is the first time we've seen this Pathway we need to set the
    // pathway name
      lib.nodePropertyValueConsumer
          .accept(pathwayNode, new Tuple2<>("Pathway", pathway.eventName()));
     Tuple2<String,String> keyTuple = new Tuple2<>(pathway.uniprotId(),pathway.id());
      if(!proteinPathwayMap.containsKey(keyTuple)){
        Transaction tx = EmbeddedGraph.INSTANCE.transactionSupplier.get();
        Node proteinNode = resolveProteinNodeFunction.apply(pathway.uniprotId());
        try {
          proteinPathwayMap.put(keyTuple,
              proteinNode.createRelationshipTo(pathwayNode, RelTypes.IN_PATHWAY));
          tx.success();
        } catch (Exception e) {
          tx.failure();
          e.printStackTrace();
        } finally {
          tx.close();
        }
      }
  };

/*
Public Consumer get method to process the UniProt-Reactome tsv file at a
specified path
 */

    @Override
    public void accept(@Nonnull Path path) {
        Duration duration = Stopwatches.start().apply(); // start a stopwatch
        new TsvRecordStreamSupplier(path).get().map(Pathway::parseCSVRecord)
                .filter(pathway -> !pathway.uniprotId().startsWith("A") )
                .filter(pathway-> Pathway.isHuman(pathway.species()) )// only human entries
                .forEach(pathwayConsumer);
        AsyncLoggingService.logInfo("Processing pathway file " +path.toString()
            +" required " +duration.inSeconds() +" seconds" );

    }
    public static void main(String... args) {
        FrameworkPropertyService.INSTANCE.getOptionalPathProperty("UNIPROT_REACTOME_HOMOSAPIENS_MAPPING")
                .ifPresent(new PathwayInfoConsumer());
    }

}
