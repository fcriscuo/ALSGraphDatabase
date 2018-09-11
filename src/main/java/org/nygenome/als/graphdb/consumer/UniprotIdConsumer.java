package org.nygenome.als.graphdb.consumer;

import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;

import java.nio.file.Path;
import java.util.function.Consumer;
import org.nygenome.als.graphdb.value.UniProtEnsemblTranscript;

public class UniprotIdConsumer extends GraphDataConsumer implements Consumer<Path> {

  private Consumer<UniProtEnsemblTranscript> uniprotConsumer = (uniprot) -> {
    if (!proteintMap.containsKey(uniprot.uniprotId())) {
      createProteinNode(strNoInfo, uniprot.uniprotId(), strNoInfo,
          strNoInfo, strNoInfo, strNoInfo);
    }

    proteintMap.get(uniprot.uniprotId()).setProperty("EnsemblTranscript",
        uniprot.ensemblTranscriptId());
    proteintMap.get(uniprot.uniprotId())
        .setProperty("ProteinName", uniprot.geneDescription());
    proteintMap.get(uniprot.uniprotId()).setProperty("GeneSymbol",uniprot.geneSymbol());
  };

  @Override public void accept(final Path path) {
    new TsvRecordStreamSupplier(path)
        .get()
        .map(UniProtEnsemblTranscript::parseCSVRecord)
        .filter(opt -> opt.nonEmpty())
        .forEach(opt -> uniprotConsumer.accept(opt.get()));
    // log out the size of the protein map
    System.out.println("The ProteinMap has " +proteintMap.size() +" proteins");

  }

}
