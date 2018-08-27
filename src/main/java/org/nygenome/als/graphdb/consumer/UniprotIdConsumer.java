package org.nygenome.als.graphdb.consumer;

import org.nygenome.als.graphdb.model.UniProtEnsemblTranscript;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;

import java.nio.file.Path;
import java.util.function.Consumer;

public class UniprotIdConsumer extends GraphDataConsumer implements Consumer<Path> {

  private Consumer<UniProtEnsemblTranscript> uniprotConsumer = (uniprot) -> {
    if (!proteintMap.containsKey(uniprot.getUniprotId())) {
      createProteinNode(strNoInfo, uniprot.getUniprotId(), strNoInfo,
          strNoInfo, strNoInfo, strNoInfo);
    }

    proteintMap.get(uniprot.getUniprotId()).setProperty("EnsemblTranscript",
        uniprot.getEnsemblTranscriptId());
    proteintMap.get(uniprot.getUniprotId())
        .setProperty("ProteinName", uniprot.getGeneDescription());
    proteintMap.get(uniprot.getUniprotId()).setProperty("GeneSymbol",uniprot.getGeneSymbol());

  };

  @Override public void accept(final Path path) {
    new TsvRecordStreamSupplier(path)
        .get()
        .map(UniProtEnsemblTranscript.parseCsvRecordFunction)
        .forEach(opt -> opt.ifPresent(uniprotConsumer));
    // log out the size of the protein map
    System.out.println("The ProteinMap has " +proteintMap.size() +" proteins");

  }

}
