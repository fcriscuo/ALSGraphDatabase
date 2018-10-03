package org.nygenome.als.graphdb.consumer;

import com.google.common.base.Preconditions;
import java.nio.file.Path;
import java.util.function.Consumer;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import org.nygenome.als.graphdb.value.EnsemblAlsGene;

/*
Consume that will import data mined from ensembl
related to gene associated with the ALS phenotype.
The genes imported from these data will constitute thw
"white list" for ALS genes
Additional ALS genes may be added to this collection manually

 */
public class AlsGeneConsumer extends GraphDataConsumer{

  private Consumer<EnsemblAlsGene> alsGeneConsumer = (gene -> {

  });

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(path != null);
    new TsvRecordStreamSupplier(path).get()
        .map(EnsemblAlsGene::parseCSVRecord)
        .forEach(alsGeneConsumer);
  }
}
