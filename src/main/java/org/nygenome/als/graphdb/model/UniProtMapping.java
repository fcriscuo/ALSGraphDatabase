package org.nygenome.als.graphdb.model;

import org.apache.commons.csv.CSVRecord;

import lombok.Builder;
import lombok.Data;
import lombok.extern.log4j.Log4j;
import scala.Tuple3;

import java.util.function.Function;
import java.util.function.Supplier;

@Data
@Builder
@Log4j
public class UniProtMapping {
  private String uniProtId;
  private String ensemblGeneId;
  private String ensemblTranscriptId;
  private String geneSymbol;

  public Supplier<Tuple3<String,String,String>> idTuple3Supplier = () ->
      new Tuple3<>(this.getUniProtId(),
          this.getEnsemblTranscriptId(),
          this.getEnsemblGeneId());

  public static Function<CSVRecord,UniProtMapping> parseCsvRecordFunction = (record) ->
    new UniProtMappingBuilder()
        .uniProtId(record.get("UniProtKB/Swiss-Prot ID"))
        .ensemblGeneId(record.get("Gene stable ID"))
        .ensemblTranscriptId(record.get("Transcript stable ID"))
        .geneSymbol(record.get("HGNC symbol"))
        .build();

}
