package org.nygenome.als.graphdb.model;

import org.apache.commons.csv.CSVRecord;
import org.apache.parquet.Strings;
import lombok.Builder;
import lombok.Data;
import lombok.extern.log4j.Log4j;
import java.util.Optional;
import java.util.function.Function;

@Data
@Log4j
@Builder
public class UniProtEnsemblTranscript extends ModelObject{
  private String uniprotId;
  private String geneDescription;
  private String geneSymbol;
  private String ensemblTranscriptId;

  public static Function<CSVRecord, Optional<UniProtEnsemblTranscript>>
      parseCsvRecordFunction = (record) -> {
    if(Strings.isNullOrEmpty(record.get("UniProtKB/Swiss-Prot ID"))) {
      return Optional.empty();
    }
    return Optional.of (
        UniProtEnsemblTranscript.builder()
            .uniprotId(record.get("\"UniProtKB/Swiss-Prot ID\""))
            .geneDescription(record.get("Gene description"))
            .geneSymbol(record.get("HGNC symbol"))
            .ensemblTranscriptId("Transcript stable ID")
            .build()
    );
  };

}
