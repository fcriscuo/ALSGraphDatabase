package org.nygenome.als.graphdb.model;

import org.apache.commons.csv.CSVRecord;

import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Data;
import lombok.extern.log4j.Log4j;
import org.nygenome.als.graphdb.service.UniProtMappingService;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;

import java.nio.file.Paths;
import java.util.Optional;
import java.util.function.Function;

@Log4j
@Data
@Builder
public class HumanTissueAtlas extends ModelObject {
  /*
  TSV record header
  Gene	Gene name	Tissue	Cell type	Level	Reliability
   */
  private String ensemblGeneId;
  private String geneName;
  private String tissue;
  private String cellType;
  private String level;
  private String reliability;
  private String ensemblTranscriptId;

  private static String resolveTranscriptId(String geneId) {
    Optional<UniProtMapping> uniOpt = UniProtMappingService.INSTANCE.resolveUniProtMappingByEnsemblGeneId(geneId);
    return (uniOpt.isPresent()) ? uniOpt.get().getEnsemblTranscriptId() :"";
  }

  private static HumanTissueAtlas parseCsvRecord (@Nonnull CSVRecord record){
    return HumanTissueAtlas.builder()
        .ensemblGeneId(record.get("Gene"))
        .geneName(record.get("Gene name"))
        .tissue(record.get("Tissue"))
        .cellType(record.get("Cell type"))
        .level(record.get("Level"))
        .reliability(record.get("Reliability"))
        .ensemblTranscriptId(resolveTranscriptId(record.get("Gene")))
        .build();
  }

  public static Function<CSVRecord,HumanTissueAtlas> parseCsvRecordFunction =
      record -> HumanTissueAtlas.parseCsvRecord(record);

  public static void main(String[] args) {
   new TsvRecordStreamSupplier(Paths.get("/data/als/HumanTissueAtlas.tsv"))
       .get()
       .limit(50)
       .map(HumanTissueAtlas.parseCsvRecordFunction)
       .forEach(System.out::println);
  }

}
