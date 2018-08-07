package org.nygenome.als.graphdb.model;

import org.apache.commons.csv.CSVRecord;

import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Data;
import lombok.extern.log4j.Log4j;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import java.nio.file.Paths;
import java.util.function.Function;

@Log4j
@Data
@Builder
public class GeneDiseaseAssociation extends ModelObject {
  /*
  TSV file header
  geneId	geneSymbol	diseaseId	diseaseName	score	NofPmids	NofSnps	source
   */
  private Integer geneId;
  private String geneSymbol;
  private String diseaseId;
  private String diseaseName;
  private Double score;
  private Integer nOfPmids;
  private Integer nOfSnps;
  private String source;

  private static GeneDiseaseAssociation parseCsvRecord (@Nonnull CSVRecord record){
    return GeneDiseaseAssociation.builder()
        .geneId(Integer.valueOf(record.get("geneId")))
        .geneSymbol(record.get("geneSymbol"))
        .diseaseId(record.get("diseaseId"))
        .diseaseName(record.get("diseaseName"))
        .score(Double.valueOf(record.get("score")))
        .nOfPmids(Integer.valueOf(record.get("NofPmids")))
        .nOfSnps(Integer.valueOf(record.get("NofSnps")))
        .source(record.get("source"))
        .build();
  }

  public static Function<CSVRecord, GeneDiseaseAssociation>
      parseCsvRecordFunction = (record) ->
      GeneDiseaseAssociation.parseCsvRecord(record);

  public static void main(String[] args) {
    new TsvRecordStreamSupplier(Paths.get("/data/als/curated_gene_disease_associations.tsv")).get()
        .limit(50)
        .map(GeneDiseaseAssociation::parseCsvRecord)
        .forEach(System.out::println);
  }


}
