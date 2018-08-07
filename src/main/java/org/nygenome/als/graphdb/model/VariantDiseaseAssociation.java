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
public class VariantDiseaseAssociation extends ModelObject {
  /*
  TSV file header
  snpId	diseaseId	diseaseName	score	NofPmids	source
   */

  private String snpId;
  private String diseaseId;
  private String diseaseName;
  private Double score;
  private Integer nOfPmids;
  private String source;

  private static VariantDiseaseAssociation parseCsvRecord (@Nonnull CSVRecord record){
    return VariantDiseaseAssociation.builder()
        .snpId(record.get("snpId"))
        .diseaseId(record.get("diseaseId"))
        .diseaseName(record.get("diseaseName"))
        .score(Double.valueOf(record.get("score")))
        .nOfPmids(Integer.valueOf(record.get("NofPmids")))
        .source(record.get("source"))
        .build();
  }

  public static Function<CSVRecord, VariantDiseaseAssociation>
      parseCsvRecordFunction = (record) ->
      VariantDiseaseAssociation.parseCsvRecord(record);

  public static void main(String[] args) {
    new TsvRecordStreamSupplier(Paths.get("/data/als/curated_variant_disease_associations.tsv")).get()
        .limit(50)
        .map(VariantDiseaseAssociation::parseCsvRecord)
        .forEach(System.out::println);
  }


}
