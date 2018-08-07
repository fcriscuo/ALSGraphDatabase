package org.nygenome.als.graphdb.model;

import org.apache.commons.csv.CSVRecord;

import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Data;
import lombok.extern.log4j.Log4j;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;

import java.nio.file.Paths;
import java.util.function.Function;
import java.util.function.Predicate;

@Log4j
@Data
@Builder
public class Uniprot2Reactome extends ModelObject{
  /*
  tsv file header
  UniProt_ID	Reactome_ID	URL	Event_Name	Evidence_Code	Species
   */
  private String uniProtId;
  private String reactomeId;
  private String url;
  private String eventName;
  private String evidenceCode;
  private String species;

  private static Uniprot2Reactome parseCsvRecord(@Nonnull CSVRecord record) {
    return Uniprot2Reactome.builder()
        .uniProtId(record.get("UniProt_ID"))
        .reactomeId(record.get("Reactome_ID"))
        .url(record.get("URL"))
        .eventName(record.get("Event_Name"))
        .evidenceCode(record.get("Evidence_Code"))
        .species(record.get("Species"))
        .build();
  }

  private static Function<CSVRecord,Uniprot2Reactome> parseCsvRecordFunction =
      record -> Uniprot2Reactome.parseCsvRecord(record);

  public static void main(String[] args) {
    new TsvRecordStreamSupplier(Paths.get("/data/als/UniProt2Reactome.tsv"))
        .get()
        .map(Uniprot2Reactome::parseCsvRecord)
        .filter(u2r ->Uniprot2Reactome.humanSpeciesPredicate.test(u2r.getSpecies()))
        .limit(50)
        .forEach(System.out::println);
  }
}
