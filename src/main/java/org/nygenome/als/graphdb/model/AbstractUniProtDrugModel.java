package org.nygenome.als.graphdb.model;

import org.apache.commons.csv.CSVRecord;

import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Data;
import lombok.extern.log4j.Log4j;

import java.util.List;
import java.util.function.Function;

@Data
@Log4j
public abstract class AbstractUniProtDrugModel extends ModelObject {

  protected String drugModelType;
  protected String id;
  protected String name;
  protected String geneName;
  protected String genbankProteinId;
  protected String genbankGeneId;
  protected String uniprotId;
  protected String uniprotTitle;
  protected String pdbId;
  protected String geneCardId;
  protected String geneAtlasId;
  protected String hgncId;
  protected String species;
  protected List<String> drugIdList;

  protected void parseCSVRecord(@Nonnull CSVRecord record) {
    this.id = record.get("ID");
    this.name = record.get("Name");
    this.geneName = record.get("Gene Name");
    this.genbankProteinId = record.get("GenBank Protein ID");
    this.genbankGeneId = record.get("GenBank Gene ID");
    this.uniprotId = record.get("UniProt ID");
    this.uniprotTitle = record.get("Uniprot Title");
    this.pdbId = record.get("PDB ID");
    this.geneCardId = record.get("GeneCard ID");
    this.geneAtlasId = record.get("GenAtlas ID");
    this.hgncId = record.get("HGNC ID");
    this.species = record.get("Species");
    this.drugIdList = parseStringOnSemiColonFunction.apply(record.get("Drug IDs"));
  }


}
