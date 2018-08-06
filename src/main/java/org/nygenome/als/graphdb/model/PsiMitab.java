package org.nygenome.als.graphdb.model;



import org.apache.commons.csv.CSVRecord;

import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Data;
import lombok.extern.log4j.Log4j;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Pattern;

@Log4j
@Data
// builder no longer included with data annotation
@Builder
public class PsiMitab {
  private String intearctorAId;
  private String interactorBId;
  private List<String> altIdAList;
  private List<String> altIdBList;
  private List<String> aliasAList;
  private List<String> aliasBList;
  private List<String> detectionMethodList;
  private List<String> firstAuthorList;
  private List<String> publicationIdList;
  private List<String> taxonmyAList;
  private List<String> taxonmyBList;
  private List<String> interactionTypeList;
  // TODO: combine into a tuple
  private List<String> sourceDtabaseList;
  private List<String> databaseIdentifierList;
  private List<String> confidenceScoreList;
  private List<String> complexExpansionList;
  private List<String> biologicalRoleAList;
  private List<String> biologicalRoleBList;
  private List<String> experimentalRoleAList;
  private List<String> experimentalRoleBList;
  private List<String> xrefAList;
  private List<String> xrefBList;
  private List<String> typeAList;
  private List<String> typeBList;
  private List<String> xrefInteractionList;
  private List<String> annotationAList;
  private List<String> annotationBList;
  private List<String> annotationInteractionList;
  private List<String> hostTaxonomyList;
  private List<String> interactionParameterList;
  private Boolean negative;
  private List<String> featureListA;
  private List<String> featureListB;

  private static Function<String,List<String>> parseStringOnPipeFunction = (s)->
      Arrays.asList( s.split(Pattern.quote("|")));

  public static  PsiMitab parseCSVRecord (@Nonnull CSVRecord record) {
    return PsiMitab.builder()
        .intearctorAId(record.get("#ID(s) interactor A"))
        .interactorBId(record.get("ID(s) interactor B"))
        .altIdAList(parseStringOnPipeFunction.apply(record.get("Alt. ID(s) interactor A")))
        .altIdBList(parseStringOnPipeFunction.apply(record.get("Alt. ID(s) interactor B")))
        .aliasAList(parseStringOnPipeFunction.apply(record.get("Alias(es) interactor A")))
        .aliasBList(parseStringOnPipeFunction.apply(record.get("Alias(es) interactor B")))
        .firstAuthorList(parseStringOnPipeFunction.apply(record.get("Publication 1st author(s)")))
        .publicationIdList(parseStringOnPipeFunction.apply(record.get("Publication Identifier(s)")))
        .interactionTypeList(parseStringOnPipeFunction.apply(record.get("Interaction type(s)")))
        .build();

  }

  /*
  Interaction detection method(s)
  Taxid interactor A
  Taxid interactor B
  Source database(s)
  Interaction identifier(s)
  Confidence value(s)
  Expansion method(s)
  Biological role(s) interactor A
  Biological role(s) interactor B
  Experimental role(s) interactor A
  Experimental role(s) interactor B
  Type(s) interactor A
  Type(s) interactor B
  Xref(s) interactor A
  Xref(s) interactor B
  Interaction Xref(s)
  Annotation(s) interactor A
  Annotation(s) interactor B
  Interaction annotation(s)
  Host organism(s)
  Interaction parameter(s)
  Negative
  Feature(s) interactor A
  Feature(s) interactor B
  Stoichiometry(s) interactor A
  Stoichiometry(s) interactor B
  Identification method participant A
  Identification method participant B
   */

  public static void main(String[] args) {
    System.out.println(">>>>>");

    try {
      new TsvRecordStreamSupplier(Paths.get("/tmp/intact_negative.txt")).get()
          .limit(50)
          .forEach(record -> {
            PsiMitab psi = PsiMitab.parseCSVRecord(record);
            System.out.println(psi.getIntearctorAId() + " to " + psi.getInteractorBId());
            psi.getAltIdAList().forEach((altA)-> log.info("alt id A: " +altA));
          });

    } catch (Exception e) {
      e.printStackTrace();
    }

  }
  }
