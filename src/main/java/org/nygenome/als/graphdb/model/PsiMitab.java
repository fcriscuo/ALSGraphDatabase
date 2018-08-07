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
// builder no longer included with lombok data annotation
@Builder
public class PsiMitab  extends ModelObject{
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


  public static  PsiMitab parseCSVRecord (@Nonnull CSVRecord record) {
    return PsiMitab.builder()
        .intearctorAId(record.get("#ID(s) interactor A")) // n.b. # sign
        .interactorBId(record.get("ID(s) interactor B"))
        .altIdAList(parseStringOnPipeFunction.apply(record.get("Alt. ID(s) interactor A")))
        .altIdBList(parseStringOnPipeFunction.apply(record.get("Alt. ID(s) interactor B")))
        .aliasAList(parseStringOnPipeFunction.apply(record.get("Alias(es) interactor A")))
        .aliasBList(parseStringOnPipeFunction.apply(record.get("Alias(es) interactor B")))
        .firstAuthorList(parseStringOnPipeFunction.apply(record.get("Publication 1st author(s)")))
        .publicationIdList(parseStringOnPipeFunction.apply(record.get("Publication Identifier(s)")))
        .taxonmyAList(parseStringOnPipeFunction.apply(record.get("Taxid interactor A")))
        .taxonmyBList(parseStringOnPipeFunction.apply(record.get("Taxid interactor B")))
        .sourceDtabaseList(parseStringOnPipeFunction.apply("Source database(s)"))
        .databaseIdentifierList(parseStringOnPipeFunction.apply("Interaction identifier(s)"))
        .interactionTypeList(parseStringOnPipeFunction.apply(record.get("Interaction type(s)")))
        .biologicalRoleAList(parseStringOnPipeFunction.apply("Biological role(s) interactor A"))
        .biologicalRoleBList(parseStringOnPipeFunction.apply("Biological role(s) interactor B"))
        .experimentalRoleAList(parseStringOnPipeFunction.apply("Experimental role(s) interactor A"))
        .experimentalRoleBList(parseStringOnPipeFunction.apply("Experimental role(s) interactor B"))
        .typeAList(parseStringOnPipeFunction.apply("Type(s) interactor A"))
        .typeBList(parseStringOnPipeFunction.apply("Type(s) interactor B"))
        .xrefAList(parseStringOnPipeFunction.apply("Xref(s) interactor A"))
        .xrefBList(parseStringOnPipeFunction.apply("Xref(s) interactor B"))
        .xrefInteractionList(parseStringOnColonFunction.apply("Interaction Xref(s)"))
        .annotationAList(parseStringOnPipeFunction.apply("Annotation(s) interactor A"))
        .annotationBList(parseStringOnPipeFunction.apply("Annotation(s) interactor B"))
        .annotationInteractionList(parseStringOnPipeFunction.apply("Interaction annotation(s)"))
        .hostTaxonomyList(parseStringOnPipeFunction.apply("Host organism(s)"))
        .featureListA(parseStringOnPipeFunction.apply("Feature(s) interactor A"))
        .featureListB(parseStringOnPipeFunction.apply("Feature(s) interactor B"))
        .negative(Boolean.valueOf(record.get("Negative")))
        .build();

  }

  public static Function<CSVRecord,PsiMitab> parseCsvRecordFunction = (record) ->
      PsiMitab.parseCSVRecord(record);

  /*  available parameters
  Interaction detection method(s)
  Confidence value(s)
  Expansion method(s)
  Interaction parameter(s)
  Stoichiometry(s) interactor A
  Stoichiometry(s) interactor B
  Identification method participant A
  Identification method participant B
   */

  /* main method for stand alone testing
   */
  public static void main(String[] args) {
    try {
      new TsvRecordStreamSupplier(Paths.get("/tmp/intact_negative.txt")).get()
          .limit(50)
          .map(parseCsvRecordFunction)
          .forEach(psi -> {
            log.info(">>>>> " +psi.getIntearctorAId() + " to " + psi.getInteractorBId() +"  negative = " +psi.getNegative());
            psi.getAltIdAList().forEach((altA)-> log.info("alt id A: " +altA));
            psi.getPublicationIdList().forEach(pub-> log.info("Publication: " +pub));
            psi.getFeatureListA().forEach(featA -> log.info("feature A: " + featA));
            psi.getFeatureListB().forEach(featB -> log.info("feature B: " + featB));
          });

    } catch (Exception e) {
      e.printStackTrace();
    }

  }
  }
