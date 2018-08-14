package org.nygenome.als.graphdb.model;



import org.apache.commons.csv.CSVRecord;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Data;
import lombok.extern.log4j.Log4j;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Log4j
@Data
// builder no longer included with lombok data annotation
@Builder
public class PsiMitab  extends ModelObject {

  private static final String INTACT_HEADER_STRING = "#ID(s) interactor A\tID(s) interactor B\tAlt. ID(s) interactor A"
      + "\tAlt. ID(s) interactor B\tAlias(es) interactor A\tAlias(es) interactor B"
      + "\tInteraction detection method(s)\tPublication 1st author(s)\tPublication Identifier(s)"
      + "\tTaxid interactor A\tTaxid interactor B\tInteraction type(s)\tSource database(s)"
      + "\tInteraction identifier(s)\tConfidence value(s)\tExpansion method(s)\tBiological role(s) interactor A"
      + "\tBiological role(s) interactor B\tExperimental role(s) interactor A\tExperimental role(s) interactor B"
      + "\tType(s) interactor A\tType(s) interactor B\tXref(s) interactor A\tXref(s) interactor B\tInteraction Xref(s)"
      + "\tAnnotation(s) interactor A\tAnnotation(s) interactor B\tInteraction annotation(s)\tHost organism(s)"
      + "\tInteraction parameter(s)\tCreation date\tUpdate date\tChecksum(s) interactor A\tChecksum(s) interactor B"
      + "\tInteraction Checksum(s)\tNegative\tFeature(s) interactor A\tFeature(s) interactor B"
      + "\tStoichiometry(s) interactor A\tStoichiometry(s) interactor B\tIdentification method participant A"
      + "\tIdentification method participant B";

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
        .detectionMethodList(parseStringOnPipeFunction.apply(record.get("Interaction detection method(s)")))
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
        .interactionParameterList(parseStringOnPipeFunction.apply(record.get("Interaction parameter(s)")))
        .build();

  }
  public static Supplier<StructType> schemaSupplier = () -> {
// Generate the schema based on the string of schema
    List<StructField> fields = Arrays.asList(INTACT_HEADER_STRING.split("\t"))
        .stream()
        .map(heading -> DataTypes.createStructField(heading,
            DataTypes.StringType, true))
        .collect(Collectors.toList());
    return DataTypes.createStructType(fields);
  };


  public static Supplier<Encoder<PsiMitab> > encoderSupplier = ()->
      Encoders.bean(PsiMitab.class);

  public static Function<CSVRecord,PsiMitab> parseCsvRecordFunction = (record) ->
      PsiMitab.parseCSVRecord(record);

  /*  available parameters
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
      new TsvRecordStreamSupplier(Paths.get("/data/als/intact_negative.txt")).get()
          .limit(50)
          .map(parseCsvRecordFunction)
          .forEach(psi -> {
            log.info(">>>>> " +psi.getIntearctorAId() + " to " + psi.getInteractorBId() +"  negative = " +psi.getNegative());
            log.info(psi.toString());
          });

    } catch (Exception e) {
      e.printStackTrace();
    }
    StructType schema = PsiMitab.schemaSupplier.get();
   Arrays.asList(schema.fieldNames()).forEach(System.out::println);

   Encoder<PsiMitab> encoder = PsiMitab.encoderSupplier.get();
   System.out.println(encoder.toString());

  }
  }
